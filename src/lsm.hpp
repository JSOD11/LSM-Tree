#ifndef LSM_HPP
#define LSM_HPP

#include <string>
#include <tuple>
#include <cassert>
#include <map>
#include <limits>
#include <filesystem>
#include <fstream>

#include "Types.hpp"
#include "Utils.hpp"
#include "bloomfilter.hpp"
#include <unordered_map>
#include <map>
#include <chrono>

struct Stats {
    size_t puts = 0;
    size_t successfulGets = 0;
    size_t failedGets = 0;
    size_t ranges = 0;
    double rangeLengthSum = 0;
    VAL_TYPE rangeValueSum = 0; // This is modulo 10**6 since it could get very large.
    size_t searchLevelCalls = 0;
    size_t bloomTruePositives = 0;
    size_t bloomFalsePositives = 0;
    size_t deletes = 0;
};

template<typename KeyType, typename ValType, typename DictValType>
struct Level {
    KeyType* keys = nullptr;
    std::variant<ValType*, DictValType*> vals;
    bool* tombstone = nullptr;
    size_t numPairs = 0;
    KeyType* fence = nullptr;
    size_t fenceLength = 0;
    BloomFilter* bloomFilter = nullptr;
    EncodingType encodingType = ENCODING_TYPE;

    // Note here the mapping from ValType to DictValType. See `Types.hpp` for more explanation.
    std::map<ValType, DictValType> dict;
    std::vector<ValType> dictReverse;

    ~Level() {
        delete[] fence;
        delete bloomFilter;
    }
};

// `LSM`
// A log structured merge tree class.
template<typename KeyType, typename ValType, typename DictValType>
class LSM {
    private:
        // The page size is the number of entries in a page.
        size_t pageSize = PAGE_SIZE;
        // bufferPages is the number of pages in the buffer.
        size_t bufferPages = BUFFER_PAGES;
        size_t numLevels = 0;
        size_t sizeRatio = SIZE_RATIO;
        std::vector<Level<KeyType, ValType, DictValType>*> levels = {};
        Stats stats;
    
    public:
        LSM() {
            assert(this->getPageSize() > 0);
            assert(this->getBufferSize() > 0);
            assert(this->getSizeRatio() > 0);

            this->populateCatalog();
        }

        // `populateCatalog()`
        // Populates the catalog with persisted data or creates a data folder if one does not exist.
        void populateCatalog(void) {
            // Create the data folder if it does not exist.
            if (!std::filesystem::exists("data")) std::filesystem::create_directory("data");

            if (!std::filesystem::exists("data/catalog.data")) {
                // The database is being started from scratch. We start just with l0.
                KeyType* keysPointer = mmapLevel<KeyType>("data/k0.data", 0);
                void* valsPointer = nullptr;
                if (ENCODING_TYPE == ENCODING_OFF) valsPointer = mmapLevel<ValType>("data/v0.data", 0);
                else if (ENCODING_TYPE == ENCODING_DICT) valsPointer = mmapLevel<DictValType>("data/v0.data", 0);
                bool* tombstonePointer = mmapLevel<bool>("data/t0.data", 0);
                this->initializeLevel(0, keysPointer, valsPointer, tombstonePointer, 0);
                // std::cout << "Started new database from scratch.\n" << std::endl;
            } else {
                // We are populating the catalog with persisted data.
                std::ifstream catalogFile("data/catalog.data");
                size_t numPairs = 0, l = 0;
                while (catalogFile >> numPairs) {
                    KeyType* keysPointer = mmapLevel<KeyType>(("data/k" + std::to_string(l) + ".data").c_str(), l);
                    void* valsPointer = nullptr;
                    if (ENCODING_TYPE == ENCODING_OFF) valsPointer = mmapLevel<ValType>(("data/v" + std::to_string(l) + ".data").c_str(), l);
                    else if (ENCODING_TYPE == ENCODING_DICT) valsPointer = mmapLevel<DictValType>(("data/v" + std::to_string(l) + ".data").c_str(), l);
                    bool* tombstonePointer = mmapLevel<bool>(("data/t" + std::to_string(l) + ".data").c_str(), l);
                    this->initializeLevel(l, keysPointer, valsPointer, tombstonePointer, numPairs);

                    // Populate the dictionary from persisted dictionary files.
                    std::ifstream dictStream ("data/dict" + std::to_string(l) + ".data");
                    ValType val;
                    DictValType encodedVal;
                    while (dictStream >> val >> encodedVal) {
                        this->getLevel(l)->dict[val] = encodedVal;
                    }
                    dictStream.close();

                    // Construct the dictReverse array.
                    std::ifstream dictReverseStream ("data/dictreverse" + std::to_string(l) + ".data");
                    ValType valReverse;
                    while (dictReverseStream >> valReverse) {
                        this->getLevel(l)->dictReverse.push_back(valReverse);
                    }
                    dictReverseStream.close();

                    l++;
                }
                std::cout << "Loaded persisted data.\n" << std::endl;
            }
        }

        // `shutdownServer()`
        // Shuts down the server upon receiving an `s` or `sw` command from the client, munmaps files,
        // and frees levels. `s` persists the data in the data folder and `sw` wipes the data folder.
        void shutdownServer(std::string userCommand) {
            if (userCommand == "sw") {
                std::filesystem::remove_all("data");
                std::cout << "Wiped data folder." << std::endl;
            } else {
                // Write the number of pairs per level into the catalog file.
                std::ofstream catalogFile("data/catalog.data", std::ios::out);
                for (size_t l = 0; l < this->getNumLevels(); l++) {
                    catalogFile << this->getPairsInLevel(l) << std::endl;
                }
                catalogFile.close();

                // Persist the dictionaries.
                for (size_t l = 0; l < this->getNumLevels(); l++) {
                    // std::cout << "Persisting dict for level " << l << std::endl;
                    std::ofstream dictStream ("data/dict" + std::to_string(l) + ".data", std::ios::out | std::ios::trunc);
                    for (const auto& x : this->getLevel(l)->dict) {
                        dictStream << x.first << " " << x.second << std::endl;
                    }
                    dictStream.close();

                    // std::cout << "Persisting dictreverse for level " << l << std::endl;
                    std::ofstream dictReverseStream ("data/dictreverse" + std::to_string(l) + ".data", std::ios::out | std::ios::trunc);
                    for (size_t i = 0; i < this->getLevel(l)->dictReverse.size(); i++) {
                        dictReverseStream << this->getLevel(l)->dictReverse[i] << std::endl;
                    }
                    dictReverseStream.close();
                }

                std::cout << "Persisted data folder." << std::endl;
            }

            for (size_t l = 0; l < this->getNumLevels(); l++) {
                munmap(this->getLevelKeys(l), this->getPairsInLevel(l) * sizeof(KeyType));
                if (this->getLevel(l)->encodingType == ENCODING_OFF) munmap(this->getLevelVals(l), this->getPairsInLevel(l) * sizeof(ValType));
                else if (this->getLevel(l)->encodingType == ENCODING_DICT) munmap(this->getLevelVals(l), this->getPairsInLevel(l) * sizeof(DictValType));
                munmap(this->getLevelTombstone(l), this->getPairsInLevel(l) * sizeof(bool));

                delete this->getLevel(l);
            }
        }

        void printStats(void) {
            // std::cout << "\n ——— Session statistics ——— \n" << std::endl;
            std::cout << "\nPuts: " << this->stats.puts << std::endl;
            std::cout << "Successful gets: " << this->stats.successfulGets << std::endl;
            std::cout << "Failed gets: " << this->stats.failedGets << std::endl;
            std::cout << "Ranges: " << this->stats.ranges << std::endl;
            std::cout << "Sum length of all ranges: " << this->stats.rangeLengthSum << std::endl;
            std::cout << "Range Value Sum % 10^6: " << this->stats.rangeValueSum << std::endl;
            // std::cout << "Calls to searchLevel(): " << this->stats.searchLevelCalls << std::endl;
            // std::cout << "Bloom true positives: " << this->stats.bloomTruePositives << std::endl;
            // std::cout << "Bloom false positives: " << this->stats.bloomFalsePositives << std::endl;
            std::cout << "Bloom FPR: " << (float)this->stats.bloomFalsePositives / (float)(this->stats.bloomFalsePositives + (this->stats.searchLevelCalls - this->stats.bloomTruePositives)) << std::endl;
            std::cout << "Deletes: " << this->stats.deletes << std::endl;
            // std::cout << "\n —————————————————————————— \n" << std::endl;
        }

        // `put()`
        // Put a key and value into the LSM tree. If the key already exists, update the value.
        // This function is also used for deletes by setting `isDelete = true`.
        std::tuple<Status, std::string> put(Status status, KeyType key, ValType val, bool isDelete) {
            if (!isDelete) this->stats.puts++;
            else this->stats.deletes++;
            this->appendPair(0, key, val, isDelete);
            return std::make_tuple(status, "");
        }

        // `get()`
        // Search the LSM tree for a key.
        std::tuple<Status, std::string> get(Status status, KeyType key) {

            // Search through each level of the LSM tree.
            for (size_t l = 0; l < this->getNumLevels(); l++) {
                int i = this->searchLevel(l, key, false);
                if (i >= 0) {
                    if (this->getTomb(l, i)) break;
                    this->stats.successfulGets++;
                    return std::make_tuple(status, std::to_string(this->getVal(l, i)));
                }
            }

            this->stats.failedGets++;
            return std::make_tuple(status, "");
        }

        // `range()`
        // Conduct a range query within the LSM tree.
        std::tuple<Status, std::string> range(Status status, KeyType leftBound, KeyType rightBound) {

            this->stats.ranges++;

            std::map<KeyType, ValType> results;

            // A range query must search through every level of the LSM tree. We iterate in reverse so that
            // only the most recent duplicate KV pair is retrieved in the case of duplicate entries.
            for (int l = this->getNumLevels() - 1; l >= 0; l--) {
                if (l == 0) {
                    for (size_t i = 0; i < this->getPairsInLevel(0); i++) {
                        if ((leftBound <= this->getKey(l, i)) && (this->getKey(l, i) < rightBound)) {
                            results[this->getKey(l, i)] = this->getVal(l, i);
                            if (this->getTomb(l, i)) results.erase(this->getKey(l, i));
                        }
                    }
                } else {
                    auto startSearch = std::chrono::high_resolution_clock::now();
                    int startIndex = this->searchLevel(l, leftBound, true);
                    int endIndex = this->searchLevel(l, rightBound, true);
                    auto endSearch = std::chrono::high_resolution_clock::now();
                    auto durationSearch = std::chrono::duration_cast<std::chrono::microseconds>(endSearch - startSearch);

                    auto startRange = std::chrono::high_resolution_clock::now();
                    for (int i = startIndex; i < endIndex; i++) {
                        results[this->getKey(l, i)] = this->getVal(l, i);
                        if (this->getTomb(l, i)) results.erase(this->getKey(l, i));
                    }
                    auto endRange = std::chrono::high_resolution_clock::now();
                    auto durationRange = std::chrono::duration_cast<std::chrono::microseconds>(endRange - startRange);

                    std::ofstream logfile("logfile.txt", std::ios::app);
                    if (logfile.is_open()) {
                        logfile << "Search time: " << durationSearch.count() << " microseconds." << std::endl;
                        std::cout << "Range time: " << durationRange.count() << " microseconds." << std::endl;
                        logfile.close();
                    } else {
                        std::cout << "Failed to open log file." << std::endl;
                    }
                }
            }

            std::cout << "Range query bounds: [" << leftBound << ", " << rightBound << "], Range query size: " << results.size() << std::endl;
            this->stats.rangeLengthSum += results.size();
            if (TESTING_SWITCH == TESTING_ON) {
                for (const auto& pair : results) {
                    this->stats.rangeValueSum = (this->stats.rangeValueSum + pair.second) % static_cast<ValType>(std::pow(10, 6));
                }
            }
            return std::make_tuple(status, mapToString(results));
        }

        void printLevels(std::string userCommand) {

            std::cout << "\n———————————————————————————————— " << std::endl;
            std::cout << "——————— Printing levels. ——————— " << std::endl;
            std::cout << "———————————————————————————————— \n" << std::endl;

            for (size_t l = 0; l < this->getNumLevels(); l++) {
                if (l == 0) std::cout << "\n ——————— Buffer ——————— " << std::endl;
                else std::cout << "\n ——————— Level " << l << " ——————— " << std::endl;

                std::cout << "Contains: " << this->getPairsInLevel(l) << " KV pairs = " << this->getPairsInLevel(l) * (sizeof(KeyType) + sizeof(ValType)) << " bytes." << std::endl;
                std::cout << "Unique keys: " << this->getUniqueKeyCount(l) << ". Unique values: " << this->getUniqueValCount(l) << std::endl;
                std::cout << "Capacity: " << this->getLevelCapacity(l) << " KV pairs = " << this->getLevelCapacity(l) * (sizeof(KeyType) + sizeof(ValType)) << " bytes." << std::endl;

                if (userCommand == "pv") {
                    // Verbose printing.
                    if (l == 0) {
                        std::cout << "Buffer is unsorted. No fence pointers." << std::endl;
                    } else if (!this->levelIsEmpty(l)) {
                        std::cout << "Fence: [";
                        for (size_t i = 0; i < this->getFenceLength(l) - 1; i++) {
                            std::cout << this->getFenceKey(l, i) << ", ";
                        }
                        std::cout << this->getFenceKey(l, this->getFenceLength(l) - 1) << "]" << std::endl;
                    }
                    std::cout << "Bloom: [";
                    for (size_t i = 0; i < this->getBloomFilter(l)->numBits() - 1; i++) {
                        std::cout << this->getBloomFilter(l)->getBit(i) << ", ";
                    }
                    std::cout << this->getBloomFilter(l)->getBit(this->getBloomFilter(l)->numBits() - 1) << "]" << std::endl;
                    for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                        std::cout << this->getKey(l, i) << " -> " << this->getVal(l, i) << "  " << this->getTomb(l, i) << std::endl;
                    }
                }
            }
        }

        std::tuple<Status, std::string> processCommand(std::string userCommand) {

            Status status = SUCCESS;

            if (userCommand == "s" || userCommand == "shutdown" || userCommand == "sw") {
                if (userCommand == "sw") return std::make_tuple(status, "Server processed shutdown command. Wiping all data.");
                return std::make_tuple(status, "Server processed shutdown command. Persisting data.");
            }

            if (userCommand == "p" || userCommand == "pv") {
                printLevels(userCommand);
                return std::make_tuple(status, "Printed levels to server.");
            }

            // Tokenize the command.
            std::vector<std::string> tokens = parseCommand(userCommand);

            // Check that the input command is valid, and proceed with routing if so.
            if (tokens[0] == "p" && tokens.size() == 3 && isNum(tokens[1]) && isNum(tokens[2])) {
                // std::cout << "Received put command.\n" <<  std::endl;
                return put(status, std::stoi(tokens[1]), std::stoi(tokens[2]), false);
            } else if (tokens[0] == "g" && tokens.size() == 2 && isNum(tokens[1])) {
                // std::cout << "Received  get command.\n" << std::endl;
                return get(status, std::stoi(tokens[1]));
            } else if (tokens[0] == "r" && tokens.size() == 3 && isNum(tokens[1]) && isNum(tokens[2])) {
                // std::cout << "Received range query command.\n" <<  std::endl;
                auto start = std::chrono::high_resolution_clock::now();
                auto res = range(status, std::stoi(tokens[1]), std::stoi(tokens[2]));
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                std::ofstream logfile("logfile.txt", std::ios::app);
                if (logfile.is_open()) {
                    std::cout << "Range query took " << duration.count() << " microseconds." << std::endl;
                    logfile.close();
                } else {
                    std::cout << "Failed to open log file." << std::endl;
                }
                return res;
            } else if (tokens[0] == "d" && tokens.size() == 2 && isNum(tokens[1])) {
                // std::cout << "Received delete command.\n" <<  std::endl;
                return put(status, std::stoi(tokens[1]), 0, true);
            } else {
                return std::make_tuple(status,
                        "Supported commands: \n\n\
                        p x y — PUT\n\
                        g x   — GET\n\
                        r x y — RANGE\n\
                        p     — Print levels to server.\n\
                        pv    — Print levels to server (verbose).\n\
                        s     — Shutdown and persist.\n\
                        sw    — Shutdown and wipe all data.\n"
                    );
            }
        }

        size_t getPageSize() { return this->pageSize; }
        size_t getBufferSize() { return this->bufferPages * this->getPageSize(); }
        size_t getNumLevels() { return this->numLevels; }
        size_t getSizeRatio() { return this->sizeRatio; }
        Level<KeyType, ValType, DictValType>* getLevel(size_t l) { return this->levels[l]; }
        KeyType* getLevelKeys(size_t l) { return this->getLevel(l)->keys; }
        std::map<ValType, DictValType>& getLevelDict(size_t l) { return this->getLevel(l)->dict; }
        bool* getLevelTombstone(size_t l) { return this->getLevel(l)->tombstone; }
        size_t getPairsInLevel(size_t l) { return this->getLevel(l)->numPairs; }
        size_t getLevelCapacity(size_t l) { return this->getBufferSize() * std::pow(this->getSizeRatio(), l); }
        bool levelIsEmpty(size_t l) { return this->getPairsInLevel(l) == 0; }
        KeyType getFenceKey(size_t l, size_t index) {
            assert(this->getLevel(l)->fence != nullptr);
            return this->getLevel(l)->fence[index];
        }
        size_t getFenceLength(size_t l) { return this->getLevel(l)->fenceLength; }
        BloomFilter* getBloomFilter(size_t l) { return this->getLevel(l)->bloomFilter; }


        int64_t getUniqueKeyCount(size_t l) {
            std::map<KeyType, bool> keys;
            for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                keys[this->getKey(l, i)] = true;
            }
            return keys.size();
        }

        int64_t getUniqueValCount(size_t l) {
            std::map<ValType, bool> vals;
            for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                vals[this->getVal(l, i)] = true;
            }
            return vals.size();
        }

        // `initializeLevel()`
        // This function is used when we intend to create a new empty level at the bottom of the LSM tree.
        void initializeLevel(size_t l, KeyType* keysPointer, void* valsPointer, bool* tombstonePointer, size_t numPairs) {
            assert(l == this->levels.size());
            Level<KeyType, ValType, DictValType>* newLevel = new Level<KeyType, ValType, DictValType>;
            newLevel->keys = keysPointer;
            if (ENCODING_TYPE == ENCODING_OFF) newLevel->vals = static_cast<ValType*>(valsPointer);
            else if (ENCODING_TYPE == ENCODING_DICT) newLevel->vals = static_cast<DictValType*>(valsPointer);
            newLevel->tombstone = tombstonePointer;
            newLevel->numPairs = numPairs;
            this->levels.push_back(newLevel);
            this->numLevels++;
            this->constructFence(l);
            this->constructBloomFilter(l);
        }

        // `appendPair()`
        // Appends a new KV pair at the end of the specified level. If dictionary encoding is
        // turned on, the key is stored as usual, and the value (of type `ValType`) and its dictionary encoded value
        // (of type `DictValType`) are stored in the dictionary. In the case of DICT encoding, the dictionary 
        // encoded value is stored in the values array instead of the uncompressed value.
        void appendPair(size_t l, KeyType key, ValType val, bool isDelete) {
            this->getLevelKeys(l)[this->getPairsInLevel(l)] = key;
            Level<KeyType, ValType, DictValType>* level = this->getLevel(l);
            if (this->getLevel(l)->encodingType == ENCODING_DICT){

                // std::cout << "Dict size: " << level->dict.size() << std::endl;
                // std::cout << "DictValType capacity: " << static_cast<int>(std::numeric_limits<DictValType>::max() + 1) << std::endl;
                
                assert(level->dict.size() <= static_cast<int>(std::numeric_limits<DictValType>::max() + 1));
                
                // If this assertion is failing, it's because there are too many unique values
                // to store in the number of bits given by DictValType (AKA this workload is not supported).
                // Comment out the two lines above the assertion to see where the issue is arising. A future project is to
                // make it so that the tree automatically increases the number of bits used in
                // the dictionary, but for now it is fixed in `Types.hpp`.

                if (level->dict.find(val) == level->dict.end()){
                    level->dict[val] = level->dict.size();
                    level->dictReverse.push_back(val);
                }
                DictValType* vals = static_cast<DictValType*>(this->getLevelVals(l));
                vals[this->getPairsInLevel(l)] = level->dict[val];
            } else {
                ValType* vals = static_cast<ValType*>(this->getLevelVals(l));
                vals[this->getPairsInLevel(l)] = val;
            }

            this->getLevelTombstone(l)[this->getPairsInLevel(l)] = isDelete;
            this->getLevel(l)->numPairs++;
            this->getLevel(l)->bloomFilter->add(key);
            if (this->getPairsInLevel(l) == this->getLevelCapacity(l)) this->propagateLevel(l);
            
            return;
        }

        // `getKey()`
        // Returns the key at the index specified in the level specified.
        KeyType getKey(size_t l, size_t entryIndex) {
            return this->getLevelKeys(l)[entryIndex];
        }

        // `getLevelVals()`
        // Returns a void* either pointing to a vals array of type ValType* (in the case of no compression) or
        // a vals array of type DictValType* (in the case that DICT compression is enabled).
        void* getLevelVals(size_t l) {
            if (this->getLevel(l)->encodingType == ENCODING_OFF) return std::get<ValType*>(this->getLevel(l)->vals);
            else if (this->getLevel(l)->encodingType == ENCODING_DICT) return std::get<DictValType*>(this->getLevel(l)->vals);
            assert(false); // If this assert executed, the encoding type is not supported.
            return nullptr;
        }

        // `getVal()`
        // Returns the uncompressed value at the index specified in the level specified.
        // Compatible with DICT encoding.
        ValType getVal(size_t l, size_t entryIndex) {
            if (this->getLevel(l)->encodingType == ENCODING_OFF) {
                return std::get<ValType*>(this->getLevel(l)->vals)[entryIndex];
            } else if (this->getLevel(l)->encodingType == ENCODING_DICT) {
                DictValType dictIndex = std::get<DictValType*>(this->getLevel(l)->vals)[entryIndex];
                return this->getLevel(l)->dictReverse[dictIndex];
            }
            assert(false); // If this assert executed, the encoding type is not supported.
            return 0;
        }

        // `getTomb()`
        // Returns the tombstone bit at the index specified in the level specified. `1` means to delete.
        bool getTomb(size_t l, size_t entryIndex) {
            return this->getLevelTombstone(l)[entryIndex];
        }
        
        // `constructFence()`
        // Constructs the fence pointer array at level l within the catalog.
        void constructFence(size_t l) {
            if (l == 0) return;
            delete[] this->getLevel(l)->fence;
            this->getLevel(l)->fenceLength = std::ceil(static_cast<double>(this->getLevel(l)->numPairs) / this->getPageSize());
            this->getLevel(l)->fence = new KeyType[this->getLevel(l)->fenceLength];
            for (size_t i = 0, j = 0; i < this->getLevel(l)->fenceLength; i++, j += this->getPageSize()) {
                if (j >= this->getLevel(l)->numPairs) {
                    std::cout << "constructFence(): Out of bounds access error." << std::endl;
                    return;
                }
                this->getLevel(l)->fence[i] = this->getKey(l, j);
            }
        }

        // `constructBloomFilter()`
        // Constructs a bloom filter boolean vector over the keys for the specified level based on the current state of the level. 
        // Called at the end of `propagateLevel()` and in `populateCatalog()`.
        void constructBloomFilter(size_t l) {
            size_t levelSize = this->getLevelCapacity(l);
            size_t numBits = static_cast<size_t>(-(levelSize * std::log(BLOOM_TARGET_FPR)) / std::pow(std::log(2), 2));
            
            // Calculate the optimal number of hash functions.
            size_t numHashes = static_cast<size_t>((numBits / static_cast<double>(levelSize)) * std::log(2));
            if (numHashes < 1) numHashes = 1;

            if (this->getLevel(l)->bloomFilter != nullptr) {
                this->getLevel(l)->bloomFilter->clear();
            } else {
                this->getLevel(l)->bloomFilter = new BloomFilter(numBits, numHashes);
            }

            if (!this->levelIsEmpty(l)) {
                for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                    this->getLevel(l)->bloomFilter->add(this->getKey(l, i));
                }
            }
        }

        // `sortLevel()`
        // Sorts the level indicated. Also handles fence pointers and the bloom filter.
        void sortLevel(size_t l) {
            if (l >= this->getNumLevels()) {
                std::cout << "sortLevel(): Tried to sort nonexistent level." << std::endl;
                return;
            }

            // This map effectively only keeps the most recent value for a given key. So if a key has been written more than once,
            // only the most recent value will be kept. The map is also ordered which lets us write directly back to the level.
            // We also filter out all of the deletions during this process.
            std::map<KeyType, std::pair<ValType, bool>> pairs;
            for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                pairs[this->getKey(l, i)] = std::make_pair(this->getVal(l, i), this->getTomb(l, i));
            }

            this->clearLevel(l);

            for (const std::pair<KeyType, std::pair<ValType, bool>>& pair : pairs) {
                // Append the pairs in order, except in the case that it is a tombstone on the final level.
                if (!(l == this->getNumLevels() - 1 && pair.second.second == true)) {
                    this->appendPair(l, pair.first, pair.second.first, pair.second.second);
                }
            }

            this->constructFence(l);
            this->constructBloomFilter(l);
        }

        // `searchFence()`
        // Searches through the fence pointers at level l for the specified key.
        // Returns the page on which the key will be found if it exists.
        int searchFence(size_t level, KeyType key) {
            // Binary search through the fence pointers to get the target page.
            int l = 0, r = this->getFenceLength(level) - 1;
            while (l <= r) {
                // The target page is the final page.
                if (l == (int)this->getFenceLength(level)- 1) break;

                int m = (l + r) / 2;
                if (this->getFenceKey(level, m) <= key && key < this->getFenceKey(level, m + 1)) {
                    return m;
                } else if (this->getFenceKey(level, m) < key) {
                    l = m + 1;
                } else {
                    r = m - 1;
                }
            }
            return r;
        }

        bool searchBloomFilter(size_t level, KeyType key) {
            return this->getBloomFilter(level)->mayContain(key);
        }

        // `searchLevel()`
        // Searches for a key within level l of the LSM tree. Returns the index i
        // of the key if it exists within the level, or -1 otherwise.
        //
        // `searchLevel()` contains a switch that allows it to be used for range queries.
        // For a range query, in the case that the target key does not exist, we return the 
        // smallest value larger than `key`. For a leftBound, this means we will get only
        // values larger than the leftBound, which is correct. This is correct for rightBounds
        // because the rightBound in these range queries is an exclusive bound.
        int searchLevel(size_t level, KeyType key, bool range) {

            if (!range) this->stats.searchLevelCalls++;
            if (!range) {
                if (this->levelIsEmpty(level) || !searchBloomFilter(level, key)) return -1;
            } else {
                // Level is empty or bound is outside the range of keys in the level. Return 0 or len(level) - 1.
                if (this->levelIsEmpty(level) || key < this->getKey(level, 0)) return 0;
                if (key > this->getKey(level, this->getPairsInLevel(level) - 1)) return this->getPairsInLevel(level);
            }

            // The buffer, l0, is not sorted by key. All layers beneath l0 are sorted by key.

            if (level == 0) {
                // Iterate backwards through the buffer to get the most recent entry.
                for (int i = this->getPairsInLevel(level) - 1; i >= 0; i--) {
                    if (this->getKey(level, i) == key) {
                        if (!range) this->stats.bloomTruePositives++;
                        return i;
                    }
                }
            } else {
                KeyType pageIndex = searchFence(level, key);
                if (pageIndex != -1) {
                    // Binary search within the page.
                    KeyType l = pageIndex * this->getPageSize();
                    KeyType r = (pageIndex + 1) * this->getPageSize();
                    if ((KeyType)this->getPairsInLevel(level) - 1 < r) r = (KeyType)this->getPairsInLevel(level) - 1;
                    while (l <= r) {
                        KeyType m = (l + r) / 2;
                        if (this->getKey(level, m) == key) {
                            if (!range) this->stats.bloomTruePositives++;
                            return m;
                        } else if (this->getKey(level, m) < key) {
                            l = m + 1;
                        } else {
                            r = m - 1;
                        }
                    }
                    if (range) {
                        if (l > (KeyType)this->getPairsInLevel(level)) l = this->getPairsInLevel(level);
                        return l;
                    }
                }
            }

            this->stats.bloomFalsePositives++;
            // std::cout << "Bloom False Positive. ";
            return -1;
        }

        // `clearLevel()`
        // Clears the specified level by resetting the number of pairs to 0, deleting the fence, and
        // clearing the bloom filter. Does not reset all values in the level array.
        void clearLevel(size_t l) {
            this->getLevel(l)->numPairs = 0;
            delete[] this->getLevel(l)->fence;
            this->getLevel(l)->fence = nullptr;
            this->getLevel(l)->fenceLength = 0;
            this->getLevel(l)->bloomFilter->clear();

            // Clear the dictionary.
            this->getLevel(l)->dict.clear();
            this->getLevel(l)->dictReverse.clear();
        }

        // `propagateData()`
        // Moves all of the data at level l to level l + 1, then wipes level l.
        void propagateData(size_t l) {
            for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                this->appendPair(l + 1, this->getKey(l, i), this->getVal(l, i), this->getTomb(l, i));
            }
            this->clearLevel(l);
            this->sortLevel(l + 1);
        }

        // `propogateLevel()`
        // Writes all the KV pairs from level l to level l + 1, then resets level l.
        void propagateLevel(size_t l) {
            if (l == this->getNumLevels() - 1) {
                // We need to initialize a new level at the bottom of the tree then copy everything into it.
                KeyType* keysPointer = mmapLevel<KeyType>(("data/k" + std::to_string(l + 1) + ".data").c_str(), l + 1);
                void* valsPointer = nullptr;
                if (ENCODING_TYPE == ENCODING_OFF) valsPointer = mmapLevel<ValType>(("data/v" + std::to_string(l + 1) + ".data").c_str(), l + 1);
                else if (ENCODING_TYPE == ENCODING_DICT) valsPointer = mmapLevel<DictValType>(("data/v" + std::to_string(l + 1) + ".data").c_str(), l + 1);
                bool* tombstonePointer = mmapLevel<bool>(("data/t" + std::to_string(l + 1) + ".data").c_str(), l + 1);
                this->initializeLevel(l + 1, keysPointer, valsPointer, tombstonePointer, 0);
            }
            this->propagateData(l);
        }
};

#endif
