#ifndef LSM_HPP
#define LSM_HPP

#include <string>
#include <tuple>
#include <cassert>
#include <map>

#include "Types.hpp"
#include "Utils.hpp"
#include "bloomfilter.hpp"
#include <unordered_map>
#include <map>
#include <chrono>
template<typename KeyType, typename ValType>
struct Level {
    KeyType* keys = nullptr;
    ValType* vals = nullptr;
    bool* tombstone = nullptr;
    size_t numPairs = 0;
    KeyType* fence = nullptr;
    size_t fenceLength = 0;
    BloomFilter* bloomFilter = nullptr;
    EncodingType encodingType = ENCODING_TYPE;
    std::map<int64_t, VAL_TYPE> dict;
    std::vector<int64_t> dictReverse;

    ~Level() {
        delete[] fence;
        delete bloomFilter;
    }
};

template<typename KeyType, typename ValType>
class LSM {
    private:
        // The page size is the number of entries in a page.
        size_t pageSize = PAGE_SIZE;
        // The buffer size is the number of entries in the buffer.
        // TODO: Fix bug when page size != buffer size.
        size_t bufferSize = BUFFER_PAGES * this->pageSize;
        size_t numLevels = 0;
        size_t sizeRatio = SIZE_RATIO;
        std::vector<Level<KeyType, ValType>*> levels = {};
    
    public:
        LSM() {
            assert(this->pageSize > 0);
            assert(this->getBufferSize() > 0);
            assert(this->getSizeRatio() > 0);
        }
        size_t getBufferSize() { return this->bufferSize; }
        size_t getNumLevels() { return this->numLevels; }
        size_t getSizeRatio() { return this->sizeRatio; }
        Level<KeyType, ValType>* getLevel(size_t l) { return this->levels[l]; }
        KeyType* getLevelKeys(size_t l) { return this->getLevel(l)->keys; }
        ValType* getLevelVals(size_t l) { return this->getLevel(l)->vals; }
        std::map<int64_t, VAL_TYPE>& getLevelDict(size_t l) { return this->getLevel(l)->dict; }
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
            std::map<KeyType, bool> vals;
            for (size_t i = 0; i < this->getPairsInLevel(l); i++) {
                vals[this->getVal(l, i)] = true;
            }
            return vals.size();
        }

        // `initializeLevel()`
        // This function is used when we intend to create a new empty level at the bottom of the LSM tree.
        void initializeLevel(size_t l, KeyType* keysPointer, ValType* valsPointer, bool* tombstonePointer, size_t numPairs) {
            assert(l == this->levels.size());
            Level<KeyType, ValType>* newLevel = new Level<KeyType, ValType>;
            newLevel->keys = keysPointer;
            newLevel->vals = valsPointer;
            newLevel->tombstone = tombstonePointer;
            newLevel->numPairs = numPairs;
            this->levels.push_back(newLevel);
            this->numLevels++;
            this->constructFence(l);
            this->constructBloomFilter(l);
        }

        // `appendPair()`
        // Appends a new KV pair at the end of the specified level.
        void appendPair(size_t l, KeyType key, ValType val, bool isDelete) {
            this->getLevelKeys(l)[this->getPairsInLevel(l)] = key;
            // check encoding type
            // get level 
            // printf("Encoding type: %d\n", this->getLevel(l)->encodingType);
            Level<KeyType, ValType>* level = this->getLevel(l);
            if(this->getLevel(l)->encodingType == DICT){
                // check if value is in dict, if not add it
               if(level->dict.find(val) == level->dict.end()){
                   level->dict[val] = level->dict.size();
                   level->dictReverse.push_back(val);
               }
               // printf("Value: %d\n", level->dict[val]);
                this->getLevelVals(l)[this->getPairsInLevel(l)] = level->dict[val];
            } else {
            this->getLevelVals(l)[this->getPairsInLevel(l)] = val;
            }
            this->getLevelTombstone(l)[this->getPairsInLevel(l)] = isDelete;
            this->getLevel(l)->numPairs++;
            this->getLevel(l)->bloomFilter->add(key);
            if (this->getPairsInLevel(l) == this->getLevelCapacity(l)) 
                this->propagateLevel(l);
            
            return;
        }

        // `getKey()`
        // Returns the key at the index specified in the level specified.
        KeyType getKey(size_t l, size_t entryIndex) {
            return this->getLevelKeys(l)[entryIndex];
        }

        // `getVal()`
        // Returns the value at the index specified in the level specified.
        ValType getVal(size_t l, size_t entryIndex) {
            // check encoding type 
            // first get level
            Level<KeyType, ValType>* level = this->getLevel(l);
            if(level->encodingType == DICT){
                return level->dictReverse[level->vals[entryIndex]];
            }
            return level->vals[entryIndex];
        }

        // `getTomb()`
        // Returns the tombstone bit at the index specified in the level specified. `1` means to delete.
        bool getTomb(size_t l, size_t entryIndex) {
            return this->getLevelTombstone(l)[entryIndex];
        }
        
        // `constructFence()`
        // Constructs the fence pointer array at level l within the catalog .
        void constructFence(size_t l) {
            if (l == 0) return;
            delete[] this->getLevel(l)->fence;
            this->getLevel(l)->fenceLength = std::ceil(static_cast<double>(this->getLevel(l)->numPairs) / this->pageSize);
            this->getLevel(l)->fence = new KeyType[this->getLevel(l)->fenceLength];
            for (size_t i = 0, j = 0; i < this->getLevel(l)->fenceLength; i++, j += this->pageSize) {
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

            if (!range) stats.searchLevelCalls++;
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
                        if (!range) stats.bloomTruePositives++;
                        return i;
                    }
                }
            } else {
                KeyType pageIndex = searchFence(level, key);
                if (pageIndex != -1) {
                    // Binary search within the page.
                    KeyType l = pageIndex * this->getBufferSize();
                    KeyType r = (pageIndex + 1) * this->getBufferSize();
                    if ((KeyType)this->getPairsInLevel(level) - 1 < r) r = (KeyType)this->getPairsInLevel(level) - 1;
                    while (l <= r) {
                        KeyType m = (l + r) / 2;
                        if (this->getKey(level, m) == key) {
                            if (!range) stats.bloomTruePositives++;
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

            stats.bloomFalsePositives++;
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
                ValType* valsPointer = mmapLevel<ValType>(("data/v" + std::to_string(l + 1) + ".data").c_str(), l + 1);
                bool* tombstonePointer = mmapLevel<bool>(("data/t" + std::to_string(l + 1) + ".data").c_str(), l + 1);
                this->initializeLevel(l + 1, keysPointer, valsPointer, tombstonePointer, 0);
            }
            this->propagateData(l);
        }
};

extern LSM<KEY_TYPE, VAL_TYPE> lsm;

template<typename T>
T* mmapLevel(const char* fileName, size_t l);
std::tuple<Status, std::string> processCommand(std::string);

#endif
