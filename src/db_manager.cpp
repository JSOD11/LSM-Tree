#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cmath>
#include <cassert>

#include "lsm.hpp"
#include "db_manager.hpp"

std::map<int, int> map;
Catalog catalog;
Stats stats;

size_t Catalog::getBufferSize() {
    return this->bufferSize;
}

size_t Catalog::getNumLevels() {
    return this->numLevels;
}

size_t Catalog::getSizeRatio() {
    return this->sizeRatio;
}

int* Catalog::getLevel(size_t l) {
    return this->levels[l];
}

size_t Catalog::getPairsInLevel(size_t l) {
    return this->pairsInLevel[l];
}

bool Catalog::levelIsEmpty(size_t l) {
    return this->getPairsInLevel(l) == 0;
}

int Catalog::getFenceKey(size_t l, size_t index) {
    assert(this->fence[l] != nullptr);
    return this->fence[l][index];
}

size_t Catalog::getFenceLength(size_t l) {
    return this->fenceLength[l];
}

BloomFilter* Catalog::getBloomFilter(size_t l) {
    return this->bloomfilters[l];
}

void Catalog::initializeLevel(size_t l, int* levelPointer, size_t numPairs) {
    this->levels[l] = levelPointer;
    this->pairsInLevel[l] = numPairs;
    this->numLevels++;

    this->constructFence(l);
    this->constructBloomFilter(l);
}

// `appendPair()`
// Appends a new KV pair at the end of the specified level.
void Catalog::appendPair(size_t level, int key, int val) {
    this->levels[level][2 * this->getPairsInLevel(level)] = key;
    this->levels[level][2 * this->getPairsInLevel(level) + 1] = val;
    this->pairsInLevel[level]++;
    this->bloomfilters[level]->add(key);
    return;
}

// `insertPair()`
// Inserts a KV pair at the specified index within a level. Be careful with this
// and only use it when redesigning an entire level, or only modifying values,
// as it does not update bloom filters.
void Catalog::insertPair(size_t level, size_t pairIndex, int key, int val) {
    this->levels[level][2 * pairIndex] = key;
    this->levels[level][2 * pairIndex + 1] = val;
    return;
}

int Catalog::getKey(size_t level, size_t entryIndex) {
    return this->levels[level][2 * entryIndex];
}

int Catalog::getVal(size_t level, size_t entryIndex) {
    return this->levels[level][2 * entryIndex + 1];
}

// `mmapLevel()`
// Takes in the name of a file for a level and the level number. Returns an int* pointing to the
// start of the level array.
int* mmapLevel(const char* fileName, size_t level) {
    int fd = open(fileName, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    size_t fileSize = 2 * catalog.getBufferSize() * sizeof(int) * std::pow(catalog.getSizeRatio(), level);
    ftruncate(fd, fileSize);
    return reinterpret_cast<int*>(mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
}

std::vector<std::string> parseCommand(std::string userCommand) {
    std::vector<std::string> tokens;
    std::istringstream stringStream(userCommand);
    std::string token;

    while (stringStream >> token) {
        tokens.push_back(token);
    }

    return tokens;
}

bool isInt(const std::string& str) {
    std::istringstream iss(str);
    int num;
    iss >> num;
    return iss.eof() && !iss.fail(); 
}

// `constructFence()`
// Constructs the fence pointer array at level l within the catalog.
void Catalog::constructFence(size_t l) {
    if (l == 0) return;

    if (!this->levelIsEmpty(l)) {
        delete[] catalog.fence[l];
    }

    catalog.fenceLength[l] = std::ceil(static_cast<double>(catalog.pairsInLevel[l]) / catalog.pageSize);

    catalog.fence[l] = new int[catalog.fenceLength[l]];
    for (size_t i = 0, j = 0; i < catalog.fenceLength[l]; i++, j += catalog.pageSize) {
        if (j >= catalog.pairsInLevel[l]) {
            std::cout << "constructFence(): Out of bounds access error." << std::endl;
            return;
        }
        catalog.fence[l][i] = catalog.getKey(l, j);
    }

    // TODO: Free fence pointers in shutdownServer().
}

// `constructBloomFilter()`
// Constructs a bloom filter boolean vector over the keys for the specified level based on the current state of the level. 
// Called at the end of `propagateLevel()` and in `populateCatalog()`.
void Catalog::constructBloomFilter(size_t level) {
    size_t levelSize = catalog.bufferSize * std::pow(catalog.sizeRatio, level);
    size_t numBits = static_cast<size_t>(-(levelSize * std::log(BLOOM_TARGET_FPR)) / std::pow(std::log(2), 2));
    
    // Calculate the optimal number of hash functions.
    size_t numHashes = static_cast<size_t>((numBits / static_cast<double>(levelSize)) * std::log(2));
    if (numHashes < 1) numHashes = 1;

    if (catalog.bloomfilters[level] != nullptr) {
        catalog.bloomfilters[level]->clear();
    } else {
        catalog.bloomfilters[level] = new BloomFilter(numBits, numHashes);
    }

    if (catalog.levels[level] != nullptr) {
        for (size_t i = 0; i < catalog.pairsInLevel[level]; i++) {
            catalog.bloomfilters[level]->add(catalog.levels[level][2 * i]);
        }
    }

    // TODO: Free bloom filters in shutdownServer().
}

// `sortLevel()`
// Sorts the level indicated. Also handles fence pointers and bloom filter.
void sortLevel(size_t l) {
    if (l >= catalog.getNumLevels()) {
        std::cout << "sortLevel(): Tried to sort nonexistent level." << std::endl;
        return;
    }

    std::vector<std::pair<int, int>> levelVector;
    for (size_t i = 0; i < catalog.getPairsInLevel(l); i++) {
        levelVector.push_back({catalog.getKey(l, i), catalog.getVal(l, i)});
    }

    std::sort(levelVector.begin(), levelVector.end(), [](const std::pair<int, int>& x, const std::pair<int, int>& y) {
        return x.first < y.first;
    });

    for (size_t i = 0; i < catalog.getPairsInLevel(l); i++) {
        catalog.insertPair(l, i, levelVector[i].first, levelVector[i].second);
    }

    catalog.constructFence(l);
    catalog.constructBloomFilter(l + 1);
}

// `clearLevel()`
// Clears the specified level by resetting the number of pairs to 0, deleting the fence, and
// clearing the bloom filter. Does not reset all values in the level array.
void Catalog::clearLevel(size_t l) {
    this->pairsInLevel[l] = 0;
    if (l > 0 && catalog.fence[l] != nullptr) {
        delete[] catalog.fence[l];
        catalog.fence[l] = nullptr;
        catalog.fenceLength[l] = 0;
    }
    catalog.bloomfilters[l]->clear();
}

// `propagateData()`
// Moves all of the data at level l to level l + 1, then wipes level l.
void propagateData(size_t l) {
    for (size_t i = 0; i < catalog.getPairsInLevel(l); i++) {
        catalog.appendPair(l + 1, catalog.getKey(l, i), catalog.getVal(l, i));
    }
    catalog.clearLevel(l);
    sortLevel(l + 1);
}

// `propogateLevel()`
// Writes all the KV pairs from level l to level l + 1, then resets level l.
void propagateLevel(size_t l) {

    if (l == 9) {
        std::cout << "LSM tree currently has a finite size, which has been reached. Shutting down." << std::endl;
        std::abort();
    }

    if (l == catalog.getNumLevels() - 1) {
        // We need to create the next level, then copy everything into it.
        int* levelPointer = mmapLevel(("data/l" + std::to_string(l + 1) + ".data").c_str(), l + 1);
        catalog.initializeLevel(l + 1, levelPointer, 0);
        propagateData(l);
    } else {
        // Level l + 1 already exists, so we can just write to it.
        propagateData(l);
        if (catalog.getPairsInLevel(l + 1) == catalog.getBufferSize() * std::pow(catalog.getSizeRatio(), l + 1)) propagateLevel(l + 1);
    }
    catalog.clearLevel(l);
}

// `searchFence()`
// Searches through the fence pointers at level l for the specified key.
// Returns the page on which the key will be found if it exists.
int searchFence(size_t level, int key) {
    // Binary search through the fence pointers to get the target page.
    int l = 0, r = catalog.getFenceLength(level) - 1;
    while (l <= r) {

        // The target page is the final page.
        if (l == (int)catalog.getFenceLength(level)- 1) break;

        int m = (l + r) / 2;
        if (catalog.getFenceKey(level, m) <= key && key < catalog.getFenceKey(level, m + 1)) {
            return m;
        } else if (catalog.getFenceKey(level, m) < key) {
            l = m + 1;
        } else {
            r = m - 1;
        }
    }

    return r;
}

bool searchBloomFilter(size_t level, int key) {
    return catalog.getBloomFilter(level)->mayContain(key);
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
int searchLevel(size_t level, int key, bool range) {

    if (!range) stats.searchLevelCalls++;
    if (!range) {
        if (catalog.levelIsEmpty(level) || !searchBloomFilter(level, key)) return -1;
    } else {
        // Bound is outside the range of keys in the level. Return 0 or len(level) - 1.
        if (catalog.levelIsEmpty(level) || key < catalog.getKey(level, 0)) return 0;
        if (key > catalog.getKey(level, catalog.getPairsInLevel(level) - 1)) return catalog.getPairsInLevel(level);
    }

    // The buffer, l0, is not sorted by key. All layers beneath l0 are sorted by key.

    if (level == 0) {
        for (size_t i = 0; i < catalog.getPairsInLevel(level); i++) {
            if (catalog.getKey(level, i) == key) {
                stats.bloomTruePositives++;
                return i;
            }
        }
    } else if (catalog.levelIsEmpty(level)) {
        // If the level is empty and we're doing a range query, we just return 0 for both
        // the leftBound index and rightBound index.
        if (range) return 0;
    } else {
        int pageIndex = searchFence(level, key);
        if (pageIndex != -1) {
            // Binary search within the page.
            int l = pageIndex * catalog.getBufferSize();
            int r = std::min((pageIndex + 1) * catalog.getBufferSize(), catalog.getPairsInLevel(level) - 1);
            while (l <= r) {
                int m = (l + r) / 2;
                if (catalog.getKey(level, m) == key) {
                    stats.bloomTruePositives++;
                    return m;
                } else if (catalog.getKey(level, m) < key) {
                    l = m + 1;
                } else {
                    r = m - 1;
                }
            }
            if (range) {
                if (l > (int)catalog.getPairsInLevel(level)) l = catalog.getPairsInLevel(level);
                return l;
            }
        }
    }

    stats.bloomFalsePositives++;
    // std::cout << "Bloom False Positive. ";
    return -1;
}

std::string vectorToString(const std::vector<int>& vec) {
    std::stringstream ss;
    ss << "[";
    if (!vec.empty()) {
        for (size_t i = 0; i < vec.size() - 1; ++i) {
            ss << vec[i] << ", ";
        }
        ss << vec.back();
    }
    ss << "]";
    return ss.str();
}

// Uncomment the two functions below and comment out the LSM tree versions in order
// to observe how the system performs when the underlying implementation is a map.

// `put()`
// This version of put uses a map. Used for debugging and development.
// std::tuple<Status, std::string> put(Status status, int key, int val) {
//     stats.puts++;
//     map[key] = val;
//     return std::make_tuple(status, "");
// }

// `get()`
// This version of get uses a map. Used for debugging and development.
// std::tuple<Status, std::string> get(Status status, int key) {
//     if (map.find(key) == map.end()) {
//         // std::cout << key << " is not a member of the LSM tree." << std::endl;
//         stats.failedGets++;
//         return std::make_tuple(status, "");
//     } else {
//         int val = map[key];
//         stats.successfulGets++;
//         // std::cout << key << " maps to " << val << std::endl;
//         return std::make_tuple(status, std::to_string(val));
//     }
// }

// `range()`
// This version of range uses a map.
// std::tuple<Status, std::string> range(Status status, int leftBound, int rightBound) {

//     stats.ranges++;

//     std::vector<int> results;

//     for (int i = leftBound; i < rightBound; i++) {
//         if (map.find(i) != map.end()) {
//             results.push_back(map[i]);
//         }
//     }

//     std::cout << "Range query size: " << results.size() << std::endl;
//     stats.rangeLengthSum += results.size();
//     return std::make_tuple(status, vectorToString(results));
// }

// `put()`
// Put a key and value into the LSM tree. If the key already exists, update the value.
std::tuple<Status, std::string> put(Status status, int key, int val) {

    stats.puts++;

    // Search through each level of the LSM tree. If the key already exists, update it.
    for (size_t l = 0; l < catalog.getNumLevels(); l++) {
        int i = searchLevel(l, key, false);
        if (i >= 0) {
            catalog.insertPair(l, i, key, val);
            return std::make_tuple(status, "");
        }
    }

    catalog.appendPair(0, key, val);
    if (catalog.getPairsInLevel(0) == catalog.getBufferSize()) propagateLevel(0);

    return std::make_tuple(status, "");
}

// `get()`
// Search the LSM tree for a key.
std::tuple<Status, std::string> get(Status status, int key) {

    // Search through each level of the LSM tree.
    for (size_t l = 0; l < catalog.getNumLevels(); l++) {
        int i = searchLevel(l, key, false);
        if (i >= 0) {
            stats.successfulGets++;
            return std::make_tuple(status, std::to_string(catalog.getVal(l, i)));
        }
    }

    stats.failedGets++;
    return std::make_tuple(status, "");
}

// `range()`
// Conduct a range query within the LSM tree.
std::tuple<Status, std::string> range(Status status, int leftBound, int rightBound) {

    stats.ranges++;

    std::vector<int> results;

    // A range query must search through every level of the LSM tree.
    for (size_t l = 0; l < catalog.getNumLevels(); l++) {
        if (l == 0) {
            for (size_t i = 0; i < catalog.getPairsInLevel(0); i++) {
                if ((leftBound <= catalog.getKey(l, i)) && (catalog.getKey(l, i) < rightBound)) {
                    results.push_back(catalog.getVal(l, i));
                }
            }
        } else {
            size_t startIndex = searchLevel(l, leftBound, true);
            size_t endIndex = searchLevel(l, rightBound, true);
            for (size_t i = startIndex; i < endIndex; i++) {
                results.push_back(catalog.getVal(l, i));
            }
        }
    }

    std::cout << "Range query bounds: [" << leftBound << ", " << rightBound << "], Range query size: " << results.size() << std::endl;
    stats.rangeLengthSum += results.size();
    return std::make_tuple(status, vectorToString(results));
}

void printLevels(std::string userCommand) {

    std::cout << "\n———————————————————————————————— " << std::endl;
    std::cout << "——————— Printing levels. ——————— " << std::endl;
    std::cout << "———————————————————————————————— \n" << std::endl;

    for (size_t l = 0; l < catalog.getNumLevels(); l++) {
        if (l == 0) std::cout << "\n ——————— Buffer ——————— " << std::endl;
        else std::cout << "\n ——————— Level " << l << " ——————— " << std::endl;

        std::cout << "Contains: " << catalog.getPairsInLevel(l) << " KV pairs = " << 2 * catalog.getPairsInLevel(l) * sizeof(int) << " bytes." << std::endl;
        std::cout << "Capacity: " << catalog.getBufferSize() * std::pow(catalog.getSizeRatio(), l) << " KV pairs = " << 2 * catalog.getBufferSize() * std::pow(catalog.getSizeRatio(), l) * sizeof(int)  << " bytes." << std::endl;

        if (userCommand == "pv") {
            // Verbose printing.
            if (l == 0) {
                std::cout << "Buffer is unsorted. No fence pointers." << std::endl;
            } else if (!catalog.levelIsEmpty(l)) {
                std::cout << "Fence: [";
                for (int i = 0; i < (int)catalog.getFenceLength(l) - 1; i++) {
                    std::cout << catalog.getFenceKey(l, i) << ", ";
                }
                std::cout << catalog.getFenceKey(l, catalog.getFenceLength(l) - 1) << "]" << std::endl;
            }
            std::cout << "Bloom: [";
            for (int i = 0; i < (int)catalog.getBloomFilter(l)->numBits() - 1; i++) {
                std::cout << catalog.getBloomFilter(l)->getBit(i) << ", ";
            }
            std::cout << catalog.getBloomFilter(l)->getBit(catalog.getBloomFilter(l)->numBits() - 1) << "]" << std::endl;
            for (size_t i = 0; i < catalog.getPairsInLevel(l); i++) {
                std::cout << catalog.getKey(l, i) << " -> " << catalog.getVal(l, i) << std::endl;
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
    if (tokens[0] == "p" && tokens.size() == 3 && isInt(tokens[1]) && isInt(tokens[2])) {
        // std::cout << "Received put command.\n" <<  std::endl;
        return put(status, std::stoi(tokens[1]), std::stoi(tokens[2]));
    } else if (tokens[0] == "g" && tokens.size() == 2 && isInt(tokens[1])) {
        // std::cout << "Received  get command.\n" << std::endl;
        return get(status, std::stoi(tokens[1]));
    } else if (tokens[0] == "r" && tokens.size() == 3 && isInt(tokens[1]) && isInt(tokens[2])) {
        // std::cout << "Received range query command.\n" <<  std::endl;
        return range(status, std::stoi(tokens[1]), std::stoi(tokens[2]));
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
