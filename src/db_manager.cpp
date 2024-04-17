#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cmath>

#include "lsm.hpp"

std::map<int, int> map;
Catalog catalog = {
    .bufferSize = BUFFER_SIZE,
    .numLevels = 0,
    .sizeRatio = SIZE_RATIO,
    .levels = {nullptr},
    .pairsInLevel = {0},
    .fence = {nullptr},
    .fenceLength = {0},
    .bloomfilters = {nullptr},
};

Stats stats = {
    .puts = 0,
    .successfulGets = 0,
    .failedGets = 0,
};

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
void constructFence(size_t l) {
    if (l == 0) return;

    if (catalog.fence[l] != nullptr) {
        delete[] catalog.fence[l];
    }

    catalog.fenceLength[l] = std::ceil(static_cast<double>(catalog.pairsInLevel[l] / catalog.bufferSize));

    catalog.fence[l] = new int[catalog.fenceLength[l]];
    for (size_t i = 0, j = 0; i < catalog.fenceLength[l]; i++, j += catalog.bufferSize) {
        if (j >= 2 * catalog.pairsInLevel[l]) {
            std::cout << "constructFence(): Out of bounds access error." << std::endl;
            return;
        }
        catalog.fence[l][i] = catalog.levels[l][2 * j];
    }

    // TODO: Free fence pointers in shutdownServer().
}

// `constructBloomFilter()`
// Constructs a bloom filter boolean vector over the keys for the specified level based on the current state of the level. 
// Called at the end of `propagateLevel()` and in `populateCatalog()`.
void constructBloomFilter(size_t level) {
    size_t numBits = catalog.bufferSize * BLOOM_BITS_PER_ENTRY * std::pow(catalog.sizeRatio, level);
    if (catalog.bloomfilters[level] != nullptr) {
        catalog.bloomfilters[level]->clear();
    } else {
        catalog.bloomfilters[level] = new BloomFilter(numBits, numBits * log(2) / catalog.bufferSize);
    }

    if (catalog.levels[level] != nullptr) {
        for (size_t i = 0; i < catalog.pairsInLevel[level]; i++) {
            catalog.bloomfilters[level]->add(catalog.levels[level][2 * i]);
        }
    }
}

// `sortLevel()`
// Sorts the level indicated.
void sortLevel(size_t l) {
    if (l >= catalog.numLevels) {
        std::cout << "sortLevel(): Tried to sort nonexistent level." << std::endl;
        return;
    }

    std::vector<std::pair<int, int>> levelVector;
    for (size_t i = 0; i < catalog.pairsInLevel[l]; i++) {
        levelVector.push_back({catalog.levels[l][2 * i], catalog.levels[l][2 * i + 1]});
    }

    std::sort(levelVector.begin(), levelVector.end(), [](const std::pair<int, int>& x, const std::pair<int, int>& y) {
        return x.first < y.first;
    });

    for (size_t i = 0; i < catalog.pairsInLevel[l]; i++) {
        catalog.levels[l][2 * i] = levelVector[i].first;
        catalog.levels[l][2 * i + 1] = levelVector[i].second;
    }

    constructFence(l);
}

// `propogateLevel()`
// Writes all the KV pairs from level l to level l + 1, then resets level l.
void propogateLevel(size_t l) {
    if (l == catalog.numLevels - 1) {
        // We need to create the next level, then copy everything into it.
        std::string dataFileName = "data/l" + std::to_string(l + 1) + ".data";
        int fd = open(dataFileName.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
        size_t fileSize = 2 * catalog.bufferSize * std::pow(catalog.sizeRatio, l + 1) * sizeof(int);
        ftruncate(fd, fileSize);
        void* levelPointer = mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        catalog.levels[l + 1] = reinterpret_cast<int*>(levelPointer);

        std::memcpy(catalog.levels[l + 1], catalog.levels[l], 2 * catalog.pairsInLevel[l] * sizeof(int));

        catalog.pairsInLevel[l + 1] = catalog.pairsInLevel[l];
        catalog.pairsInLevel[l] = 0;

        catalog.numLevels++;

        sortLevel(l + 1);
    } else {
        // Level l + 1 already exists, so we can just write to it.
        for (size_t i = 0; i < catalog.pairsInLevel[l]; i++) {
            catalog.levels[l + 1][2 * (catalog.pairsInLevel[l + 1] + i)] = catalog.levels[l][2 * i];
            catalog.levels[l + 1][2 * (catalog.pairsInLevel[l + 1] + i) + 1] = catalog.levels[l][2 * i + 1];
        }
        catalog.pairsInLevel[l + 1] += catalog.pairsInLevel[l];

        size_t sizeL = catalog.pairsInLevel[l];
        catalog.pairsInLevel[l] = 0;

        sortLevel(l + 1);

        if (catalog.pairsInLevel[l + 1] == sizeL * catalog.sizeRatio) propogateLevel(l + 1);
    }

    if (l > 0 && catalog.fence[l] != nullptr) {
        delete[] catalog.fence[l];
        catalog.fence[l] = nullptr;
        catalog.fenceLength[l] = 0;
    }

    catalog.bloomfilters[l]->clear();
    constructBloomFilter(l + 1);
}

int searchFence(size_t level, int key) {
    // Binary search through the fence pointers to get the target page.
    int l = 0, r = catalog.fenceLength[level] - 1;
    while (l <= r) {

        // The target page is the final page.
        if (l == (int)catalog.fenceLength[level] - 1) break;

        int m = (l + r) / 2;
        if (catalog.fence[level][m] <= key && key < catalog.fence[level][m + 1]) {
            return m;
        } else if (catalog.fence[level][m] < key) {
            l = m + 1;
        } else {
            r = m - 1;
        }
    }

    return r;
}

bool searchBloomFilter(size_t level, int key) {
    return catalog.bloomfilters[level]->mayContain(key);
}

// `searchLevel()`
// Searches for a key within level l of the LSM tree. Returns the index i
// of the key if it exists within the level, or -1 otherwise. To extract the
// value associated with the key, use `catalog.levels[l][2 * i + 1]`.
int searchLevel(size_t level, int key) {

    stats.searchLevelCalls++;

    if (!searchBloomFilter(level, key)) return -1;

    // The buffer, l0, is not sorted by key. All layers beneath l0 are sorted by key.

    if (level == 0) {
        for (size_t i = 0; i < catalog.pairsInLevel[level]; i++) {
            if (catalog.levels[level][2 * i] == key) {
                stats.bloomTruePositives++;
                return i;
            }
        }
    } else if (catalog.fence[level] != nullptr) {
        // If catalog.fence[level] exists, the level is non-empty.
        int pageIndex = searchFence(level, key);
        if (pageIndex != -1) {
            // Binary search within the page.
            int l = pageIndex * catalog.bufferSize;
            int r = std::min((pageIndex + 1) * catalog.bufferSize, catalog.pairsInLevel[level] - 1);
            while (l <= r) {
                int m = (l + r) / 2;
                if (catalog.levels[level][2 * m] == key) {
                    stats.bloomTruePositives++;
                    return m;
                } else if (catalog.levels[level][2 * m] < key) {
                    l = m + 1;
                } else {
                    r = m - 1;
                }
            }
        }
    }

    stats.bloomFalsePositives++;
    // std::cout << "Bloom False Positive. ";
    return -1;
}

// `mapPut()`
// This version of put uses a map. Used for debugging and development.
// std::tuple<Status, std::string> put(Status status, int key, int val) {
//     map[key] = val;
//     return std::make_tuple(status, "");
// }

// `mapGet()`
// This version of get uses a map. Used for debugging and development.
// std::tuple<Status, std::string> get(Status status, int key) {
//     if (map.find(key) == map.end()) {
//         std::cout << key << " is not a member of the LSM tree." << std::endl;
//         return std::make_tuple(status, "");
//     } else {
//         int val = map[key];
//         std::cout << key << " maps to " << val << std::endl;
//         return std::make_tuple(status, std::to_string(val));
//     }
// }

// `put()`
// Put a key and value into the LSM tree. If the key already exists, update the value.
std::tuple<Status, std::string> put(Status status, int key, int val) {

    stats.puts++;

    // Search through each level of the LSM tree. If the key already exists, update it.
    for (size_t l = 0; l < catalog.numLevels; l++) {
        int i = searchLevel(l, key);
        if (i >= 0) {
            catalog.levels[l][2 * i + 1] = val;
            return std::make_tuple(status, "");
        }
    }

    catalog.levels[0][2 * catalog.pairsInLevel[0]] = key;
    catalog.levels[0][2 * catalog.pairsInLevel[0] + 1] = val;
    catalog.pairsInLevel[0]++;
    catalog.bloomfilters[0]->add(key);

    if (catalog.pairsInLevel[0] == catalog.bufferSize) {
        propogateLevel(0);
    }

    return std::make_tuple(status, "");
}

// `get()`
// Search the LSM tree for a key.
std::tuple<Status, std::string> get(Status status, int key) {

    // Search through each level of the LSM tree.
    for (size_t l = 0; l < catalog.numLevels; l++) {
        int i = searchLevel(l, key);
        if (i >= 0) {
            std::cout << "Level " << l << ": " << key << " maps to " << catalog.levels[l][2 * i + 1] << std::endl;
            stats.successfulGets++;
            return std::make_tuple(status, std::to_string(catalog.levels[l][2 * i + 1]));
        }
    }

    std::cout << key << " is not a member of the LSM tree." << std::endl;
    stats.failedGets++;
    return std::make_tuple(status, "");
}

void printLevels(std::string userCommand) {

    std::cout << "\n———————————————————————————————— " << std::endl;
    std::cout << "——————— Printing levels. ——————— " << std::endl;
    std::cout << "———————————————————————————————— \n" << std::endl;

    for (size_t l = 0; l < catalog.numLevels; l++) {
        int* level = catalog.levels[l];

        if (l == 0) std::cout << "\n ——————— Buffer ——————— " << std::endl;
        else std::cout << "\n ——————— Level " << l << " ——————— " << std::endl;

        std::cout << "Contains: " << catalog.pairsInLevel[l] << " KV pairs = " << 2 * catalog.pairsInLevel[l] * sizeof(int) << " bytes." << std::endl;
        std::cout << "Capacity: " << catalog.bufferSize * std::pow(catalog.sizeRatio, l) << " KV pairs = " << 2 * catalog.bufferSize * std::pow(catalog.sizeRatio, l) * sizeof(int)  << " bytes." << std::endl;

        if (userCommand == "pv") {
            // Verbose printing.
            if (l == 0) {
                std::cout << "Buffer is unsorted. No fence pointers." << std::endl;
            } else if (catalog.fence[l] != nullptr) {
                std::cout << "Fence: [";
                for (int i = 0; i < (int)catalog.fenceLength[l] - 1; i++) {
                    std::cout << catalog.fence[l][i] << ", ";
                }
                std::cout << catalog.fence[l][catalog.fenceLength[l] - 1] << "]" << std::endl;
            }
            std::cout << "Bloom: [";
            for (int i = 0; i < (int)catalog.bloomfilters[l]->numBits() - 1; i++) {
                std::cout << catalog.bloomfilters[l]->getBit(i) << ", ";
            }
            std::cout << catalog.bloomfilters[l]->getBit(catalog.bloomfilters[l]->numBits() - 1) << "]" << std::endl;
            for (size_t i = 0; i < catalog.pairsInLevel[l]; i++) {
                std::cout << level[2 * i] << " -> " << level[2 * i + 1] << std::endl;
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
    } else {
        return std::make_tuple(status,
                "Supported commands: \n\n\
                p x y — PUT\n\
                g x   — GET\n\
                p     — Print levels to server.\n\
                pv    — Print levels to server (verbose).\n\
                s     — Shutdown and persist.\n\
                sw    — Shutdown and wipe all data.\n"
            );
    }
}
