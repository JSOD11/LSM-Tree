#include <string>
#include <sstream>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "lsm.hpp"

// std::map<int, int> map;
Catalog catalog = {
    .bufferSize = sysconf(_SC_PAGESIZE) / (2 * sizeof(int)),
    .numLevels = 0,
    .sizeRatio = 3,
    .levels = {nullptr},
    .pairsInLevel = {0},
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

// `mapPut()`
// This version of put uses a map. Used for debugging and development.
// std::tuple<Status, std::string> mapPut(Status status, int key, int val) {
//     map[key] = val;
//     return std::make_tuple(status, "");
// }

// `mapGet()`
// This version of get uses a map. Used for debugging and development.
// std::tuple<Status, std::string> mapGet(Status status, int key) {
//     if (map.find(key) == map.end()) {
//         std::cout << key << " is not a member of the LSM tree." << std::endl;
//         return std::make_tuple(status, "");
//     } else {
//         int val = map[key];
//         std::cout << key << " maps to " << val << std::endl;
//         return std::make_tuple(status, std::to_string(val));
//     }
// }

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
}

// `searchLevel()`
// Searches for a key within level l of the LSM tree. Returns the index i
// of the key if it exists within the level, or -1 otherwise. To extract the
// value associated with the key, use `catalog.levels[l][2 * i + 1]`.
int searchLevel(size_t level, int key) {

    if (level == 0) {
        // The buffer, l0, is not sorted by key.
        for (size_t i = 0; i < catalog.pairsInLevel[level]; i++) {
            if (catalog.levels[level][2 * i] == key) {
                return i;
            }
        }
    } else {
        // All layers beneath l0 are sorted by key.
        int l = 0, r = catalog.pairsInLevel[level] - 1;
        while (l <= r) {
            int m = (l + r) / 2;
            if (catalog.levels[level][2 * m] == key) {
                return m;
            } else if (catalog.levels[level][2 * m] < key) {
                l = m + 1;
            } else {
                r = m - 1;
            }
        }
    }

    return -1;
}

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
    std::cout << "\nPrinting levels." << std::endl;
    for (size_t i = 0; i < catalog.numLevels; i++) {
        int* level = catalog.levels[i];
        if (i == 0) std::cout << "\nBuffer " << " (" << catalog.bufferSize << " entries = " << 2 * catalog.bufferSize * sizeof(int) << " bytes). Unsorted." << std::endl;
        else std::cout << "\nLevel " << i << " (" << catalog.bufferSize * std::pow(catalog.sizeRatio, i) << " entries = " << 2 * catalog.bufferSize * std::pow(catalog.sizeRatio, i) * sizeof(int) << " bytes). Sorted." << std::endl;

        if (userCommand == "p") {
            std::cout << catalog.pairsInLevel[i] << " KV pairs. " << 2 * catalog.pairsInLevel[i] * sizeof(int) << " bytes." << std::endl;
        } else {
            for (size_t j = 0; j < catalog.pairsInLevel[i]; j++) {
                std::cout << level[2 * j] << " -> " << level[2 * j + 1] << std::endl;
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
        status = ERROR;
        return std::make_tuple(status, "Invalid command.");
    }
}
