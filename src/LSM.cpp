#include <string>
#include <sstream>
#include <iostream>
#include <cassert>

#include "Types.hpp"
#include "LSM.hpp"
#include "Utils.hpp"

// std::map<KEY_TYPE, VAL_TYPE> map;
LSM<KEY_TYPE, VAL_TYPE> lsm;
Stats stats;

// Uncomment the functions below and comment out the LSM tree versions
// to see how the system performs when the underlying implementation is a map.

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
std::tuple<Status, std::string> put(Status status, KEY_TYPE key, VAL_TYPE val) {

    stats.puts++;

    lsm.appendPair(0, key, val);
    return std::make_tuple(status, "");
}

// `get()`
// Search the LSM tree for a key.
std::tuple<Status, std::string> get(Status status, KEY_TYPE key) {

    // Search through each level of the LSM tree.
    for (size_t l = 0; l < lsm.getNumLevels(); l++) {
        int i = lsm.searchLevel(l, key, false);
        if (i >= 0) {
            stats.successfulGets++;
            return std::make_tuple(status, std::to_string(lsm.getVal(l, i)));
        }
    }

    stats.failedGets++;
    return std::make_tuple(status, "");
}

// `range()`
// Conduct a range query within the LSM tree.
std::tuple<Status, std::string> range(Status status, KEY_TYPE leftBound, KEY_TYPE rightBound) {

    stats.ranges++;

    std::map<KEY_TYPE, VAL_TYPE> results;

    // A range query must search through every level of the LSM tree. We iterate in reverse so that
    // only the most recent duplicate KV pair is retrieved in the case of duplicate entries.
    for (int l = lsm.getNumLevels() - 1; l >= 0; l--) {
        if (l == 0) {
            for (size_t i = 0; i < lsm.getPairsInLevel(0); i++) {
                if ((leftBound <= lsm.getKey(l, i)) && (lsm.getKey(l, i) < rightBound)) {
                    results[lsm.getKey(l, i)] = lsm.getVal(l, i);
                }
            }
        } else {
            int startIndex = lsm.searchLevel(l, leftBound, true);
            int endIndex = lsm.searchLevel(l, rightBound, true);
            for (int i = startIndex; i < endIndex; i++) {
                results[lsm.getKey(l, i)] = lsm.getVal(l, i);
            }
        }
    }

    std::cout << "Range query bounds: [" << leftBound << ", " << rightBound << "], Range query size: " << results.size() << std::endl;
    stats.rangeLengthSum += results.size();
    return std::make_tuple(status, mapToString(results));
}

void printLevels(std::string userCommand) {

    std::cout << "\n———————————————————————————————— " << std::endl;
    std::cout << "——————— Printing levels. ——————— " << std::endl;
    std::cout << "———————————————————————————————— \n" << std::endl;

    for (size_t l = 0; l < lsm.getNumLevels(); l++) {
        if (l == 0) std::cout << "\n ——————— Buffer ——————— " << std::endl;
        else std::cout << "\n ——————— Level " << l << " ——————— " << std::endl;

        std::cout << "Contains: " << lsm.getPairsInLevel(l) << " KV pairs = " << lsm.getPairsInLevel(l) * (sizeof(KEY_TYPE) + sizeof(VAL_TYPE)) << " bytes." << std::endl;
        std::cout << "Capacity: " << lsm.getLevelCapacity(l) << " KV pairs = " << lsm.getLevelCapacity(l) * (sizeof(KEY_TYPE) + sizeof(VAL_TYPE)) << " bytes." << std::endl;

        if (userCommand == "pv") {
            // Verbose printing.
            if (l == 0) {
                std::cout << "Buffer is unsorted. No fence pointers." << std::endl;
            } else if (!lsm.levelIsEmpty(l)) {
                std::cout << "Fence: [";
                for (size_t i = 0; i < lsm.getFenceLength(l) - 1; i++) {
                    std::cout << lsm.getFenceKey(l, i) << ", ";
                }
                std::cout << lsm.getFenceKey(l, lsm.getFenceLength(l) - 1) << "]" << std::endl;
            }
            std::cout << "Bloom: [";
            for (size_t i = 0; i < lsm.getBloomFilter(l)->numBits() - 1; i++) {
                std::cout << lsm.getBloomFilter(l)->getBit(i) << ", ";
            }
            std::cout << lsm.getBloomFilter(l)->getBit(lsm.getBloomFilter(l)->numBits() - 1) << "]" << std::endl;
            for (size_t i = 0; i < lsm.getPairsInLevel(l); i++) {
                std::cout << lsm.getKey(l, i) << " -> " << lsm.getVal(l, i) << std::endl;
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
        return put(status, std::stoi(tokens[1]), std::stoi(tokens[2]));
    } else if (tokens[0] == "g" && tokens.size() == 2 && isNum(tokens[1])) {
        // std::cout << "Received  get command.\n" << std::endl;
        return get(status, std::stoi(tokens[1]));
    } else if (tokens[0] == "r" && tokens.size() == 3 && isNum(tokens[1]) && isNum(tokens[2])) {
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
