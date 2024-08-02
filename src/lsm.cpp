#include <string>
#include <sstream>
#include <iostream>
#include <cassert>
#include <chrono>

#include "Types.hpp"
#include "lsm.hpp"
#include "Utils.hpp"
#include <fstream>

LSM<KEY_TYPE, VAL_TYPE, DICT_VAL_TYPE> lsm;
Stats stats;

// `put()`
// Put a key and value into the LSM tree. If the key already exists, update the value.
// This function is also used for deletes by setting `isDelete = true`.
std::tuple<Status, std::string> put(Status status, KEY_TYPE key, VAL_TYPE val, bool isDelete) {
    if (!isDelete) stats.puts++;
    else stats.deletes++;
    lsm.appendPair(0, key, val, isDelete);
    return std::make_tuple(status, "");
}

// `get()`
// Search the LSM tree for a key.
std::tuple<Status, std::string> get(Status status, KEY_TYPE key) {

    // Search through each level of the LSM tree.
    for (size_t l = 0; l < lsm.getNumLevels(); l++) {
        int i = lsm.searchLevel(l, key, false);
        if (i >= 0) {
            if (lsm.getTomb(l, i)) break;
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
                    if (lsm.getTomb(l, i)) results.erase(lsm.getKey(l, i));
                }
            }
        } else {
            auto startSearch = std::chrono::high_resolution_clock::now();
            int startIndex = lsm.searchLevel(l, leftBound, true);
            int endIndex = lsm.searchLevel(l, rightBound, true);
            auto endSearch = std::chrono::high_resolution_clock::now();
            auto durationSearch = std::chrono::duration_cast<std::chrono::microseconds>(endSearch - startSearch);

            auto startRange = std::chrono::high_resolution_clock::now();
            for (int i = startIndex; i < endIndex; i++) {
                results[lsm.getKey(l, i)] = lsm.getVal(l, i);
                if (lsm.getTomb(l, i)) results.erase(lsm.getKey(l, i));
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
    stats.rangeLengthSum += results.size();
    if (TESTING_SWITCH == TESTING_ON) {
        for (const auto& pair : results) {
            stats.rangeValueSum = (stats.rangeValueSum + pair.second) % static_cast<VAL_TYPE>(std::pow(10, 6));
        }
    }
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
        std::cout << "Unique keys: " << lsm.getUniqueKeyCount(l) << ". Unique values: " << lsm.getUniqueValCount(l) << std::endl;
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
                std::cout << lsm.getKey(l, i) << " -> " << lsm.getVal(l, i) << "  " << lsm.getTomb(l, i) << std::endl;
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
