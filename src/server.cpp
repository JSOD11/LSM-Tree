#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <tuple>
#include <chrono>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <fstream>
#include <cmath>
#include <iomanip>

#include "Types.hpp"
#include "Utils.hpp"
#include "lsm.hpp"

// `populateCatalog()`
// Populates the catalog with persisted data or creates a data folder if one does not exist.
void populateCatalog(void) {
    // Create the data folder if it does not exist.
    if (!std::filesystem::exists("data")) std::filesystem::create_directory("data");

    if (!std::filesystem::exists("data/catalog.data")) {
        // The database is being started from scratch. We start just with l0.
        KEY_TYPE* keysPointer = mmapLevel<KEY_TYPE>("data/k0.data", 0);
        VAL_TYPE* valsPointer = mmapLevel<VAL_TYPE>("data/v0.data", 0);
        bool* tombstonePointer = mmapLevel<bool>("data/t0.data", 0);
        lsm.initializeLevel(0, keysPointer, valsPointer, tombstonePointer, 0);
        // std::cout << "Started new database from scratch.\n" << std::endl;
    } else {
        // We are populating the catalog with persisted data.
        std::ifstream catalogFile("data/catalog.data");
        size_t numPairs = 0, l = 0;
        while (catalogFile >> numPairs) {
            KEY_TYPE* keysPointer = mmapLevel<KEY_TYPE>(("data/k" + std::to_string(l) + ".data").c_str(), l);
            VAL_TYPE* valsPointer = mmapLevel<VAL_TYPE>(("data/v" + std::to_string(l) + ".data").c_str(), l);
            bool* tombstonePointer = mmapLevel<bool>(("data/t" + std::to_string(l) + ".data").c_str(), l);
            lsm.initializeLevel(l, keysPointer, valsPointer, tombstonePointer, numPairs);

            // Populate the dictionary from persisted dictionary files.
            std::ifstream dictStream ("data/dict" + std::to_string(l) + ".data");
            VAL_TYPE val;
            DICT_VAL_TYPE encodedVal;
            while (dictStream >> val >> encodedVal) {
                lsm.getLevel(l)->dict[val] = encodedVal;
            }
            dictStream.close();

            // Construct the dictReverse array.
            std::ifstream dictReverseStream ("data/dictreverse" + std::to_string(l) + ".data");
            VAL_TYPE valReverse;
            while (dictReverseStream >> valReverse) {
                lsm.getLevel(l)->dictReverse.push_back(valReverse);
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
        for (size_t i = 0; i < lsm.getNumLevels(); i++) {
            catalogFile << lsm.getPairsInLevel(i) << std::endl;
        }
        catalogFile.close();

        // Persist the dictionaries.
        for (size_t l = 0; l < lsm.getNumLevels(); l++) {
            std::cout << "Persisting dict for level " << l << std::endl;
            std::ofstream dictStream ("data/dict" + std::to_string(l) + ".data", std::ios::out | std::ios::trunc);
            for (const auto& x : lsm.getLevel(l)->dict) {
                dictStream << x.first << " " << x.second << std::endl;
            }
            dictStream.close();

            std::cout << "Persisting dictreverse for level " << l << std::endl;
            std::ofstream dictReverseStream ("data/dictreverse" + std::to_string(l) + ".data", std::ios::out | std::ios::trunc);
            for (size_t i = 0; i < lsm.getLevel(l)->dictReverse.size(); i++) {
                dictReverseStream << lsm.getLevel(l)->dictReverse[i] << std::endl;
            }
            dictReverseStream.close();
        }

        std::cout << "Persisted data folder." << std::endl;
    }

    for (size_t l = 0; l < lsm.getNumLevels(); l++) {
        munmap(lsm.getLevel(l)->keys, lsm.getPairsInLevel(l) * sizeof(KEY_TYPE));
        munmap(lsm.getLevel(l)->vals, lsm.getPairsInLevel(l) * sizeof(VAL_TYPE));
        munmap(lsm.getLevel(l)->tombstone, lsm.getPairsInLevel(l) * sizeof(bool));

        delete lsm.getLevel(l);
    }
}

void printStats(void) {
    // std::cout << "\n ——— Session statistics ——— \n" << std::endl;
    std::cout << "Puts: " << stats.puts << std::endl;
    std::cout << "Successful gets: " << stats.successfulGets << std::endl;
    std::cout << "Failed gets: " << stats.failedGets << std::endl;
    std::cout << "Ranges: " << stats.ranges << std::endl;
    std::cout << "Sum length of all ranges: " << stats.rangeLengthSum << std::endl;
    // std::cout << "Calls to searchLevel(): " << stats.searchLevelCalls << std::endl;
    // std::cout << "Bloom true positives: " << stats.bloomTruePositives << std::endl;
    // std::cout << "Bloom false positives: " << stats.bloomFalsePositives << std::endl;
    std::cout << "Bloom FPR: " << (float)stats.bloomFalsePositives / (float)(stats.bloomFalsePositives + (stats.searchLevelCalls - stats.bloomTruePositives)) << std::endl;
    std::cout << "Deletes: " << stats.deletes << std::endl;
    // std::cout << "\n —————————————————————————— \n" << std::endl;
}

int main() {
    std::cout << "\nStarting up server...\n" << std::endl;
    std::cout << "Buffer size: " << lsm.getBufferSize() << std::endl;
    std::cout << "Size ratio: " << SIZE_RATIO << std::endl;
    std::cout << "Bloom target FPR: " << BLOOM_TARGET_FPR << "\n" << std::endl;
    std::cout << std::fixed << std::setprecision(0) << std::endl;

    populateCatalog();

    std::ifstream file("../dsl/put-get-range/100k.dsl");
    std::istream* input = &file;
    if (!file) {
        std::cerr << "Error opening file, falling back to std::cin" << std::endl;
        input = &std::cin;
    }

    std::string userCommand;
    auto start = std::chrono::high_resolution_clock::now();
    while (std::getline(*input, userCommand)) {
        // std::cout << "executing: " << userCommand << std::endl;

        std::string replyMessage;
        Status status;
        std::tie(status, replyMessage) = processCommand(userCommand);
        std::cout << replyMessage << std::endl;

        if (userCommand == "s" || userCommand == "sw") break;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "total runtime: " << runtime.count() << " ms" << std::endl;

    shutdownServer(userCommand);
    printStats();
    return 0;
}
