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
#include "LSM.hpp"

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
        std::cout << "Started new database from scratch.\n" << std::endl;
    } else {
        // We are populating the catalog with persisted data.
        std::ifstream catalogFile("data/catalog.data");
        size_t numPairs = 0, l = 0;
        while (catalogFile >> numPairs) {
            KEY_TYPE* keysPointer = mmapLevel<KEY_TYPE>(("data/k" + std::to_string(l) + ".data").c_str(), l);
            VAL_TYPE* valsPointer = mmapLevel<VAL_TYPE>(("data/v" + std::to_string(l) + ".data").c_str(), l);
            bool* tombstonePointer = mmapLevel<bool>(("data/t" + std::to_string(l) + ".data").c_str(), l);
            lsm.initializeLevel(l, keysPointer, valsPointer, tombstonePointer, numPairs);
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
        std::cout << "Persisted data folder." << std::endl;
    }

    for (size_t l = 0; l < lsm.getNumLevels(); l++) {
        munmap(lsm.getLevel(l)->keys, lsm.getPairsInLevel(l) * sizeof(KEY_TYPE));
        munmap(lsm.getLevel(l)->vals, lsm.getPairsInLevel(l) * sizeof(VAL_TYPE));
        delete lsm.getLevel(l);
    }
}

void printStats(void) {
    std::cout << "\n ——— Session statistics ——— \n" << std::endl;
    std::cout << "Puts: " << stats.puts << std::endl;
    std::cout << "Successful gets: " << stats.successfulGets << std::endl;
    std::cout << "Failed gets: " << stats.failedGets << std::endl;
    std::cout << "Ranges: " << stats.ranges << std::endl;
    std::cout << "Sum length of all ranges: " << stats.rangeLengthSum << std::endl;
    // std::cout << "Calls to searchLevel(): " << stats.searchLevelCalls << std::endl;
    // std::cout << "Bloom true positives: " << stats.bloomTruePositives << std::endl;
    std::cout << "Bloom false positives: " << stats.bloomFalsePositives << std::endl;
    std::cout << "Bloom FPR: " << (float)stats.bloomFalsePositives / (float)(stats.bloomFalsePositives + (stats.searchLevelCalls - stats.bloomTruePositives)) << std::endl;
    std::cout << "Deletes: " << stats.deletes << std::endl;
    std::cout << "\n —————————————————————————— \n" << std::endl;
}

int main() {

    std::cout << "\nStarting up server...\n" << std::endl;
    std::cout << "Buffer size: " << lsm.getBufferSize() << std::endl;
    std::cout << "Size ratio: " << SIZE_RATIO << std::endl;
    std::cout << "Bloom target FPR: " << BLOOM_TARGET_FPR << "\n" << std::endl;
    std::cout << std::fixed << std::setprecision(0) << std::endl;

    populateCatalog();

    int listening = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in hint;
    hint.sin_family = AF_INET;
    hint.sin_port = htons(PORT);
    hint.sin_addr.s_addr = INADDR_ANY;
    bind(listening, (sockaddr*)&hint, sizeof(hint));
    listen(listening, SOMAXCONN);
    sockaddr_in client;
    socklen_t clientSize = sizeof(client);
    int clientSocket = accept(listening, (sockaddr*)&client, &clientSize);
    close(listening);

    std::cout << "Connected to client. Server is listening...\n" << std::endl;

    uint32_t length = 0;
    char buf[4096];
    std::string userCommand;
    while (true) {
        // Clear the buffer.
        memset(buf, 0, 4096);

        // The first message describes the length of the client command.
        int sizeBytesReceived = recv(clientSocket, &length, sizeof(length), 0);

        if (sizeBytesReceived == 0) {
            std::cout << "\nThe client disconnected.\n" << std::endl;
            break;
        }

        length = ntohl(length);

        // This message contains the client's actual command.
        int bytesReceived = recv(clientSocket, buf, length, 0);

        userCommand = std::string(buf, buf + bytesReceived);
        
        // std::cout << "Client command: " << userCommand << std::endl;

        // Process the message and send a reply to the client.
        std::string replyMessage;
        Status status;
        std::tie(status, replyMessage) = processCommand(userCommand);

        Message* message = new Message();
        message->status = status;
        strncpy(message->message, replyMessage.c_str(), sizeof(message->message) - 1);
        message->message[sizeof(message->message) - 1] = '\0';
        message->messageLength = strlen(message->message);

        send(clientSocket, message, sizeof(Message), 0);

        delete message;
    }

    shutdownServer(userCommand);
    printStats();
    close(clientSocket);
    return 0;
}
