#include <string>
#include <sstream>
#include <iostream>

#include "lsm.hpp"

// std::map<int, int> map;
Catalog catalog = {
    .bufferSize = 10,
    .numLevels = 0,
    .sizeRatio = 3,
    .levels = {nullptr},
    .pairsInLevel = {0},
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

// `put()`
// Put a key and value into the LSM tree.
std::tuple<Status, std::string> put(Status status, int key, int val) {
    catalog.levels[0][2 * catalog.pairsInLevel[0]] = key;
    catalog.levels[0][2 * catalog.pairsInLevel[0] + 1] = val;
    catalog.pairsInLevel[0]++;
    return std::make_tuple(status, "");
}

// `get()`
// Search the LSM tree for a key.
std::tuple<Status, std::string> get(Status status, int key) {

    for (size_t i = 0; i < catalog.bufferSize; i += 2) {
        if (catalog.levels[0][i] == key) {
            std::cout << key << " maps to " << catalog.levels[0][i + 1] << std::endl;
            return std::make_tuple(status, std::to_string(catalog.levels[0][i + 1]));
        }
    }

    std::cout << key << " is not a member of the LSM tree." << std::endl;
    return std::make_tuple(status, "");
}

void printLevels(void) {
    for (size_t i = 0; i < catalog.numLevels; i++) {
        int* level = catalog.levels[i];
        std::cout << "Level " << i << std::endl;
        for (size_t j = 0; j < catalog.pairsInLevel[i]; j++) {
            std::cout << level[2*j] << " -> " << level[2*j+1] << std::endl;
        }
    }
}

std::tuple<Status, std::string> processCommand(std::string userCommand) {

    Status status = SUCCESS;

    if (userCommand == "shutdown") return std::make_tuple(status, "Server processed shutdown command.");

    if (userCommand == "printLevels") {
        printLevels();
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
