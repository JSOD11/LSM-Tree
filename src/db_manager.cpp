#include <string>
#include <sstream>
#include <iostream>

#include "lsm.hpp"

std::unordered_map<int, int> map;

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

std::tuple<Status, std::string> put(Status status, int key, int val) {
    map[key] = val;
    return std::make_tuple(status, "");
}

std::tuple<Status, std::string> get(Status status, int key) {
    if (map.find(key) == map.end()) {
        std::cout << key << " is not a member of the LSM tree." << std::endl;
        return std::make_tuple(status, "");
    } else {
        int val = map[key];
        std::cout << key << " maps to " << val << std::endl;
        return std::make_tuple(status, std::to_string(val));
    }
}

std::tuple<Status, std::string> processCommand(std::string userCommand) {

    Status status = SUCCESS;

    if (userCommand == "shutdown") return std::make_tuple(status, "Server processed shutdown command.");

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
