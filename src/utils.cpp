#include "Utils.hpp"
#include "Types.hpp"

// `parseCommand()`
// Parses a command such as `p 1 3` or `g 7` into tokens.
std::vector<std::string> parseCommand(std::string userCommand) {
    std::vector<std::string> tokens;
    std::istringstream stringStream(userCommand);
    std::string token;
    while (stringStream >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

bool isNum(const std::string& str) {
    std::istringstream iss(str);
    int64_t num;
    iss >> num;
    return iss.eof() && !iss.fail(); 
}

std::string mapToString(const std::map<KEY_TYPE, VAL_TYPE>& map) {
    std::stringstream ss;
    ss << "[";
    if (!map.empty()) {
        for (auto it = map.begin(); it != map.end(); ++it) {
            if (it != map.begin()) {
                ss << ", ";
            }
            ss << it->first << ":" << it->second;
        }
    }
    ss << "]";
    return ss.str();
}
