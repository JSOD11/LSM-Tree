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

std::string vectorToString(const std::vector<VAL_TYPE>& vec) {
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
