#ifndef UTILS_HPP
#define UTILS_HPP

#include <cstddef>
#include <vector>
#include <sstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cmath>

#include "Types.hpp"

// `mmapLevel()`
// Takes in the name of a file for a level and the level number. Returns an int* pointing to the
// start of the level array.
template<typename T>
T* mmapLevel(const char* fileName, size_t l) {
    int fd = open(fileName, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    size_t fileSize = PAGE_SIZE * BUFFER_PAGES * std::pow(SIZE_RATIO, l) * sizeof(T);
    ftruncate(fd, fileSize);
    return reinterpret_cast<T*>(mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
}

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

#endif
