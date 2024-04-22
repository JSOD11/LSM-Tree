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

std::vector<std::string> parseCommand(std::string userCommand);
bool isNum(const std::string& str);
std::string mapToString(const std::map<KEY_TYPE, VAL_TYPE>& map);

#endif
