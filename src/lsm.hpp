#ifndef LSM_H
#define LSM_H

#include <cstddef>
#include <string>
#include <map>

const int PORT = 6789;

const size_t BUFFER_SIZE = sysconf(_SC_PAGESIZE) / (2 * sizeof(int));
// const size_t BUFFER_SIZE = 3;
const size_t SIZE_RATIO = 8;
const size_t bloomBitsPerEntry = 10;

enum Status {
    SUCCESS = 0,
    ERROR = 1
};

struct Message {
    Status status;
    size_t messageLength;
    char message[512];
};

struct Catalog {
    // The buffer size is the number of KV pairs in l0 (usually set to fill one page).
    size_t bufferSize;
    size_t numLevels;
    size_t sizeRatio;
    int* levels[10];
    size_t pairsInLevel[10];
    int* fence[10];
    size_t fenceLength[10];
    // bloom[l] stores the bloom filter bit array for level l. The size of bloom[l] is
    // bufferSize * sizeRatio^l * bloomBitsPerEntry bits.
};

struct Stats {
    size_t puts;
    size_t successfulGets;
    size_t failedGets;
};

extern std::map<int, int> map;
extern Catalog catalog;
extern Stats stats;

#endif
