#ifndef LSM_H
#define LSM_H

#include <cstddef>
#include <string>
#include <map>

#include "BloomFilter.hpp"

const int PORT = 6789;

// PAGE_SIZE is the number of entries that fit in a page.
const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE) / (2 * sizeof(int));
const size_t BUFFER_SIZE = PAGE_SIZE;
const size_t SIZE_RATIO = 10;
const size_t BLOOM_BITS_PER_ENTRY = 10;
const float BLOOM_TARGET_FPR = 0.01;

// Uncomment the below to create small trees for debugging.
// const size_t PAGE_SIZE = 3;
// const size_t BUFFER_SIZE = PAGE_SIZE;
// const size_t SIZE_RATIO = 3;
// const size_t BLOOM_BITS_PER_ENTRY = 5;
// const float BLOOM_TARGET_FPR = 0.01;

enum Status {
    SUCCESS = 0,
    ERROR = 1
};

struct Message {
    Status status;
    size_t messageLength;
    char message[512];
};

struct Stats {
    size_t puts = 0;
    size_t successfulGets = 0;
    size_t failedGets = 0;
    size_t ranges = 0;
    double rangeLengthSum = 0;
    size_t searchLevelCalls = 0;
    size_t bloomTruePositives = 0;
    size_t bloomFalsePositives = 0;
};

extern std::map<int, int> map;
extern Stats stats;

#endif
