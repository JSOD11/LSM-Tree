#ifndef LSM_H
#define LSM_H

#include <cstddef>
#include <string>
#include <map>

#include "BloomFilter.hpp"

const int PORT = 6789;

const size_t BUFFER_SIZE = sysconf(_SC_PAGESIZE) / (2 * sizeof(int));
// const size_t BUFFER_SIZE = 5;
const size_t SIZE_RATIO = 8;
const size_t BLOOM_BITS_PER_ENTRY = 10;
const float BLOOM_TARGET_FPR = 0.01;

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
    // The buffer size is the number of KV pairs in l0 (usually a multiple of the page size).
    size_t bufferSize;
    size_t numLevels;
    size_t sizeRatio;
    int* levels[10];
    size_t pairsInLevel[10];
    int* fence[10];
    size_t fenceLength[10];
    BloomFilter* bloomfilters[10];
};

struct Stats {
    size_t puts;
    size_t successfulGets;
    size_t failedGets;
    size_t ranges;
    double rangeLengthSum;
    size_t searchLevelCalls;
    size_t bloomTruePositives;
    size_t bloomFalsePositives;
};

extern std::map<int, int> map;
extern Catalog catalog;
extern Stats stats;

#endif
