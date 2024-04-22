#ifndef TYPES_H
#define TYPES_H

#include <cstddef>
#include <string>
#include <cstdint>
#include <unistd.h>
#include <map>

using KEY_TYPE = int32_t;
using VAL_TYPE = int64_t;

const int PORT = 6789;

// We set PAGE_SIZE to this since int64_t is the largest type supported.
const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE) / sizeof(int64_t);
const size_t BUFFER_PAGES = 1;
const size_t SIZE_RATIO = 10;
const float BLOOM_TARGET_FPR = 0.01;

// Uncomment the below to create small trees for debugging.
// const size_t PAGE_SIZE = 3;
// const size_t BUFFER_PAGES = 1;
// const size_t SIZE_RATIO = 2;
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
    size_t deletes = 0;
};

extern std::map<KEY_TYPE, VAL_TYPE> map;
extern Stats stats;

#endif
