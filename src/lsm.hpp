#ifndef LSM_H
#define LSM_H

#include <cstddef>
#include <string>
#include <map>

const int PORT = 6789;

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
    // The buffer size is the number of KV pairs in l0.
    size_t bufferSize;
    size_t numLevels;
    size_t sizeRatio;
    int* levels[10];
    size_t pairsInLevel[10];
};

struct Stats {
    size_t puts;
    size_t successfulGets;
    size_t failedGets;
};

// extern std::map<int, int> map;
extern Catalog catalog;
extern Stats stats;

#endif
