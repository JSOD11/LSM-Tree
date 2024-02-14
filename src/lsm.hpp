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
    // The buffer size is the number of KV pairs in l0 (usually set to fill one page).
    size_t bufferSize;
    size_t numLevels;
    size_t sizeRatio;
    // TODO: Change these to be std::vectors. Value *inside* the vector is int* or size_t.
    int* levels[10];
    size_t pairsInLevel[10];
    // fence[l][i] stores the minimum value in levels[i * bufferSize] at level l.
    int* fence[10];
    size_t fenceLength[10];
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
