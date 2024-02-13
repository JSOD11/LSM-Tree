#ifndef LSM_H
#define LSM_H

#include <cstddef>
#include <string>

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

extern std::unordered_map<int, int> map;

#endif
