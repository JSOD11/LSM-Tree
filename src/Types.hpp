#ifndef TYPES_H
#define TYPES_H

#include <cstddef>
#include <string>
#include <cstdint>
#include <unistd.h>
#include <map>

// `KEY_TYPE` and `VAL_TYPE` are the key and value types inserted into
// the tree. The dictionary used in DICT encoding will map values of
// type `VAL_TYPE` to an encoded value of type `DICT_VAL_TYPE`.
using KEY_TYPE = int32_t;
using VAL_TYPE = int64_t;
using DICT_VAL_TYPE = uint8_t;

const int PORT = 6789;

// If TESTING_SWITCH == TESTING_ON, then range queries take a little longer because
// we calculate the sum of all values in all ranges. This is useful
// because we can compare this result to what we get from the python
// evaluate function and ensure correctness of outputs. If the sum length of
// all range queries % 10^6 is the same for both, and the number of successful gets, 
// failed gets, and length of all ranges are the same, then we can be confident that 
// the tree is working correctly.
enum TestingSwitch {
    TESTING_OFF,
    TESTING_ON,
};

const TestingSwitch TESTING_SWITCH = TESTING_ON;

enum EncodingType {
    ENCODING_OFF,
    ENCODING_DICT,
};

const EncodingType ENCODING_TYPE = ENCODING_OFF;

// We set PAGE_SIZE to this since int64_t is the largest type supported.
const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE) / sizeof(int64_t);
const size_t BUFFER_PAGES = 4;
const size_t SIZE_RATIO = 10;
const float BLOOM_TARGET_FPR = 0.01;

// Uncomment the below to create small trees for debugging.
// const size_t PAGE_SIZE = 3;
// const size_t BUFFER_PAGES = 1;
// const size_t SIZE_RATIO = 3;
// const float BLOOM_TARGET_FPR = 0.01;

enum Status {
    SUCCESS,
    ERROR,
};

#endif
