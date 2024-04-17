#ifndef BLOOM_H
#define BLOOM_H

#include <vector>

#include "MurmurHash3.hpp"

class BloomFilter {
private:
    std::vector<bool> bits;
    size_t numHashes;

public:
    BloomFilter(size_t numBits, size_t numHashFunctions);
    void add(int key);
    bool mayContain(int key);
    void clear();
    size_t numBits();
    size_t getBit(size_t index);
};

#endif
