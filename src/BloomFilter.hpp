#ifndef BLOOM_H
#define BLOOM_H

#include <vector>

#include "MurmurHash3.hpp"
#include "Types.hpp"

class BloomFilter {
    private:
        std::vector<bool> bits;
        size_t numHashes;

    public:
        BloomFilter(size_t numBits, size_t numHashFunctions);
        void add(KEY_TYPE key);
        bool mayContain(KEY_TYPE key);
        void clear();
        size_t numBits();
        size_t getBit(size_t index);
};

#endif
