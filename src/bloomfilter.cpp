#include <vector>
#include <functional>

#include "BloomFilter.hpp"

BloomFilter::BloomFilter(size_t numBits, size_t numHashFunctions)
    : bits(numBits), numHashes(numHashFunctions) {}

void BloomFilter::add(int key) {
    std::hash<int> hashFunc;
    for (size_t i = 0; i < numHashes; ++i) {
        size_t hash = hashFunc(key + i) % bits.size();
        bits[hash] = true;
    }
}

bool BloomFilter::mayContain(int key) {
    std::hash<int> hashFunc;
    for (size_t i = 0; i < numHashes; ++i) {
        size_t hash = hashFunc(key + i) % bits.size();
        if (!bits[hash]) {
            return false;
        }
    }
    return true;
}

// `BloomFilter::clear()`
// Resets the bloom filter bit vector to all false.
void BloomFilter::clear() {
    std::fill(this->bits.begin(), this->bits.end(), false);
}

size_t BloomFilter::numBits() {
    return this->bits.size();
}

size_t BloomFilter::getBit(size_t index) {
    return this->bits[index];
}
