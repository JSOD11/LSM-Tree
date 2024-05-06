#include <vector>
#include <functional>

#include "bloomfilter.hpp"
#include "Types.hpp"

BloomFilter::BloomFilter(size_t numBits, size_t numHashFunctions)
    : bits(numBits), numHashes(numHashFunctions) {}

void BloomFilter::add(KEY_TYPE key) {
    uint32_t hash[1];
    for (size_t i = 0; i < this->numHashes; ++i) {
        // i is used as the seed.
        MurmurHash3_x86_32(&key, sizeof(key), i, hash);
       this->bits[hash[0] % this->bits.size()] = true;
    }
}

bool BloomFilter::mayContain(KEY_TYPE key) {
    uint32_t hash[1];
    for (size_t i = 0; i < this->numHashes; ++i) {
        // i is used as the seed.
        MurmurHash3_x86_32(&key, sizeof(key), i, hash);
        if (!this->bits[hash[0] % this->bits.size()]) {
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
