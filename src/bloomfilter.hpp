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
        BloomFilter(size_t numBits, size_t numHashFunctions)
            : bits(numBits), numHashes(numHashFunctions) {}
        
        void add(KEY_TYPE key) {
            uint32_t hash[1];
            for (size_t i = 0; i < this->numHashes; ++i) {
                // i is used as the seed.
                MurmurHash3_x86_32(&key, sizeof(key), i, hash);
            this->bits[hash[0] % this->bits.size()] = true;
            }
        }

        bool mayContain(KEY_TYPE key) {
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
        void clear() {
            std::fill(this->bits.begin(), this->bits.end(), false);
        }

        size_t numBits() {
            return this->bits.size();
        }

        size_t getBit(size_t index) {
            return this->bits[index];
        }
};

#endif
