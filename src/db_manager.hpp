#ifndef DB_MANAGER_H
#define DB_MANAGER_H

#include <string>
#include <tuple>

#include "lsm.hpp"

struct Level {
    int* data = nullptr;
    size_t numPairs = 0;
    int* fence = nullptr;
    size_t fenceLength = 0;
    BloomFilter* bloomFilter = nullptr;
};

class Catalog {
    private:
        // The buffer size is the number of KV pairs in l0 (usually a multiple of the page size).
        size_t pageSize = PAGE_SIZE;
        size_t bufferSize = BUFFER_SIZE;
        size_t numLevels = 0;
        size_t sizeRatio = SIZE_RATIO;
        std::vector<Level*> levels = {};
    
    public:
        size_t getBufferSize();
        size_t getNumLevels();
        size_t getSizeRatio();
        Level* getLevel(size_t l);
        int* getLevelData(size_t l);
        size_t getPairsInLevel(size_t l);
        bool levelIsEmpty(size_t level);
        int getFenceKey(size_t l, size_t index);
        size_t getFenceLength(size_t l);
        BloomFilter* getBloomFilter(size_t l);

        void initializeLevel(size_t l, int* levelPointer, size_t numPairs);
        void appendPair(size_t level, int key, int val);
        void insertPair(size_t level, size_t pairIndex, int key, int val);
        int getKey(size_t level, size_t entryIndex);
        int getVal(size_t level, size_t entryIndex);
        
        void constructFence(size_t l);
        void constructBloomFilter(size_t numBits);

        void clearLevel(size_t l);
};

extern Catalog catalog;

int* mmapLevel(const char* fileName, size_t level);
std::tuple<Status, std::string> processCommand(std::string);

#endif
