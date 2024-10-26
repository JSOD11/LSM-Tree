#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <tuple>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <cmath>
#include <iomanip>

#include "Types.hpp"
#include "Utils.hpp"
#include "lsm.hpp"

// `main()`
// Run `./server` to start up the LSM tree. See `Types.hpp` to change the encoding type, testing switch,
// buffer pages, size ratio, and other knobs.
int main() {
    std::cout << "\nStarting up server...\n" << std::endl;

    LSM<KEY_TYPE, VAL_TYPE, DICT_VAL_TYPE> lsm;

    if (ENCODING_TYPE == ENCODING_OFF) std::cout << "Encoding type: ENCODING_OFF" << std::endl;
    else if (ENCODING_TYPE == ENCODING_DICT) std::cout << "Encoding type: ENCODING_DICT" << std::endl;
    if (TESTING_SWITCH == TESTING_OFF) std::cout << "Testing: TESTING_OFF" << std::endl;
    else if (TESTING_SWITCH == TESTING_ON) std::cout << "Encoding type: TESTING_ON" << std::endl;
    std::cout << "Buffer size: " << lsm.getBufferSize() << std::endl;
    std::cout << "Size ratio: " << SIZE_RATIO << std::endl;
    std::cout << "Bloom target FPR: " << BLOOM_TARGET_FPR << "\n" << std::endl;
    std::cout << std::fixed << std::setprecision(0) << std::endl;

    std::string userCommand;
    auto start = std::chrono::high_resolution_clock::now();
    while (std::getline(std::cin, userCommand)) {
        std::string replyMessage;
        Status status;
        std::tie(status, replyMessage) = lsm.processCommand(userCommand);
        std::cout << replyMessage << std::endl;
        if (userCommand == "s" || userCommand == "sw") break;
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto runtime = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "total runtime: " << runtime.count() << " ms" << std::endl;

    if (ENCODING_TYPE == ENCODING_OFF) std::cout << "Encoding type: ENCODING_OFF" << std::endl;
    else if (ENCODING_TYPE == ENCODING_DICT) std::cout << "Encoding type: ENCODING_DICT" << std::endl;
    if (TESTING_SWITCH == TESTING_OFF) std::cout << "Testing: TESTING_OFF" << std::endl;
    else if (TESTING_SWITCH == TESTING_ON) std::cout << "Encoding type: TESTING_ON" << std::endl;
    lsm.printStats();
    lsm.shutdownServer(userCommand);
    return 0;
}
