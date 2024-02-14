#ifndef DB_MANAGER_H
#define DB_MANAGER_H

#include <string>
#include <tuple>

#include "lsm.hpp"

void constructFence(size_t l);
std::tuple<Status, std::string> processCommand(std::string);

#endif
