#pragma once

#include <string>
#include <map>
#include <vector>
#include <future>

#include "test_params.hpp"

void runExp2(const std::string& dbPath, size_t dbSize);
void writeCSVheaders();
TestParams buildParams(const std::string& dbName, size_t items, size_t dbSize);
