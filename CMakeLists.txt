cmake_minimum_required(VERSION 3.10)
project(HierarchicalDB)

# Set C++ standard and optimization flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include paths
include_directories(
    /opt/homebrew/include       # for RocksDB, spdlog, etc.
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/bloom
)

link_directories(/opt/homebrew/lib)

find_package(spdlog REQUIRED)
find_package(RocksDB REQUIRED)
# Find RocksDB library
# find_library(ROCKSDB_LIB rocksdb REQUIRED HINTS /opt/homebrew/lib)

# Add required compression libraries for RocksDB
set(COMPRESSION_LIBS bz2 z snappy lz4 zstd)

set(SOURCES
    src/db_manager.cpp
    src/bloom_manager.cpp
    src/compaction_event_listener.cpp
    src/main.cpp
    src/TestRunner.cpp
    src/partition_manager.cpp
    bloom/bloomTree.cpp
    bloom/bloom_value.cpp
    bloom/node.cpp
)

# Add executable
add_executable(HierarchicalDB ${SOURCES})

# Link libraries
target_link_libraries(HierarchicalDB
    PRIVATE
        # Link spdlog via its target
        spdlog::spdlog

        # Link the RocksDB library we found
        RocksDB::rocksdb

        # Additional compression libs
        ${COMPRESSION_LIBS}
        
        pthread
)