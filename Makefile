# Variables
CXX = clang++

CXXFLAGS = -std=c++17  -I/usr/local/include/rocksdb -Iinclude -Ibloom -I/usr/include/spdlog 
LDFLAGS = -L/usr/local/lib  -lz -lbz2 -lsnappy -llz4 -lzstd -pthread -ldl -fvisibility=hidden -fvisibility-inlines-hidden -lrocksdb -lfmt 

TARGET = HierarchicalDB
SRC = \
    src/db_manager.cpp \
    src/bloom_manager.cpp \
    src/compaction_event_listener.cpp \
    src/main.cpp \
    src/TestRunner.cpp \
    bloom/bloomTree.cpp \
    bloom/bloom_value.cpp \
    bloom/node.cpp \
    bloom/MurmurHash3.cpp

# Build target
$(TARGET): $(SRC)
	$(CXX) -fno-rtti -o $(TARGET) $(SRC) $(CXXFLAGS) $(LDFLAGS)

# Clean target
clean:
	rm -f $(TARGET) \
    rm -rf db \

