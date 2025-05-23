# Variables
CXX = clang++

CXXFLAGS = -std=c++20  -I/usr/local/include/rocksdb -Iinclude -Ibloom -I/usr/include/spdlog
LDFLAGS = -L/usr/local/lib  -lz -lbz2 -lsnappy -llz4 -lzstd -pthread -ldl -fvisibility=hidden -fvisibility-inlines-hidden -lrocksdb -lfmt -lboost_system -lboost_thread

TARGET = HierarchicalDB
SRC = \
    src/db_manager.cpp \
    src/bloom_manager.cpp \
    src/main.cpp \
    src/exp1.cpp \
    src/exp2.cpp \
    src/exp3.cpp \
    src/exp4.cpp \
    src/exp5.cpp \
    src/exp6.cpp \
    src/exp7.cpp \
    src/exp8.cpp \
    src/exp_utils.cpp \
    bloom/bloomTree.cpp \
    bloom/bloom_value.cpp \
    bloom/node.cpp \
    bloom/MurmurHash3.cpp

# Convert source files to object files
OBJ_DIR = obj
OBJ = $(SRC:%.cpp=$(OBJ_DIR)/%.o)

# Create object directories
$(shell mkdir -p $(sort $(dir $(OBJ))))

# Build target
$(TARGET): $(OBJ)
	$(CXX) -fno-rtti -o $@ $^ $(LDFLAGS)

# Pattern rule for object files
$(OBJ_DIR)/%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean target
clean:
	rm -f $(TARGET)
	rm -rf $(OBJ_DIR)
	rm -rf db

.PHONY: clean

