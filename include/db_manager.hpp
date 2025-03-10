#ifndef DB_MANAGER_HPP
#define DB_MANAGER_HPP

#include <string>
#include <vector>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/sst_file_reader.h>
#include "bloomTree.hpp"


/**
 * @brief A class to manage RocksDB operations such as creating, inserting, and scanning for SST files.
 */
class DBManager {
public:

    /**
     * @brief Opens a RocksDB database at the given path. 
     *        Optionally attaches a compaction listener.
     *
     * @param dbname Path to the RocksDB database folder.
     * @param withListener If true, attach a compaction event listener.
     * @throw std::runtime_error if opening fails.
     */
    void openDB(const std::string& dbname, bool withListener = false);

    /**
     * @brief Inserts `numRecords` into the database. Example: key = "key{i}", value = "value{i}".
     */
    void insertRecords(int numRecords);

    /**
     * @brief Returns a list of all .sst files in the database directory.
     */
    std::vector<std::string> scanSSTFiles(const std::string& dbname);

    /**
     * @brief Checks whether the database is currently open.
     */
    bool isOpen() const { return static_cast<bool>(db_); }

    /**
     * @brief Closes the currently open database (if any). 
     *        This sets the unique_ptr to nullptr.
     */
    void closeDB();

    bool checkValueInHierarchy(bloomTree& hierarchy, const std::string& value);
    bool checkValueInHierarchyWithoutConcurrency(bloomTree& hierarchy, const std::string& value);

    bool checkValueWithoutBloomFilters(const std::string& value);
    bool ScanFileForValue(const std::string& filename, const std::string& value);



private:
    // A custom deleter for RocksDB to ensure we call 'delete db_' properly.
    struct RocksDBDeleter {
        void operator()(rocksdb::DB* dbPtr) const {
            delete dbPtr;  // Safe to call delete on a nullptr
        }
    };

    // Use unique_ptr with a custom deleter (though the default deleter also works fine)
    std::unique_ptr<rocksdb::DB, RocksDBDeleter> db_{nullptr};
};

#endif // DB_MANAGER_HPP
