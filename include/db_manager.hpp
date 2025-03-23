#ifndef DB_MANAGER_HPP
#define DB_MANAGER_HPP

#include <rocksdb/db.h>
#include <rocksdb/sst_file_reader.h>

#include <memory>
#include <string>
#include <vector>

#include "bloomTree.hpp"

class DBManager {
   public:
    void openDB(const std::string &dbname, bool withListener = false);
    void insertRecords(int numRecords, std::vector<std::string> columns);
    std::vector<std::string> scanSSTFilesForColumn(const std::string &dbname, const std::string &column);
    bool isOpen() const { return static_cast<bool>(db_); }
    void closeDB();
    bool checkValueWithoutBloomFilters(const std::string &value);
    bool ScanFileForValue(const std::string &filename, const std::string &value);
    bool findRecordInHierarchy(BloomTree &hierarchy, const std::string &value,
                               const std::string &startKey = "", const std::string &endKey = "");

    bool checkValueAcrossHierarchies(BloomTree &hierarchy1, const std::string &value1,
                                     BloomTree &hierarchy2, const std::string &value2);
    bool noBloomCheckRecordWithTwoColumns(const std::string &column1, const std::string &value1,
                                          const std::string &column2, const std::string &value2);
    bool noBloomcheckValueInColumn(const std::string &column, const std::string &value);
    bool findRecordInHierarchies(BloomTree &hierarchy1, const std::string &value1,
                                 BloomTree &hierarchy2, const std::string &value2);

   private:
    std::unordered_set<std::string> scanFileForKeysWithValue(const std::string &filename, const std::string &value,
                                                             const std::string &rangeStart, const std::string &rangeEnd);
    // A custom deleter for RocksDB to ensure we call 'delete db_' properly.
    struct RocksDBDeleter {
        void operator()(rocksdb::DB *dbPtr) const {
            delete dbPtr;  // Safe to call delete on a nullptr
        }
    };

    // Use unique_ptr with a custom deleter (though the default deleter also works fine)
    std::unique_ptr<rocksdb::DB, RocksDBDeleter> db_{nullptr};
    std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>> cf_handles_;
};

#endif  // DB_MANAGER_HPP
