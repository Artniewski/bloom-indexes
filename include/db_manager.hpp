#ifndef DB_MANAGER_HPP
#define DB_MANAGER_HPP

#include <rocksdb/db.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/sst_file_reader.h>

#include <memory>
#include <string>
#include <vector>

#include "bloomTree.hpp"

class DBManager {
   public:
    void compactAllColumnFamilies();
    void openDB(const std::string &dbname, bool withListener = false, std::vector<std::string> columns = {"phone", "mail", "address", "name", "surname"});
    void insertRecords(int numRecords, std::vector<std::string> columns);
    void insertRecordsWithSearchTargets(int numRecords, const std::vector<std::string> &columns, int targetCount, std::string searchPattern);
    std::vector<std::string> scanSSTFilesForColumn(const std::string &dbname, const std::string &column);
    bool isOpen() const { return static_cast<bool>(db_); }
    void closeDB();
    // key - value
    bool checkValueWithoutBloomFilters(const std::string &value);
    bool ScanFileForValue(const std::string &filename, const std::string &value);
    // single column check
    bool noBloomcheckValueInColumn(const std::string &column, const std::string &value);
    bool findRecordInHierarchy(BloomTree &hierarchy, const std::string &value,
                               const std::string &startKey = "", const std::string &endKey = "");
    // multiple columns without Bloom filters
    std::vector<std::string> scanForRecordsInColumns(const std::vector<std::string> &columns, const std::vector<std::string> &values);
    // scan given SST file for keys with a specific value
    std::vector<std::string> scanFileForKeysWithValue(const std::string &filename, const std::string &value,
                                                      const std::string &rangeStart, const std::string &rangeEnd);
    // query hierarchy for one column and then get from DB
    std::vector<std::string> findUsingSingleHierarchy(BloomTree &hierarchy,
                                                      const std::vector<std::string> &columns,
                                                      const std::vector<std::string> &values);

   private:
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
