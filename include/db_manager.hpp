#ifndef DB_MANAGER_HPP
#define DB_MANAGER_HPP

#include <rocksdb/db.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/sst_file_reader.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "bloomTree.hpp"

class DBManager {
 public:
  void compactAllColumnFamilies(size_t numRecords = 0);
  void openDB(const std::string &dbname,
              std::vector<std::string> columns = {"phone", "mail", "address"});
  void insertRecords(int numRecords, std::vector<std::string> columns);
  void insertRecordsWithSearchTargets(
      int numRecords, const std::vector<std::string> &columns,
      const std::unordered_set<int> &targetIndices);
  std::vector<std::string> scanSSTFilesForColumn(const std::string &dbname,
                                                 const std::string &column);
  bool isOpen() const { return static_cast<bool>(db_); }
  rocksdb::Status closeDB();

  std::string getValue(const std::string &column_family_name,
                       const std::string &key);
  rocksdb::ColumnFamilyHandle *getColumnFamilyHandle(
      const std::string &column_family_name);

  rocksdb::Status applyModifications(
      const std::vector<std::tuple<std::string, std::string, std::string>>
          &modifications,
      size_t numRecords);
  rocksdb::Status revertModifications(
      const std::vector<std::tuple<std::string, std::string, std::string>>
          &reversions,
      size_t numRecords);

  // key - value
  bool checkValueWithoutBloomFilters(const std::string &value);
  bool ScanFileForValue(const std::string &filename, const std::string &value);
  // single column check
  bool noBloomcheckValueInColumn(const std::string &column,
                                 const std::string &value);
  bool findRecordInHierarchy(BloomTree &hierarchy, const std::string &value,
                             const std::string &startKey = "",
                             const std::string &endKey = "");
  // multiple columns without Bloom filters
  std::vector<std::string> scanForRecordsInColumns(
      const std::vector<std::string> &columns,
      const std::vector<std::string> &values);
  // scan given SST file for keys with a specific value
  std::vector<std::string> scanFileForKeysWithValue(
      const std::string &filename, const std::string &value,
      const std::string &rangeStart, const std::string &rangeEnd);
  // query hierarchy for one column and then get from DB
  std::vector<std::string> findUsingSingleHierarchy(
      BloomTree &hierarchy, const std::vector<std::string> &columns,
      const std::vector<std::string> &values);

 private:
  struct RocksDBDeleter {
    void operator()(rocksdb::DB *dbPtr) const {
      delete dbPtr;  // Safe to call delete on a nullptr
    }
  };

  std::unique_ptr<rocksdb::DB, RocksDBDeleter> db_{nullptr};
  std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>>
      cf_handles_;
};

#endif  // DB_MANAGER_HPP
