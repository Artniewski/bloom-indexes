// #include <iostream>
// #include <string>
// #include "rocksdb/db.h"

// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/status.h>
// #include "rocksdb/sst_file_reader.h"

// #include "rocksdb/db.h"
// #include "rocksdb/db.h"
// #include <iostream>

// #include "rocksdb/db.h"
// #include <iostream>

// using namespace ROCKSDB_NAMESPACE;

// int main() {
//     DB* db;
//     std::string db_path = "testdb";

//     // Correct type to hold column family names
//     std::vector<std::string> cf_names;

//     // Get the list of existing column family names
//     Status status = DB::ListColumnFamilies(DBOptions(), db_path, &cf_names);
//     if (!status.ok()) {
//         std::cerr << "Error listing column families: " << status.ToString() << "\n";
//         return 1;
//     }

//     // Prepare ColumnFamilyDescriptors to open DB
//     std::vector<ColumnFamilyDescriptor> cf_descs;
//     for (const auto& name : cf_names) {
//         cf_descs.emplace_back(name, ColumnFamilyOptions());
//     }

//     // Handles for each Column Family
//     std::vector<ColumnFamilyHandle*> cf_handles;

//     // Open DB with column families
//     status = DB::Open(DBOptions(), db_path, cf_descs, &cf_handles, &db);
//     if (!status.ok()) {
//         std::cerr << "Error opening DB: " << status.ToString() << "\n";
//         return 1;
//     }

//     // Print out Column Family Metadata including SST files
//     for (size_t i = 0; i < cf_handles.size(); ++i) {
//         ColumnFamilyMetaData cf_meta;
//         db->GetColumnFamilyMetaData(cf_handles[i], &cf_meta);

//         std::cout << "Column Family: " << cf_meta.name << "\n";
//         for (const auto& level : cf_meta.levels) {
//             std::cout << "--- Level " << level.level << " ---\n";
//             for (const auto& file : level.files) {
//                 std::cout << "SST File: " << file.name << " [" << file.size << " bytes]\n";
//             }
//         }
//     }

//     // Clean up
//     for (auto handle : cf_handles) {
//         delete handle;
//     }
//     delete db;

//     return 0;
// }
