#ifndef COMPACTION_EVENT_LISTENER_HPP
#define COMPACTION_EVENT_LISTENER_HPP

#include <rocksdb/listener.h>
#include <rocksdb/db.h>
#include <iostream>

/**
 * @brief A custom RocksDB event listener to handle compaction events.
 */
class CompactionEventListener : public rocksdb::EventListener {
public:
    void OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& info) override;
};

#endif // COMPACTION_EVENT_LISTENER_HPP
