#include "compaction_event_listener.hpp"

void CompactionEventListener::OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& info) {
    std::cout << "Compaction completed:\n";

    std::cout << "Input files:\n";
    for (const auto& input_file : info.input_files) {
        std::cout << "  - " << input_file << std::endl;
    }

    std::cout << "Output files:\n";
    for (const auto& output_file : info.output_files) {
        std::cout << "  - " << output_file << std::endl;
    }

    std::cout << "Output level: " << info.output_level << std::endl;
    std::cout << "Elapsed time (ms): " << info.stats.elapsed_micros / 1000 << std::endl;
}
