#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>
#include <jni/JNIByteCreatable.h>

namespace DB
{

class JNIMetaClient : public JNIByteCreatable
{
public:
    JNIMetaClient(const std::string & class_factory_full_classname, const std::string & full_classname, const std::string & pb_message);
    ~JNIMetaClient() override = default;

    std::string getTable();
    std::string getPartitionPaths();
    std::string getFilesInPartition(const std::string & partition_path);


    std::string getPaimonSchema(const std::map<std::string, std::string> & params);
    // PaimonScanInfo represents a tuple of (encoded_table, encoded_predicate, encoded_splits, raw_files)
    using PaimonScanInfo = std::tuple<std::string, std::optional<std::string>, std::vector<std::string>, std::vector<std::string>>;
    PaimonScanInfo getPaimonScanInfo(const std::map<std::string, std::string> & params);

    std::string getTable(const std::string & database, const std::string & table);
    std::string getDatabase(const std::string & database);
    std::string
    getPartitionsByFilter(const std::string & database, const std::string & table, const std::string & filter, int16_t max_parts);
    std::vector<std::string> listDatabases();
    std::vector<std::string> listTables(const std::string & database);
    bool isTableExist(const std::string & database, const std::string & table);
};
}
