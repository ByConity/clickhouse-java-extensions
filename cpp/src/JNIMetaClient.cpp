#include "jni/JNIMetaClient.h"

#include "jni/JNIByteCreatable.h"
#include "jni/JNIHelper.h"

#include <cassert>

namespace DB
{
JNIMetaClient::JNIMetaClient(
    const std::string & class_factory_full_classname,
    const std::string & full_classname,
    const std::string & pb_message,
    const std::map<std::string, std::string> & options)
    : JNIByteCreatable(class_factory_full_classname, full_classname, pb_message, options)
{
    registerMethod("getTable", "()[B");
    registerMethod("getPartitionPaths", "()[B");
    registerMethod("getFilesInPartition", "([B)[B");
    registerMethod("getPaimonSchema", "(Ljava/util/Map;)[B");
    registerMethod("getPaimonScanInfo", "(Ljava/util/Map;)Ljava/util/Map;");
    registerMethod("getIcebergSchema", "(Ljava/util/Map;)[B");
    registerMethod("getIcebergScanInfo", "(Ljava/util/Map;)Ljava/util/Map;");
    registerMethod("getTableByName", "(Ljava/lang/String;Ljava/lang/String;)[B");
    registerMethod("isTableExists", "(Ljava/lang/String;Ljava/lang/String;)Z");
    registerMethod("getDatabase", "(Ljava/lang/String;)[B");
    registerMethod("getPartitionsByFilter", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;S)[B");
    registerMethod("listDatabases", "()[Ljava/lang/String;");
    registerMethod("listTables", "(Ljava/lang/String;)[Ljava/lang/String;");
}

std::string JNIMetaClient::getTable()
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getTable");
    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);
    return jni::jbyteArrayToStr(env, jbytes);
}

std::string JNIMetaClient::getPartitionPaths()
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getPartitionPaths");
    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);
    return jni::jbyteArrayToStr(env, jbytes);
}

std::string JNIMetaClient::getFilesInPartition(const std::string & partition_path)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getFilesInPartition");
    jni::AutoLocalJobject jstr_partition_path = jni::newJByteArray(env, partition_path.c_str(), partition_path.size());

    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jstr_partition_path.get()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);
    return jni::jbyteArrayToStr(env, jbytes);
}

std::string JNIMetaClient::getPaimonSchema(const std::map<std::string, std::string> & params)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getPaimonSchema");
    jni::AutoLocalJobject jmap = jni::mapToJmap(env, params);

    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jmap.get()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);
    return jni::jbyteArrayToStr(env, jbytes);
}

JNIMetaClient::PaimonScanInfo JNIMetaClient::getPaimonScanInfo(const std::map<std::string, std::string> & params)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getPaimonScanInfo");
    jni::AutoLocalJobject jmap_params = jni::mapToJmap(env, params);

    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jmap_params.get()));
    jni::AutoLocalJobject jmap_res = return_val.l;

    jni::AutoLocalJobject jbytes_encoded_table = jni::getFromJmap(env, jmap_res, "encoded_table");
    jni::AutoLocalJobject jbytes_encoded_predicate = jni::getFromJmap(env, jmap_res, "encoded_predicate");
    jni::AutoLocalJobject jbytes_encoded_splits = jni::getFromJmap(env, jmap_res, "encoded_splits");
    jni::AutoLocalJobject jbytes_raw_files = jni::getFromJmap(env, jmap_res, "raw_files");

    if (jbytes_encoded_predicate)
        return {
            jni::jbyteArrayToStr(env, static_cast<jbyteArray>(jbytes_encoded_table)),
            jni::jbyteArrayToStr(env, static_cast<jbyteArray>(jbytes_encoded_predicate)),
            jni::jlistByteArrayToVector(env, jbytes_encoded_splits),
            jni::jlistByteArrayToVector(env, jbytes_raw_files)};
    else
        return {
            jni::jbyteArrayToStr(env, static_cast<jbyteArray>(jbytes_encoded_table)),
            std::nullopt,
            jni::jlistByteArrayToVector(env, jbytes_encoded_splits),
            jni::jlistByteArrayToVector(env, jbytes_raw_files)};
}

std::string JNIMetaClient::getIcebergSchema(const std::map<std::string, std::string> & params)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getIcebergSchema");
    jni::AutoLocalJobject jmap = jni::mapToJmap(env, params);

    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jmap.get()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);
    return jni::jbyteArrayToStr(env, jbytes);
}

JNIMetaClient::IcebergScanInfo JNIMetaClient::getIcebergScanInfo(const std::map<std::string, std::string> & params)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getIcebergScanInfo");
    jni::AutoLocalJobject jmap_params = jni::mapToJmap(env, params);

    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jmap_params.get()));
    jni::AutoLocalJobject jmap_res = return_val.l;

    jni::AutoLocalJobject jbytes_data_files = jni::getFromJmap(env, jmap_res, "data_files");

    return {jni::jlistByteArrayToVector(env, jbytes_data_files)};
}

std::string JNIMetaClient::getTable(const std::string & database, const std::string & table)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getTableByName");
    jvalue return_val;
    jni::AutoLocalJobject jstr_database = jni::newJString(env, database);
    jni::AutoLocalJobject jstr_table = jni::newJString(env, table);

    THROW_JNI_EXCEPTION(
        env,
        jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jstr_database.get(), jstr_table.get()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);

    return jni::jbyteArrayToStr(env, jbytes);
}

std::string JNIMetaClient::getDatabase(const std::string & database)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getDatabase");
    jvalue return_val;
    jni::AutoLocalJobject jstr_database = jni::newJString(env, database);

    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jstr_database.get()));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);

    return jni::jbyteArrayToStr(env, jbytes);
}

std::string
JNIMetaClient::getPartitionsByFilter(const std::string & database, const std::string & table, const std::string & filter, int16_t max_parts)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("getPartitionsByFilter");
    jvalue return_val;
    jni::AutoLocalJobject jstr_database = jni::newJString(env, database);
    jni::AutoLocalJobject jstr_table = jni::newJString(env, table);
    jni::AutoLocalJobject jstr_filter = jni::newJString(env, filter);
    jshort jshort_max_parts = max_parts;
    THROW_JNI_EXCEPTION(
        env,
        jni::invokeObjectMethod(
            env,
            &return_val,
            obj,
            method.method_id,
            method.signature.c_str(),
            jstr_database.get(),
            jstr_table.get(),
            jstr_filter.get(),
            jshort_max_parts));
    jni::AutoLocalJobject jbytes = static_cast<jbyteArray>(return_val.l);
    return jni::jbyteArrayToStr(env, jbytes);
}

std::vector<std::string> JNIMetaClient::listDatabases()
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("listDatabases");
    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str()));
    jni::AutoLocalJobject jobjarrays = return_val.l;
    return jni::jstringsToStrings(env, jobjarrays);
}


std::vector<std::string> JNIMetaClient::listTables(const std::string & database)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("listTables");
    jvalue return_val;
    jni::AutoLocalJobject jstr_database = jni::newJString(env, database);

    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jstr_database.get()));
    jni::AutoLocalJobject jobjarrays = return_val.l;
    return jni::jstringsToStrings(env, jobjarrays);
}

bool JNIMetaClient::isTableExist(const std::string & database, const std::string & table)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("isTableExists");
    jni::AutoLocalJobject jstr_database = jni::newJString(env, database);
    jni::AutoLocalJobject jstr_table = jni::newJString(env, table);
    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env,
        jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jstr_database.get(), jstr_table.get()));
    return return_val.z;
}

}
