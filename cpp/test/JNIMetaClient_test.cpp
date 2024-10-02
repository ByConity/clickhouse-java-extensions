#include <map>
#include <string>
#include <gtest/gtest.h>
#include <jni/JNIHelper.h>
#include <jni/JNIMetaClient.h>

namespace DB
{

namespace jni
{
    extern thread_local std::string tls_query_id;
}

class JNIMetaClientTest : public JNIMetaClient
{
public:
    JNIMetaClientTest(const std::string & class_factory_full_classname, const std::string & full_classname, const std::string & pb_message)
        : JNIMetaClient(class_factory_full_classname, full_classname, pb_message)
    {
        registerMethod("initLocalFilesystem", "(Ljava/util/Map;)V");
    }

    void initLocalFilesystem(const std::map<std::string, std::string> & params)
    {
        auto * env = JNIHelper::getJNIEnv();
        const auto & method = getMethod("initLocalFilesystem");
        jni::AutoLocalJobject jmap = jni::mapToJmap(env, params);

        jvalue return_val;
        THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), jmap.get()));
    }
};

TEST(TestPaimonApi, getSplits)
{
    jni::tls_query_id = "test_query_id";
    auto jni_client = std::make_shared<JNIMetaClientTest>(
        "org/byconity/paimon/PaimonClassFactory",
        "org/byconity/paimon/PaimonMetaClient",
        R"({"metastoreType":"filesystem","filesystemType":"LOCAL","path":"/tmp/paimon"})");


    std::map<std::string, std::string> params;
    params["database"] = "test_db";
    params["table"] = "test_table";
    params["required_fields"] = "col_int,col_str";

    jni_client->initLocalFilesystem(params);

    auto scan_info = jni_client->getPaimonScanInfo(params);
}
}
