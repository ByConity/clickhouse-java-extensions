#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <jni.h>
#include <jni/JNIHelper.h>

namespace DB
{
class JNIModuleClassFactory
{
public:
    static JNIModuleClassFactory *
    getFactory(const std::string & factory_class_name, const std::map<std::string, std::string> & options = {});
    static std::unordered_map<std::string, std::unique_ptr<JNIModuleClassFactory>> factorys;

private:
    static std::mutex mutex;

public:
    explicit JNIModuleClassFactory(const std::string & factory_class_name, const std::map<std::string, std::string> & options = {});
    jclass getClass(const std::string & class_name);
    void setQueryId();


private:
    jni::AutoGlobalJobject cls_class_factory = nullptr;
    jni::AutoGlobalJobject cls_thread_context = nullptr;
    jni::AutoGlobalJobject obj_class_factory = nullptr;

    jmethodID mid_ctor;
    jmethodID mid_init;
    jmethodID mid_get_class;

    inline static constexpr char * method_name_ctor = "<init>";
    inline static constexpr char * method_name_init = "initModuleContext";
    inline static constexpr char * method_name_get_class = "getClass";
    inline static constexpr char * signature_ctor = "(Ljava/util/Map;)V";
    inline static constexpr char * signature_init = "()V";
    inline static constexpr char * signature_get_class = "(Ljava/lang/String;)Ljava/lang/Class;";
};

}
