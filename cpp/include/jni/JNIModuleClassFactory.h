#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <jni.h>

namespace DB
{
class JNIModuleClassFactory
{
public:
    static JNIModuleClassFactory * getFactory(const std::string & factory_class_name);
    static std::unordered_map<std::string, std::unique_ptr<JNIModuleClassFactory>> factorys;

private:
    static std::mutex mutex;

public:
    explicit JNIModuleClassFactory(const std::string & factory_class_name);
    jclass getClass(const std::string & class_name);
    void setQueryId();


private:
    jclass cls_class_factory = nullptr;
    jclass cls_thread_context = nullptr;
    jobject obj_class_factory = nullptr;

    jmethodID mid_ctor;
    jmethodID mid_init;
    jmethodID mid_get_class;

    inline static constexpr char * method_name_ctor = "<init>";
    inline static constexpr char * method_name_init = "initModuleContext";
    inline static constexpr char * method_name_get_class = "getClass";
    inline static constexpr char * signature_ctor = "()V";
    inline static constexpr char * signature_init = "()V";
    inline static constexpr char * signature_get_class = "(Ljava/lang/String;)Ljava/lang/Class;";
};

}
