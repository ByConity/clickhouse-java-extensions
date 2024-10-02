#pragma once
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#include <jni.h>
#include "jni/JNIModuleClassFactory.h"

#include <string>
#include <unordered_map>
#include <vector>
namespace DB
{
// General interface for byte creatable JNI class
// a factory method 'create(byte[])' should be implemented for each Java class
// single thread
class JNIByteCreatable
{
public:
    JNIByteCreatable(const std::string & class_factory_classname, const std::string & target_classname, const std::string & binary);

    virtual ~JNIByteCreatable();

protected:
    struct Method
    {
        std::string signature;
        jmethodID method_id;
    };
    void registerMethod(const std::string & name, const std::string & signature);
    const Method & getMethod(const std::string & name) { return methods[name]; }

    JNIModuleClassFactory * factory;
    jclass cls = nullptr;
    jobject obj = nullptr;
    std::unordered_map<std::string, Method> methods;
};
}
