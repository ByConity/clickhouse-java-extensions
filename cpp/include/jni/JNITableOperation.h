#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>
#include <jni.h>
#include <jni/JNIByteCreatable.h>

namespace DB
{

class JNITableOperation
{
public:
    JNITableOperation(jobject obj_);
    ~JNITableOperation();
    void commit(const std::string & binary);
    void abort(const std::string & binary);

private:
    void callJNI(const std::string & method_name, const std::string & signature, const std::string & binary);
    jobject table_obj; // in java, TableOperation
    jclass cls = nullptr;
};

}