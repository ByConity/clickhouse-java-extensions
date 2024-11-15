#include "jni/JNITableOperation.h"
#include <string>
#include <jni.h>
#include "jni/JNIHelper.h"

namespace DB
{
JNITableOperation::JNITableOperation(jobject obj_)
{
    auto * env = JNIHelper::getJNIEnv();
    table_obj = env->NewGlobalRef(obj_);
    auto cls_local = env->GetObjectClass(table_obj);
    cls = static_cast<jclass>(env->NewGlobalRef(cls));
}

JNITableOperation::~JNITableOperation()
{
    auto * env = JNIHelper::getJNIEnv();
    env->DeleteGlobalRef(cls);
    env->DeleteGlobalRef(table_obj);
}

void JNITableOperation::commit(const std::string & bytes)
{
    static std::string method = "commit";
    static std::string signature = "()[B";
    callJNI(method, signature, bytes);
}

void JNITableOperation::abort(const std::string & bytes)
{
    static std::string method = "abort";
    static std::string signature = "()[B";
    callJNI(method, signature, bytes);
}

void JNITableOperation::callJNI(const std::string & method_name, const std::string & signature, const std::string & binary)
{
    auto * env = JNIHelper::getJNIEnv();
    jmethodID mid;
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid, static_cast<jclass>(cls), method_name.c_str(), signature.c_str(), false),
        "Unable to find method " + method_name);

    jvalue return_val;
    jni::AutoLocalJobject jbinary = jni::newJByteArray(env, binary.c_str(), binary.size());

    THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, table_obj, mid, signature.c_str(), jbinary.get()));
}


}