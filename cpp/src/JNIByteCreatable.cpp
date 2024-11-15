#include "jni/JNIByteCreatable.h"

#include "jni/JNIHelper.h"

#include <cassert>

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{

JNIByteCreatable::JNIByteCreatable(
    const std::string & class_factory_classname,
    const std::string & target_classname,
    const std::string & binary,
    const std::map<std::string, std::string> & options)
{
    auto * env = JNIHelper::getJNIEnv();

    factory = JNIModuleClassFactory::getFactory(class_factory_classname, options);
    factory->setQueryId();
    cls = factory->getClass(target_classname);

    std::string signature_create = "([B)L" + target_classname + ";";
    jmethodID mid_create;
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_create, cls, "create", signature_create.c_str(), true), "Cannot find static create factory method");

    // build byte array
    jni::AutoLocalJobject jbyte_array = jni::newJByteArray(env, binary.c_str(), binary.size());

    // invoke factory method
    jvalue return_val;
    THROW_JNI_EXCEPTION(env, jni::invokeStaticMethod(env, &return_val, cls, mid_create, signature_create.c_str(), jbyte_array.get()));
    jni::AutoLocalJobject jobj = return_val.l;
    obj = env->NewGlobalRef(jobj);
}

void JNIByteCreatable::registerMethod(const std::string & name, const std::string & signature)
{
    auto * env = JNIHelper::getJNIEnv();
    auto it = methods.find(name);
    if (it != methods.end())
        abort(); /// duplicate method, logical error

    jmethodID mid;
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid, static_cast<jclass>(cls), name.c_str(), signature.c_str(), false), "Unable to find method " + name);
    methods[name] = Method{signature, mid};
}
}
