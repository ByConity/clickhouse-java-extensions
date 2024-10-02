#include "jni/JNIModuleClassFactory.h"

#include <algorithm>
#include "jni/JNIHelper.h"

namespace DB
{
std::unordered_map<std::string, std::unique_ptr<JNIModuleClassFactory>> JNIModuleClassFactory::factorys;
std::mutex JNIModuleClassFactory::mutex;

JNIModuleClassFactory * JNIModuleClassFactory::getFactory(const std::string & factory_class_name)
{
    std::lock_guard<std::mutex> l(mutex);
    if (factorys.find(factory_class_name) == factorys.end())
    {
        factorys[factory_class_name] = std::make_unique<JNIModuleClassFactory>(factory_class_name);
    }
    return factorys[factory_class_name].get();
}

JNIModuleClassFactory::JNIModuleClassFactory(const std::string & factory_class_name)
{
    auto * env = JNIHelper::getJNIEnv();
    CHECK_JNI_EXCEPTION(jni::findClass(env, &cls_class_factory, factory_class_name.c_str()), "Cannot find class " + factory_class_name);

    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_ctor, cls_class_factory, method_name_ctor, signature_ctor, false), "Cannot find class factory method");
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_init, cls_class_factory, method_name_init, signature_init, false), "Cannot find class factory method");
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_get_class, cls_class_factory, method_name_get_class, signature_get_class, false),
        "Cannot find class factory method");

    // Create instance
    {
        jni::AutoLocalJobject local_obj_class_factory = env->NewObject(cls_class_factory, mid_ctor);
        obj_class_factory = env->NewGlobalRef(local_obj_class_factory);
    }

    // Call init method
    {
        jvalue return_val;
        THROW_JNI_EXCEPTION(env, jni::invokeObjectMethod(env, &return_val, obj_class_factory, mid_init, signature_init));
    }

    // Load org.apache.logging.log4j.ThreadContext
    {
        cls_thread_context = getClass("org.apache.logging.log4j.ThreadContext");
    }
}

jclass JNIModuleClassFactory::getClass(const std::string & class_name)
{
    auto * env = JNIHelper::getJNIEnv();
    std::string dot_class_name = class_name;
    std::replace(dot_class_name.begin(), dot_class_name.end(), '/', '.');
    jni::AutoLocalJobject jstr_dot_classname = env->NewStringUTF(dot_class_name.c_str());
    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, obj_class_factory, mid_get_class, signature_get_class, jstr_dot_classname.get()));
    jni::AutoLocalJobject cls = return_val.l;
    return static_cast<jclass>(env->NewGlobalRef(cls));
}

void JNIModuleClassFactory::setQueryId()
{
    auto * env = JNIHelper::getJNIEnv();
    jni::set_query_id(env, cls_thread_context);
}
}
