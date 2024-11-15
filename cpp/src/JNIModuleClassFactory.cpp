#include "jni/JNIModuleClassFactory.h"

#include <algorithm>
#include "jni/JNIHelper.h"

namespace DB
{
std::unordered_map<std::string, std::unique_ptr<JNIModuleClassFactory>> JNIModuleClassFactory::factorys;
std::mutex JNIModuleClassFactory::mutex;

JNIModuleClassFactory *
JNIModuleClassFactory::getFactory(const std::string & factory_class_name, const std::map<std::string, std::string> & options)
{
    std::string key = factory_class_name;
    for (const auto & [k, v] : options)
    {
        key += "," + k + "=" + v;
    }
    std::lock_guard<std::mutex> l(mutex);
    if (factorys.find(key) == factorys.end())
    {
        factorys[key] = std::make_unique<JNIModuleClassFactory>(factory_class_name, options);
    }
    return factorys[key].get();
}

JNIModuleClassFactory::JNIModuleClassFactory(const std::string & factory_class_name, const std::map<std::string, std::string> & options)
{
    auto * env = JNIHelper::getJNIEnv();
    CHECK_JNI_EXCEPTION(
        jni::findClass(env, static_cast<jclass *>(cls_class_factory.address()), factory_class_name.c_str()),
        "Cannot find class " + factory_class_name);

    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_ctor, cls_class_factory, method_name_ctor, signature_ctor, false),
        "Cannot find class factory ctor method");
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_init, cls_class_factory, method_name_init, signature_init, false),
        "Cannot find class factory init method");
    CHECK_JNI_EXCEPTION(
        jni::findMethodId(env, &mid_get_class, cls_class_factory, method_name_get_class, signature_get_class, false),
        "Cannot find class factory method");

    // Create instance
    {
        jobject local_obj_class_factory;
        jni::AutoLocalJobject joptions = jni::mapToJmap(env, options);
        THROW_JNI_EXCEPTION(env, jni::invokeNewObject(env, &local_obj_class_factory, cls_class_factory, mid_ctor, joptions.get()));
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
