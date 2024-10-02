#pragma clang diagnostic ignored "-Wunused-macros"

#include "jni/JNIHelper.h"

#include <cassert>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

static constexpr auto CLASSPATH = "CLASSPATH";
static constexpr auto OPT_CLASSPATH = "-Djava.class.path=";
static constexpr auto JVM_ARGS = "LIBHDFS_OPTS";

/** The Native return types that methods could return */
#define JVOID 'V'
#define JOBJECT 'L'
#define JARRAYOBJECT '['
#define JBOOLEAN 'Z'
#define JBYTE 'B'
#define JCHAR 'C'
#define JSHORT 'S'
#define JINT 'I'
#define JLONG 'J'
#define JFLOAT 'F'
#define JDOUBLE 'D'

jclass jcls_class = nullptr;
jclass jcls_list = nullptr;
jclass jcls_hashmap = nullptr;
jclass jcls_exception_utils = nullptr;
static void init_common_classes(JNIEnv * env)
{
    if (!jcls_class)
        DB::jni::findClass(env, &jcls_class, "java/lang/Class");
    if (!jcls_list)
        DB::jni::findClass(env, &jcls_list, "java/util/List");
    if (!jcls_hashmap)
        DB::jni::findClass(env, &jcls_hashmap, "java/util/HashMap");
    if (!jcls_exception_utils)
        DB::jni::findClass(env, &jcls_exception_utils, "org/apache/commons/lang3/exception/ExceptionUtils");
}

static std::mutex jni_global_mutex;

/// global mutex must be acquired to call this function
static JNIEnv * getGlobalJNIEnv()
{
    static constexpr auto VM_BUF_LENGTH = 1;
    JavaVM * vm_buf[VM_BUF_LENGTH];
    jint num_vms = 0;
    JavaVM * vm;
    JNIEnv * jni_env;
    jint rv = JNI_GetCreatedJavaVMs(&vm_buf[0], VM_BUF_LENGTH, &num_vms);
    if (rv != 0)
    {
        throw std::runtime_error("JNI_GetCreatedJavaVMs failed with error: " + rv);
    }

    if (num_vms == 0)
    {
        std::vector<std::string> options = {"-Djdk.lang.processReaperUseDefaultStackSize=true", "-Xrs"};

        char * class_path = getenv(CLASSPATH);
        if (class_path != nullptr)
        {
            std::string opt_class_path = OPT_CLASSPATH;
            opt_class_path.append(class_path);
            options.push_back(std::move(opt_class_path));
        }
        else
        {
            throw std::runtime_error("Environment variable CLASSPATH not set!");
        }

        char * jvm_args = getenv(JVM_ARGS);
        if (jvm_args != nullptr)
        {
            std::string opt_jvm_args = jvm_args;
            std::istringstream iss(opt_jvm_args);
            std::string option;
            while (std::getline(iss, option, ' '))
            {
                options.push_back(option);
            }
        }

        JavaVMInitArgs vm_args;
        JavaVMOption vm_options[options.size()];
        for (size_t i = 0; i < options.size(); ++i)
        {
            vm_options[i].optionString = new char[options[i].size() + 1];
            std::strcpy(vm_options[i].optionString, options[i].c_str());
        }

        vm_args.version = JNI_VERSION_1_8;
        vm_args.options = vm_options;
        vm_args.nOptions = options.size();
        vm_args.ignoreUnrecognized = JNI_TRUE;

        rv = JNI_CreateJavaVM(&vm, reinterpret_cast<void **>(&jni_env), &vm_args);
        if (rv != 0)
        {
            throw std::runtime_error("JNI_CreateJavaVM failed with error: " + rv);
        }
        init_common_classes(jni_env);
    }
    else
    {
        vm = vm_buf[0];
        rv = vm->AttachCurrentThread(reinterpret_cast<void **>(&jni_env), nullptr);
        if (rv != 0)
        {
            throw std::runtime_error("AttachCurrentThread failed with error: " + rv);
        }
    }

    return jni_env;
}

#ifndef DISABLE_BTHREAD_CHECK
extern "C" {
uint64_t bthread_self();
}
#endif

namespace DB
{

JNIEnv * JNIHelper::getJNIEnv()
{
#ifndef DISABLE_BTHREAD_CHECK
    // bthread_self returns 0 if the current thread is not a bthread.
    // But if you set FLAGS_usercode_in_pthread to true, and this check may also return positive number.
    // So if you set FLAGS_usercode_in_pthread to true, you should not use this check.
    if (::bthread_self() != 0)
    {
        throw std::logic_error("JNIHelper::getJNIEnv() should not be called in bthread");
    }
#endif
    if (cached_env == nullptr)
    {
        /// just use global mutex here
        std::lock_guard lock(jni_global_mutex);
        cached_env = getGlobalJNIEnv();
        if (cached_env == nullptr)
        {
            throw std::runtime_error("Failed to get JNIEnv for unknown reason");
        }
    }

    return cached_env;
}

namespace jni
{
    thread_local std::string tls_query_id;

    jthrowable getPendingExceptionAndClear(JNIEnv * env)
    {
        jthrowable jthr = env->ExceptionOccurred();
        if (jthr == nullptr)
            return jthr;

        env->ExceptionDescribe();
        env->ExceptionClear();
        return jthr;
    }

    jthrowable findClass(JNIEnv * env, jclass * out, const char * className)
    {
        jthrowable jthr = nullptr;
        jclass jcls_global = nullptr;
        jclass jcls_local = nullptr;
        do
        {
            jcls_local = env->FindClass(className);
            if (!jcls_local)
            {
                jthr = getPendingExceptionAndClear(env);
                break;
            }
            jcls_global = static_cast<jclass>(env->NewGlobalRef(jcls_local));
            if (!jcls_global)
            {
                jthr = getPendingExceptionAndClear(env);
                break;
            }
            *out = jcls_global;
            jthr = nullptr;
        } while (false);

        if (jthr && jcls_global)
        {
            env->DeleteGlobalRef(jcls_global);
        }
        if (jcls_local)
        {
            env->DeleteLocalRef(jcls_local);
        }
        return jthr;
    }

    jthrowable
    findMethodId(JNIEnv * env, jmethodID * out, jclass cls, const char * methodName, const char * methodSiganture, bool static_method)
    {
        jmethodID mid = nullptr;
        if (static_method)
            mid = env->GetStaticMethodID(cls, methodName, methodSiganture);
        else
            mid = env->GetMethodID(cls, methodName, methodSiganture);
        if (mid == nullptr)
            return getPendingExceptionAndClear(env);
        *out = mid;
        return nullptr;
    }

    // NOLINTNEXTLINE
    jthrowable invokeObjectMethod(JNIEnv * env, jvalue * retval, jobject instObj, jmethodID mid, const char * methSignature, ...)
    {
        va_list args;
        jthrowable jthr;
        const char * str;
        char return_type;

        str = methSignature;
        while (*str != ')')
            str++;
        str++;
        return_type = *str;
        va_start(args, methSignature);
        if (return_type == JOBJECT || return_type == JARRAYOBJECT)
        {
            jobject jobj = nullptr;
            jobj = env->CallObjectMethodV(instObj, mid, args);
            retval->l = jobj;
        }
        else if (return_type == JVOID)
        {
            env->CallVoidMethodV(instObj, mid, args);
        }
        else if (return_type == JBOOLEAN)
        {
            jboolean jbool = 0;
            jbool = env->CallBooleanMethodV(instObj, mid, args);
            retval->z = jbool;
        }
        else if (return_type == JSHORT)
        {
            jshort js = 0;
            js = env->CallShortMethodV(instObj, mid, args);
            retval->s = js;
        }
        else if (return_type == JLONG)
        {
            jlong jl = -1;
            jl = env->CallLongMethodV(instObj, mid, args);
            retval->j = jl;
        }
        else if (return_type == JINT)
        {
            jint ji = -1;
            ji = env->CallIntMethodV(instObj, mid, args);
            retval->i = ji;
        }
        va_end(args);

        jthr = env->ExceptionOccurred();
        if (jthr)
        {
            env->ExceptionDescribe();
            env->ExceptionClear();
            return jthr;
        }
        return nullptr;
    }

    // NOLINTNEXTLINE
    jthrowable invokeStaticMethod(JNIEnv * env, jvalue * retval, jclass cls, jmethodID mid, const char * methSignature, ...)
    {
        va_list args;
        jthrowable jthr;
        const char * str;
        char return_type;

        str = methSignature;
        while (*str != ')')
            str++;
        str++;
        return_type = *str;
        va_start(args, methSignature);
        if (return_type == JOBJECT || return_type == JARRAYOBJECT)
        {
            jobject jobj = nullptr;
            jobj = env->CallStaticObjectMethodV(cls, mid, args);
            retval->l = jobj;
        }
        else if (return_type == JVOID)
        {
            env->CallStaticVoidMethodV(cls, mid, args);
        }
        else if (return_type == JBOOLEAN)
        {
            jboolean jbool = 0;
            jbool = env->CallStaticBooleanMethodV(cls, mid, args);
            retval->z = jbool;
        }
        else if (return_type == JSHORT)
        {
            jshort js = 0;
            js = env->CallStaticShortMethodV(cls, mid, args);
            retval->s = js;
        }
        else if (return_type == JLONG)
        {
            jlong jl = -1;
            jl = env->CallStaticLongMethodV(cls, mid, args);
            retval->j = jl;
        }
        else if (return_type == JINT)
        {
            jint ji = -1;
            ji = env->CallStaticIntMethodV(cls, mid, args);
            retval->i = ji;
        }
        va_end(args);

        jthr = env->ExceptionOccurred();
        if (jthr)
        {
            env->ExceptionDescribe();
            env->ExceptionClear();
            return jthr;
        }
        return nullptr;
    }

    std::string jstringToStr(JNIEnv * env, jstring jstr)
    {
        if (!jstr)
            return "";
        const char * chars = env->GetStringUTFChars(jstr, nullptr);
        std::string res = chars;
        env->ReleaseStringUTFChars(jstr, chars);
        return res;
    }

    std::string jbyteArrayToStr(JNIEnv * env, jbyteArray obj)
    {
        jbyte * bytes = env->GetByteArrayElements(obj, nullptr);
        jsize length = env->GetArrayLength(obj);
        std::string result(reinterpret_cast<char *>(bytes), length);
        env->ReleaseByteArrayElements(obj, bytes, JNI_ABORT);
        return result;
    }

    jobject newJByteArray(JNIEnv * env, const char * data, size_t size)
    {
        jbyteArray jbytes = env->NewByteArray(size);
        env->SetByteArrayRegion(jbytes, 0, size, reinterpret_cast<const jbyte *>(data));
        return jbytes;
    }

    jobject getFromJmap(JNIEnv * env, jobject jmap, const std::string & key)
    {
        const char * signature_get = "(Ljava/lang/Object;)Ljava/lang/Object;";
        jmethodID mid_get;
        CHECK_JNI_EXCEPTION(findMethodId(env, &mid_get, jcls_hashmap, "get", signature_get, false), "Cannot find get method");

        AutoLocalJobject jstr_key = env->NewStringUTF(key.c_str());
        jvalue return_val;
        THROW_JNI_EXCEPTION(env, invokeObjectMethod(env, &return_val, jmap, mid_get, signature_get, jstr_key.get()));
        return return_val.l;
    }

    jobject mapToJmap(JNIEnv * env, const std::map<std::string, std::string> & params)
    {
        const char * signature_ctor = "()V";
        const char * signature_put = "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;";
        jmethodID mid_ctor;
        CHECK_JNI_EXCEPTION(findMethodId(env, &mid_ctor, jcls_hashmap, "<init>", signature_ctor, false), "Cannot find constructor");
        jmethodID mid_put;
        CHECK_JNI_EXCEPTION(findMethodId(env, &mid_put, jcls_hashmap, "put", signature_put, false), "Cannot find put method");

        jobject jmap = env->NewObject(jcls_hashmap, mid_ctor);

        jvalue return_val;
        for (const auto & entry : params)
        {
            AutoLocalJobject jstr_key = env->NewStringUTF(entry.first.c_str());
            AutoLocalJobject jstr_value = env->NewStringUTF(entry.second.c_str());
            return_val = {};
            THROW_JNI_EXCEPTION(env, invokeObjectMethod(env, &return_val, jmap, mid_put, signature_put, jstr_key.get(), jstr_value.get()));
            AutoLocalJobject tmp = return_val.l;
        }
        return jmap;
    }

    std::vector<std::string> jlistByteArrayToVector(JNIEnv * env, jobject jlist)
    {
        // Find the List class and its methods
        AutoLocalJobject jcls_list = env->GetObjectClass(jlist);
        const char * signature_size = "()I";
        const char * signagure_get = "(I)Ljava/lang/Object;";
        jmethodID mid_size;
        CHECK_JNI_EXCEPTION(findMethodId(env, &mid_size, jcls_list, "size", signature_size, false), "Cannot find size method");
        jmethodID mid_get;
        CHECK_JNI_EXCEPTION(findMethodId(env, &mid_get, jcls_list, "get", signagure_get, false), "Cannot find get method");

        // Get the size of the Java list
        jvalue return_val;
        THROW_JNI_EXCEPTION(env, invokeObjectMethod(env, &return_val, jlist, mid_size, signature_size));
        jint size = return_val.i;

        // Create a C++ vector to store the strings
        std::vector<std::string> vec;
        vec.reserve(size);

        // Iterate over the Java list and convert each element to std::string
        for (jint i = 0; i < size; ++i)
        {
            return_val = {};
            THROW_JNI_EXCEPTION(env, invokeObjectMethod(env, &return_val, jlist, mid_get, signagure_get, i));
            AutoLocalJobject jbytes = return_val.l;

            // Get the length of the byte array
            jsize byte_array_length = env->GetArrayLength(jbytes);

            // Get the bytes from the byte array
            jbyte * byte_array_elements = env->GetByteArrayElements(jbytes, nullptr);

            // Convert the bytes to a std::string
            std::string str(reinterpret_cast<char *>(byte_array_elements), byte_array_length);

            // Release the byte array elements
            env->ReleaseByteArrayElements(jbytes, byte_array_elements, JNI_ABORT);

            // Add the string to the vector
            vec.push_back(str);
        }

        return vec;
    }

    /// DO NOT throw exception in this function
    std::string getExceptionSummary(JNIEnv * env, jthrowable jthr)
    {
        AutoLocalJobject jcls_execption = env->GetObjectClass(jthr);
        jmethodID mid_get_message;
        const char * signature_get_message = "()Ljava/lang/String;";
        if (findMethodId(env, &mid_get_message, jcls_execption, "getMessage", signature_get_message, false))
            return "Cannot find getMessage method";

        jvalue return_val;
        if (AutoLocalJobject jthr_get_message = invokeObjectMethod(env, &return_val, jthr, mid_get_message, signature_get_message);
            jthr_get_message)
            return "Failed to get exception message";

        AutoLocalJobject jstr = return_val.l;
        return jstringToStr(env, jstr);
    }

    /// DO NOT throw exception in this function
    std::string getStackTrace(JNIEnv * env, jthrowable jthr)
    {
        jmethodID mid_get_stack_trace;
        const char * signature_get_stack_strace = "(Ljava/lang/Throwable;)Ljava/lang/String;";
        if (findMethodId(env, &mid_get_stack_trace, jcls_exception_utils, "getStackTrace", signature_get_stack_strace, true))
            return "Cannot find getStackTrace method";

        jvalue return_val;
        if (AutoLocalJobject jthr_get_stack_trace
            = invokeStaticMethod(env, &return_val, jcls_exception_utils, mid_get_stack_trace, signature_get_stack_strace, jthr);
            jthr_get_stack_trace)
        {
            AutoLocalJobject jcls_execption = env->GetObjectClass(jthr);
            const char * signature_get_class_name = "()Ljava/lang/String;";
            std::string class_name = "unknown";
            jmethodID mid_get_class_name;
            if (!findMethodId(env, &mid_get_class_name, jcls_class, "getName", signature_get_class_name, false))
            {
                jvalue return_val_get_class_name;
                invokeObjectMethod(env, &return_val_get_class_name, jcls_execption, mid_get_class_name, signature_get_class_name);
                AutoLocalJobject jstr_class_name = return_val_get_class_name.l;
                class_name = jstringToStr(env, jstr_class_name);
            }
            return "Failed to get stack trace for unknown reason (exception type: " + class_name + ")";
        }

        AutoLocalJobject jstr = return_val.l;
        return jstringToStr(env, jstr);
    }

    jstring newJString(JNIEnv * env, const std::string & str)
    {
        return env->NewStringUTF(str.c_str());
    }

    std::vector<std::string> jstringsToStrings(JNIEnv * env, jobjectArray jstrs)
    {
        jsize length = env->GetArrayLength(jstrs);
        std::vector<std::string> vec(length);
        for (jsize i = 0; i < length; i++)
        {
            jstring jstr = static_cast<jstring>(env->GetObjectArrayElement(jstrs, i));
            const char * char_string = env->GetStringUTFChars(jstr, nullptr);
            vec[i] = std::string(char_string);
            env->ReleaseStringUTFChars(jstr, char_string);
        }
        return vec;
    }

    jthrowable findAllMethods(JNIEnv * env, jclass cls, std::string & all_methods)
    {
        jobject obj = env->AllocObject(cls);
        const char * signature_get_fields = "()[Ljava/lang/reflect/Method;";
        jmethodID mid_get_fields;
        CHECK_JNI_EXCEPTION(
            findMethodId(env, &mid_get_fields, cls, "getMethods", signature_get_fields, false), "Cannot find getMethods method");
        jvalue return_val;
        THROW_JNI_EXCEPTION(env, invokeObjectMethod(env, &return_val, obj, mid_get_fields, signature_get_fields));
        AutoLocalJobject jobjs = return_val.l;
        jsize len = env->GetArrayLength(jobjs);
        jsize i;

        for (i = 0; i < len; i++)
        {
            AutoLocalJobject obj_method = env->GetObjectArrayElement(jobjs, i);
            AutoLocalJobject jcls_method = env->GetObjectClass(obj_method);
            const char * signagure_get_name = "()Ljava/lang/String;";
            jmethodID mid_get_name;
            CHECK_JNI_EXCEPTION(
                findMethodId(env, &mid_get_name, jcls_method, "getName", signagure_get_name, false), "Cannot find getName method");
            return_val = {};
            THROW_JNI_EXCEPTION(env, invokeObjectMethod(env, &return_val, obj_method, mid_get_name, signagure_get_name));
            AutoLocalJobject jstr_name = return_val.l;
            const char * name = env->GetStringUTFChars(jstr_name, nullptr);

            all_methods = all_methods + name + " ";
            env->ReleaseStringUTFChars(jstr_name, name);
        }
        return nullptr;
    }

    void set_query_id(JNIEnv * env, jclass jcls_thread_context)
    {
        if (!jcls_thread_context || tls_query_id.empty())
            return;
        const char * signature_put = "(Ljava/lang/String;Ljava/lang/String;)V";
        jmethodID mid_put;
        CHECK_JNI_EXCEPTION(findMethodId(env, &mid_put, jcls_thread_context, "put", signature_put, true), "Cannot find put method");

        AutoLocalJobject jstr_key = newJString(env, "QueryId");
        AutoLocalJobject jstr_value = newJString(env, tls_query_id);
        jvalue return_val;
        THROW_JNI_EXCEPTION(
            env, invokeStaticMethod(env, &return_val, jcls_thread_context, mid_put, signature_put, jstr_key.get(), jstr_value.get()));
    }
}

}
