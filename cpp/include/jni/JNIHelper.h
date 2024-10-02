#pragma once

#include <map>
#include <string>
#include <vector>

#pragma clang diagnostic ignored "-Wreserved-id-macro"
#include <jni.h>

namespace DB
{
class JNIHelper final
{
public:
    /// Get the JNIEnv for the given thread, Please Call this function to get one every time you need it!!!
    /// if no JVM exists, the global one will be created
    /// CLASSPATH environment variable
    /// similar to hadoop-hdfs/src/main/native/libhdfs/jni_helper.h
    static JNIEnv * getJNIEnv();

private:
    JNIHelper() = default;
    inline static thread_local JNIEnv * cached_env;
};

namespace jni
{
    jthrowable findClass(JNIEnv * env, jclass * out, const char * className);
    jthrowable
    findMethodId(JNIEnv * env, jmethodID * out, jclass cls, const char * methodName, const char * methodSiganture, bool static_method);
    jthrowable findAllMethods(JNIEnv * env, jclass cls, std::string & all_methods);

    jthrowable invokeObjectMethod(JNIEnv * env, jvalue * retval, jobject instObj, jmethodID mid, const char * methSignature, ...);
    jthrowable invokeStaticMethod(JNIEnv * env, jvalue * retval, jclass cls, jmethodID mid, const char * methSignature, ...);

    std::string jstringToStr(JNIEnv * env, jstring jstr);
    std::string jbyteArrayToStr(JNIEnv * env, jbyteArray obj);
    jobject newJByteArray(JNIEnv * env, const char * data, size_t size);
    jobject getFromJmap(JNIEnv * env, jobject jmap, const std::string & key);
    jobject mapToJmap(JNIEnv * env, const std::map<std::string, std::string> & params);
    std::vector<std::string> jlistByteArrayToVector(JNIEnv * env, jobject jlist);
    std::string getExceptionSummary(JNIEnv * env, jthrowable jthr);
    std::string getStackTrace(JNIEnv * env, jthrowable jthr);
    jstring newJString(JNIEnv * env, const std::string & str);
    std::vector<std::string> jstringsToStrings(JNIEnv *, jobjectArray jstrs);

    void set_query_id(JNIEnv * env, jclass jcls_thread_context);

    enum RefType
    {
        LOCAL,
        GLOBAL,
        WEAK_GLOBAL,
    };


    template <RefType ref_type>
    class AutoJobject
    {
    public:
        // NOLINTBEGIN(google-explicit-constructor, google-runtime-int)
        AutoJobject(jobject obj_) : obj(obj_) { }
        AutoJobject() : obj(nullptr) { }
        AutoJobject(const AutoJobject &) = delete;
        AutoJobject(AutoJobject &&) = delete;
        AutoJobject & operator=(const AutoJobject &) = delete;
        AutoJobject & operator=(AutoJobject &&) = delete;
        AutoJobject & operator=(jobject obj_)
        {
            release();
            obj = obj_;
            return *this;
        }
        ~AutoJobject() { release(); }

        jobject get() { return obj; }
        operator bool() const { return obj != nullptr; }
        operator jobject() const { return obj; }
        operator jclass() const { return static_cast<jclass>(obj); }
        operator jthrowable() const { return static_cast<jthrowable>(obj); }
        operator jstring() const { return static_cast<jstring>(obj); }
        operator jarray() const { return static_cast<jarray>(obj); }
        operator jbooleanArray() const { return static_cast<jbooleanArray>(obj); }
        operator jbyteArray() const { return static_cast<jbyteArray>(obj); }
        operator jcharArray() const { return static_cast<jcharArray>(obj); }
        operator jshortArray() const { return static_cast<jshortArray>(obj); }
        operator jintArray() const { return static_cast<jintArray>(obj); }
        operator jlongArray() const { return static_cast<jlongArray>(obj); }
        operator jfloatArray() const { return static_cast<jfloatArray>(obj); }
        operator jdoubleArray() const { return static_cast<jdoubleArray>(obj); }
        operator jobjectArray() const { return static_cast<jobjectArray>(obj); }
        // NOLINTEND(google-explicit-constructor, google-runtime-int)
    private:
        void release()
        {
            if constexpr (RefType::LOCAL == ref_type)
            {
                if (obj != nullptr)
                    JNIHelper::getJNIEnv()->DeleteLocalRef(obj);
            }
            else if constexpr (RefType::GLOBAL == ref_type)
            {
                if (obj != nullptr)
                    JNIHelper::getJNIEnv()->DeleteGlobalRef(obj);
            }
            else if constexpr (RefType::WEAK_GLOBAL == ref_type)
            {
                if (obj != nullptr)
                    JNIHelper::getJNIEnv()->DeleteWeakGlobalRef(obj);
            }
        }
        jobject obj;
    };

    using AutoLocalJobject = AutoJobject<RefType::LOCAL>;
    using AutoGlobalJobject = AutoJobject<RefType::GLOBAL>;
    using AutoWeakGlobalJobject = AutoJobject<RefType::WEAK_GLOBAL>;
}

// Concat x and y
#define TOKEN_CONCAT(x, y) x##y
// Make sure x and y are fully expanded
#define TOKEN_CONCAT_FORWARD(x, y) TOKEN_CONCAT(x, y)

#define CHECK_JNI_EXCEPTION(expr, errorMsg) \
    do \
    { \
        jni::AutoLocalJobject TOKEN_CONCAT_FORWARD(jthr, __LINE__) = (expr); \
        if ((TOKEN_CONCAT_FORWARD(jthr, __LINE__)).operator bool()) \
        { \
            throw std::runtime_error(errorMsg); \
        } \
    } while (false)

#define THROW_JNI_EXCEPTION(env, expr) \
    do \
    { \
        jni::AutoLocalJobject TOKEN_CONCAT_FORWARD(jthr, __LINE__) = (expr); \
        if ((TOKEN_CONCAT_FORWARD(jthr, __LINE__)).operator bool()) \
        { \
            throw std::runtime_error( \
                "Receive JNI exception, message: " + jni::getExceptionSummary((env), (TOKEN_CONCAT_FORWARD(jthr, __LINE__))) \
                + ", stack: " + jni::getStackTrace((env), (TOKEN_CONCAT_FORWARD(jthr, __LINE__)))); \
        } \
    } while (false)

}
