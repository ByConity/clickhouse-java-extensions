#include <gtest/gtest.h>

#include <thread>
#include <jni/JNIHelper.h>

namespace DB
{
TEST(JNIHElpers, concurrencySafety)
{
    static constexpr size_t THREAD_NUM = 64;
    static constexpr size_t SIZE_1M = 1024 * 1024;

    std::vector<std::thread> threads;
    std::atomic<size_t> count = 0;

    for (size_t i = 0; i < THREAD_NUM; ++i)
    {
        threads.emplace_back([&count]() {
            JNIEnv * env = DB::JNIHelper::getJNIEnv();
            jni::AutoLocalJobject jbytes = env->NewByteArray(SIZE_1M);
            count += env->GetArrayLength(jbytes);
        });
    }

    for (size_t i = 0; i < THREAD_NUM; ++i)
    {
        threads[i].join();
    }

    ASSERT_EQ(count, THREAD_NUM * SIZE_1M);
}

TEST(JNIHelpers, memorySafety)
{
    static constexpr size_t TIMES = 1024;
    static constexpr size_t SIZE_1M = 1024 * 1024;

    JNIEnv * env = DB::JNIHelper::getJNIEnv();
    size_t count = 0;
    for (size_t i = 0; i < TIMES; ++i)
    {
        jni::AutoLocalJobject jbytes = env->NewByteArray(SIZE_1M);
        count += env->GetArrayLength(jbytes);
    }

    ASSERT_EQ(count, TIMES * SIZE_1M);
}
}
