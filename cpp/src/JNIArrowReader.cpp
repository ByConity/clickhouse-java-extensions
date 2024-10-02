#include "jni/JNIArrowReader.h"

#include <jni/JNIArrowStream.h>
#include "jni/JNIByteCreatable.h"
#include "jni/JNIHelper.h"

#include <stdexcept>
#include <system_error>
#include <string.h>

namespace DB
{

/// must check errcode != 0 before
static void throwStreamException(int errcode, struct ArrowArrayStream * stream)
{
    const char * errdesc = stream->get_last_error(stream);
    if (errdesc != nullptr)
    {
        throw std::runtime_error("Unexpected stream error " + std::string(errdesc));
    }
    else
    {
        throw std::runtime_error("Unexpected stream error code " + std::string(strerror(errcode)));
    }
}

JNIArrowReader::JNIArrowReader(
    const std::string & class_factory_full_classname, const std::string & full_classname, const std::string & pb_message)
    : JNIByteCreatable(class_factory_full_classname, full_classname, pb_message)
{
    stream.release = nullptr;
    schema.release = nullptr;

    registerMethod("initStream", "(J)V");
}

JNIArrowReader::~JNIArrowReader()
{
    if (schema.release)
        schema.release(&schema);
    if (stream.release)
        stream.release(&stream);
}

void JNIArrowReader::initStream()
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    const auto & method = getMethod("initStream");
    jvalue return_val;
    THROW_JNI_EXCEPTION(
        env, jni::invokeObjectMethod(env, &return_val, obj, method.method_id, method.signature.c_str(), reinterpret_cast<jlong>(&stream)));

    int errcode = stream.get_schema(&stream, &schema);
    if (errcode != 0)
    {
        throwStreamException(errcode, &stream);
    }
}

bool JNIArrowReader::next(ArrowArray & chunk)
{
    auto * env = JNIHelper::getJNIEnv();
    factory->setQueryId();
    int errcode = stream.get_next(&stream, &chunk);
    if (errcode != 0)
    {
        /// handle error
        throwStreamException(errcode, &stream);
        return false;
    }

    /// if eof, chunk.release is null
    return chunk.release != nullptr;
}

}
