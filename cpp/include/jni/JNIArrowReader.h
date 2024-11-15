#pragma once

#include "JNIArrowStream.h"
#include "JNIByteCreatable.h"

namespace DB
{

class JNIArrowReader : public JNIByteCreatable
{
public:
    JNIArrowReader(
        const std::string & class_factory_full_classname,
        const std::string & full_classname,
        const std::string & pb_message,
        const std::map<std::string, std::string> & options = {});
    ~JNIArrowReader() override;

    ArrowSchema & getSchema() { return schema; }

    void initStream();
    bool next(ArrowArray & chunk);
    void close();

private:
    ArrowArrayStream stream;
    ArrowSchema schema;
};
}
