include(FetchContent)
if (NANOARROW_PATH)
    set(NANOARROW_NAMESPACE "NanoArrow")
    FetchContent_Declare(
        nanoarrow_content
        SOURCE_DIR ${NANOARROW_PATH})
    FetchContent_MakeAvailable(nanoarrow_content)
else()
    set(NANOARROW_NAMESPACE "NanoArrow")
    FetchContent_Declare(
        nanoarrow_content
        URL https://github.com/apache/arrow-nanoarrow/releases/download/apache-arrow-nanoarrow-0.1.0/apache-arrow-nanoarrow-0.1.0.tar.gz
        URL_HASH SHA512=dc62480b986ee76aaad8e38c6fbc602f8cef2cc35a5f5ede7da2a93b4db2b63839bdca3eefe8a44ae1cb6895a2fd3f090e3f6ea1020cf93cfe86437304dfee17)
    FetchContent_MakeAvailable(nanoarrow_content)
endif()
