cmake_minimum_required(VERSION 3.16)
project(java-extensions)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

file(GLOB SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
file(GLOB HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/include/jni/*.h")

OPTION(ENABLE_TEST "Enable tests" OFF)

add_library(clickhouse_jni_lib ${SOURCES} ${HEADERS})
target_include_directories(clickhouse_jni_lib SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")

# jni library
include(cmake_modules/jvm.cmake)
target_link_libraries(clickhouse_jni_lib PUBLIC jvm)

# protos
include(cmake_modules/proto.cmake)
target_link_libraries(clickhouse_jni_lib PUBLIC jni_data_protos)

# nanoarrow
include(cmake_modules/nanoarrow.cmake)
set_source_files_properties(JNIArrowStream.cpp PROPERTIES COMPILE_FLAGS "-w") # disable warning from nanoarrow
target_link_libraries(clickhouse_jni_lib PRIVATE nanoarrow)

if (ENABLE_TEST)
    find_package(GTest REQUIRED)

    add_definitions(-DDISABLE_BTHREAD_CHECK)

    file(GLOB TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp")
    add_executable(clickhouse_jni_test ${TEST_SOURCES})

    target_link_libraries(clickhouse_jni_test PRIVATE clickhouse_jni_lib)
    target_link_libraries(clickhouse_jni_test PRIVATE ${GTEST_LIBRARIES})
    target_link_libraries(clickhouse_jni_test PRIVATE ${GTEST_MAIN_LIBRARIES})
endif()
