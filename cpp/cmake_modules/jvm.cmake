set(JAVA_HOME $ENV{JAVA_HOME})
if (NOT JAVA_HOME)
    message(FATAL_ERROR "env variable JAVA_HOME is not set")
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if (NOT JAVA_HOME)
        message(FATAL_ERROR "JAVA_HOME is not set")
    endif()
    message("JAVA_HOME: ${JAVA_HOME}")
    add_library(jvm SHARED IMPORTED)
    file(GLOB_RECURSE LIB_JVM ${JAVA_HOME}/jre/lib/*/server/libjvm.so)
    if("${LIB_JVM}" STREQUAL "")
        file(GLOB_RECURSE LIB_JVM ${JAVA_HOME}/lib/server/libjvm.so)
    endif()
    if("${LIB_JVM}" STREQUAL "")
        message(FATAL_ERROR "cannot find libjvm.so in ${JAVA_HOME}")
    endif()
    message("LIB_JVM: ${LIB_JVM}")
    set_target_properties(jvm PROPERTIES IMPORTED_LOCATION ${LIB_JVM})
    include_directories(${JAVA_HOME}/include)
    include_directories(${JAVA_HOME}/include/linux)
else ()
    find_package(JNI)
    add_library(jvm SHARED IMPORTED)
    include_directories(${JNI_INCLUDE_DIRS})
endif()
