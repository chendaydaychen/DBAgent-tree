cmake_minimum_required(VERSION 3.25)
project(DataAgent_demo)

# ===== 编译标准 =====
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ===== 编译选项 =====
add_compile_options(-Wno-deprecated-declarations)
add_compile_definitions(NOGRAPHITE=1)

# Debug / Release
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")


# ===== 头文件目录 =====
include_directories(
        ${PROJECT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/Server
        ${PROJECT_SOURCE_DIR}/Storage
        ${PROJECT_SOURCE_DIR}/Storage/System
        ${PROJECT_SOURCE_DIR}/Storage/DataFormat
        ${PROJECT_SOURCE_DIR}/Storage/concurrency_control
        ${PROJECT_SOURCE_DIR}/Storage/test
)

# ===== 源码文件 =====
file(GLOB_RECURSE SRC_FILES
        ${PROJECT_SOURCE_DIR}/main.cpp
        ${PROJECT_SOURCE_DIR}/Server/*.cpp
        ${PROJECT_SOURCE_DIR}/Storage/System/*.cpp
        ${PROJECT_SOURCE_DIR}/Storage/DataFormat/*.cpp
        ${PROJECT_SOURCE_DIR}/Storage/concurrency_control/*.cpp
        ${PROJECT_SOURCE_DIR}/Storage/test/*.cpp
)

add_executable(rundb ${SRC_FILES})
target_link_options(rundb PRIVATE -no-pie)

# ===== 线程库 =====
find_package(Threads REQUIRED)
target_link_libraries(rundb PRIVATE Threads::Threads)

# jemalloc: 先本地静态库，再系统库
set(LOCAL_JEMALLOC "${PROJECT_SOURCE_DIR}/Storage/libs/libjemalloc.a")

if (EXISTS "${LOCAL_JEMALLOC}")
        target_link_libraries(rundb PRIVATE "${LOCAL_JEMALLOC}")
        target_compile_definitions(rundb PRIVATE USE_JEMALLOC=1)
        message(STATUS "Using local jemalloc: ${LOCAL_JEMALLOC}")
else()
        find_library(JEMALLOC_LIB NAMES jemalloc)
        if (JEMALLOC_LIB)
                target_link_libraries(rundb PRIVATE ${JEMALLOC_LIB})
                target_compile_definitions(rundb PRIVATE USE_JEMALLOC=1)
                message(STATUS "Using system jemalloc: ${JEMALLOC_LIB}")
        else()
                message(WARNING "jemalloc not found, fallback to default allocator.")
        endif()
endif()
