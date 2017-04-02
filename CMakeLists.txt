cmake_minimum_required(VERSION 3.6)
project(hash_grouping_update)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp KVBuffer.h KVBuffer.cpp murmurhash.h MyHash.cpp MyHash.h timer.h)
add_executable(hash_grouping_update ${SOURCE_FILES})