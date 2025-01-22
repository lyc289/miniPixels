cmake_minimum_required(VERSION 3.0)
project(pixels-cli)

set(CMAKE_CXX_STANDARD 17)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

if(POLICY CMP0057)
    cmake_policy(SET CMP0057 NEW)
endif()

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

set(boost_regex_DIR "/home/liuyichen/boost_1_87_0/stage/lib/cmake/boost_regex-1.87.0")
set(boost_program_options_DIR "/home/liuyichen/boost_1_87_0/stage/lib/cmake/boost_program_options-1.87.0")
set(Boost_DIR "/home/liuyichen/boost_1_87_0/tools/cmake/config")
set(boost_headers_DIR "/home/liuyichen/boost_1_87_0/bin.v2/libs/headers/build/stage")
set(BOOST_INCLUDE_DIR "/home/liuyichen/boost_1_87_0/boost")
set(Boost_LIBRARY_DIR "/home/liuyichen/boost_1_87_0/stage/lib")

include_directories(${Boost_INCLUDE_DIR})
link_directories(${Boost_LIBRARY_DIR})
find_package(Boost REQUIRED COMPONENTS program_options regex)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message(FATAL_ERROR "Could not find Boost")
endif()

set(pixels_cli_cxx
        main.cpp
        lib/executor/LoadExecutor.cpp
        lib/load/Parameters.cpp
        lib/load/PixelsConsumer.cpp)
add_executable(pixels-cli ${pixels_cli_cxx})
include_directories(include)
include_directories(../pixels-core/include)
include_directories(../pixels-common/include)
include_directories(/home/liuyichen/boost_1_87_0/boost)
include_directories(/home/liuyichen/boost_1_87_0)
target_link_libraries(pixels-cli ${Boost_LIBRARY_DIR}/libboost_program_options.so ${Boost_LIBRARY_DIR}/libboost_regex.so pixels-core pixels-common duckdb)