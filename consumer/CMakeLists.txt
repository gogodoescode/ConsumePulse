cmake_minimum_required(VERSION 3.20)
project(consumepulse_consumer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(RdKafka CONFIG REQUIRED)

add_executable(consumer src/main.cpp)
target_link_libraries(consumer PRIVATE RdKafka::rdkafka++)
