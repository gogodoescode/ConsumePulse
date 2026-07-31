cmake_minimum_required(VERSION 3.20)
project(consumepulse_consumer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(RdKafka CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)

add_executable(consumer src/main.cpp)
target_link_libraries(consumer PRIVATE RdKafka::rdkafka++ nlohmann_json::nlohmann_json libpqxx::pqxx)
