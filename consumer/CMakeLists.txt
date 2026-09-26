cmake_minimum_required(VERSION 3.20)
project(consumepulse_consumer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(RdKafka CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_executable(consumer src/main.cpp)
target_link_libraries(consumer PRIVATE RdKafka::rdkafka++ nlohmann_json::nlohmann_json libpqxx::pqxx
                                        Threads::Threads)

option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    find_package(Catch2 3 CONFIG REQUIRED)

    add_executable(consumer_tests
        tests/test_anomaly_strategy.cpp
        tests/test_device_factory.cpp
        tests/test_event_processor.cpp
        tests/test_offset_tracker.cpp
    )
    target_link_libraries(consumer_tests PRIVATE Catch2::Catch2WithMain nlohmann_json::nlohmann_json
                                                  libpqxx::pqxx)

    include(CTest)
    include(Catch)
    catch_discover_tests(consumer_tests)
endif()
