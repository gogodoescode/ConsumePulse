#pragma once

#include <string>

struct Event {
    std::string event_id;
    std::string device_id;
    std::string device_type;
    std::string metric;
    double value;
    std::string unit;
    std::string event_timestamp;
};
