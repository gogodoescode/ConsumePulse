#pragma once

#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

// std::cout::operator<< isn't atomic across threads — two workers logging
// at once interleave mid-line. Build the line first, then write it in one
// locked call.
inline std::mutex& logMutex() {
    static std::mutex mutex;
    return mutex;
}

inline void logLine(std::ostream& os, const std::string& line) {
    std::lock_guard<std::mutex> lock(logMutex());
    os << line << "\n";
}
