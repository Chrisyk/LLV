#ifndef DATA_H
#define DATA_H

#include <string>
#include <atomic>
#include <mutex>
#include <list>
#include <condition_variable>
#include <thread>
struct tuple{
    std::atomic<int> Cx;
    std::atomic<int> Cs;
    std::string value;

    tuple(const std::string& val) : Cx(0), Cs(0), value(val) {}
};

enum class LockMode { Shared, Exclusive };

struct LockRequest {
    std::condition_variable cv;
    LockMode mode;
    bool granted = false;
    std::thread::id owner;
};

struct LockHead {
    std::mutex mtx;
    LockMode current_mode = LockMode::Shared;
    int shared_count = 0;
    bool exclusive = false;
    std::list<std::shared_ptr<LockRequest>> queue;
};

#endif

