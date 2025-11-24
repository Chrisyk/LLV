#pragma once

#include <unordered_map>
#include <list>
#include <mutex>
#include <string>
#include <memory>
#include <condition_variable>
#include <vector>
#include "../core/record.h"

class LockManager2PL {
public:
    void acquire(const std::string& key, LockMode mode);
    void release(const std::string& key, LockMode mode);

    void acquire_all_atomically(const std::vector<std::string>& reads,
                                const std::vector<std::string>& writes);
    void release_all(const std::vector<std::string>& reads,
                     const std::vector<std::string>& writes);

private:
    std::unordered_map<std::string, LockHead> locks_;
    std::mutex map_mtx_;

    std::mutex global_mtx_;
    std::condition_variable global_cv_;

    LockHead& get_lock_head(const std::string& key);
    bool can_grant(const LockHead& head, const std::shared_ptr<LockRequest>& req);
};