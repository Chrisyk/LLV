#include "lock_manager_2pl.h"
#include <vector>
#include <thread>

void LockManager2PL::acquire(const std::string& key, LockMode mode) {
    auto& head = get_lock_head(key);
    std::unique_lock<std::mutex> lk(head.mtx);

    auto req = std::make_shared<LockRequest>();
    req->mode = mode;
    req->owner = std::this_thread::get_id();

    head.queue.push_back(req);

    while (!can_grant(head, req)) {
        req->cv.wait(lk);
    }
    req->granted = true;

    if (mode == LockMode::Shared) {
        head.shared_count++;
        head.current_mode = LockMode::Shared;
    } else {
        head.exclusive = true;
        head.current_mode = LockMode::Exclusive;
    }
}

void LockManager2PL::release(const std::string& key, LockMode mode) {
    auto& head = get_lock_head(key);
    std::unique_lock<std::mutex> lk(head.mtx);

    for (auto it = head.queue.begin(); it != head.queue.end(); ++it) {
        auto& req = *it;
        if (req->owner == std::this_thread::get_id() && req->mode == mode && req->granted) {
            if (mode == LockMode::Shared) {
                head.shared_count--;
            } else {
                head.exclusive = false;
            }
            head.queue.erase(it);
            break;
        }
    }

    for (auto& next : head.queue) {
        if (!next->granted && can_grant(head, next)) {
            next->cv.notify_one();
        }
    }
}

LockHead& LockManager2PL::get_lock_head(const std::string& key) {
    std::lock_guard<std::mutex> lg(map_mtx_);
    return locks_[key];
}

bool LockManager2PL::can_grant(const LockHead& head, const std::shared_ptr<LockRequest>& req) {
    if (req->mode == LockMode::Shared) {

        for (const auto& r : head.queue) {
            if (r == req) break;
            if (r->mode == LockMode::Exclusive && !r->granted) return false;
        }
        return !head.exclusive;
    } else {

        return head.queue.front() == req && head.shared_count == 0 && !head.exclusive;
    }
}

void LockManager2PL::acquire_all_atomically(const std::vector<std::string>& reads,
                                            const std::vector<std::string>& writes) {
    std::unique_lock<std::mutex> lk(global_mtx_);
    while (true) {
        bool ok = true;
        {

            std::lock_guard<std::mutex> map_lk(map_mtx_);
            for (const auto& k : writes) (void)locks_[k];
            for (const auto& k : reads)  (void)locks_[k];
        }

        {
            std::lock_guard<std::mutex> map_lk(map_mtx_);
            for (const auto& k : writes) {
                const auto it = locks_.find(k);
                const auto& h = it->second;
                if (h.exclusive || h.shared_count > 0) { ok = false; break; }
            }
            if (ok) {
                for (const auto& k : reads) {
                    const auto it = locks_.find(k);
                    const auto& h = it->second;
                    if (h.exclusive) { ok = false; break; }
                }
            }
        }

        if (ok) {

            std::lock_guard<std::mutex> map_lk(map_mtx_);
            for (const auto& k : writes) {
                auto& h = locks_[k];
                h.exclusive = true;
                h.current_mode = LockMode::Exclusive;
            }
            for (const auto& k : reads) {
                auto& h = locks_[k];
                h.shared_count++;
                h.current_mode = LockMode::Shared;
            }
            return;
        }

        global_cv_.wait(lk);
    }
}

void LockManager2PL::release_all(const std::vector<std::string>& reads,
                                 const std::vector<std::string>& writes) {
    {
        std::lock_guard<std::mutex> lk(global_mtx_);
        std::lock_guard<std::mutex> map_lk(map_mtx_);
        for (const auto& k : writes) {
            auto it = locks_.find(k);
            if (it != locks_.end()) {
                it->second.exclusive = false;
                it->second.current_mode = LockMode::Shared;
            }
        }
        for (const auto& k : reads) {
            auto it = locks_.find(k);
            if (it != locks_.end()) {
                if (it->second.shared_count > 0) it->second.shared_count--;
            }
        }
    }
    global_cv_.notify_all();
}