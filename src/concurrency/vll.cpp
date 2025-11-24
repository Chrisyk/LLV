#include "vll.h"
#include "../transaction/transaction.h"

#include <algorithm>
#include <thread>
#include <chrono>
#include <functional>

namespace ConcVLL {

TxnQueue::TxnQueue() = default;

txn_ptr TxnQueue::beginTransaction() {
    auto id = nextId_.fetch_add(1, std::memory_order_relaxed);
    auto txn = std::make_shared<ConcVLL::Transaction>(id);
    {

        std::lock_guard<std::mutex> lg(mtx_);

        queue_.push_back(txn);
    }
    return txn;
}

void TxnQueue::BeginTransaction(const txn_ptr& T, storageManager& store) {
    if (!T) return;

    if (T->id == 0) {
        T->id = nextId_.fetch_add(1, std::memory_order_relaxed);
    }

    T->type = decltype(T->type)::Free;

    for (const auto &key : T->ReadSet) {
        tuple* t = store.get(key);
        if (!t) {

            store.insert(key, std::string());
            t = store.get(key);
        }

        t->Cs.fetch_add(1, std::memory_order_relaxed);
        if (t->Cx.load(std::memory_order_relaxed) > 0) {
            T->type = decltype(T->type)::Blocked;
        }
    }

    for (const auto &key : T->WriteSet) {
        tuple* t = store.get(key);
        if (!t) {
            store.insert(key, std::string());
            t = store.get(key);
        }
        t->Cx.fetch_add(1, std::memory_order_relaxed);
        if (t->Cx.load(std::memory_order_relaxed) > 1 || t->Cs.load(std::memory_order_relaxed) > 0) {
            T->type = decltype(T->type)::Blocked;
        }
    }

    {
        std::lock_guard<std::mutex> lg(mtx_);
        queue_.push_back(T);
    }
}

void TxnQueue::FinishTransaction(const txn_ptr& T, ::storageManager& store) {
    if (!T) return;

    for (const auto &key : T->ReadSet) {
        tuple* t = store.get(key);
        if (t) t->Cs.fetch_sub(1, std::memory_order_relaxed);
    }

    for (const auto &key : T->WriteSet) {
        tuple* t = store.get(key);
        if (t) t->Cx.fetch_sub(1, std::memory_order_relaxed);
    }

    {
        std::lock_guard<std::mutex> lg(mtx_);
        auto it = std::find_if(queue_.begin(), queue_.end(), [&](const txn_ptr& x){ return x->id == T->id; });
        if (it != queue_.end()) queue_.erase(it);
    }
}

void TxnQueue::finishTransaction(const txn_ptr& txn) {
    if (!txn) return;
    std::lock_guard<std::mutex> lg(mtx_);
    auto it = std::find_if(queue_.begin(), queue_.end(), [&](const txn_ptr& t){ return t->id == txn->id; });
    if (it != queue_.end()) queue_.erase(it);
}

std::size_t TxnQueue::activeCount() const {
    std::lock_guard<std::mutex> lg(mtx_);
    return queue_.size();
}

void TxnQueue::CancelAll(::storageManager& store) {
    std::lock_guard<std::mutex> lg(mtx_);
    for (auto &T : queue_) {
        if (!T) continue;

        for (const auto &key : T->ReadSet) {
            tuple* t = store.get(key);
            if (t) t->Cs.fetch_sub(1, std::memory_order_relaxed);
        }

        for (const auto &key : T->WriteSet) {
            tuple* t = store.get(key);
            if (t) t->Cx.fetch_sub(1, std::memory_order_relaxed);
        }
    }
    queue_.clear();
}

static inline bool intersects_sorted(const std::vector<std::string>& a,
                                     const std::vector<std::string>& b) {
    std::size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i] == b[j]) return true;
        if (a[i] < b[j]) ++i; else ++j;
    }
    return false;
}

static bool conflictsWithOlder(const std::deque<ConcVLL::txn_ptr>& q, std::size_t idx) {
    const auto &t = q[idx];
    for (std::size_t i = 0; i < idx; ++i) {
        const auto &older = q[i];

        if (intersects_sorted(t->WriteSet, older->WriteSet)) return true;
        if (intersects_sorted(t->WriteSet, older->ReadSet)) return true;
        if (intersects_sorted(t->ReadSet,  older->WriteSet)) return true;
    }
    return false;
}

void TxnQueue::VLLMainLoop(::storageManager& store,
                           std::function<void(txn_ptr)> execute,
                           std::function<txn_ptr()> getNewTxnRequest,
                           std::function<bool()> shouldStop,
                           std::size_t maxQueueSize) {
    while (true) {
        txn_ptr toRun = nullptr;

        {
            std::lock_guard<std::mutex> lg(mtx_);
            for (std::size_t i = 0; i < queue_.size(); ++i) {
                const auto &cand = queue_[i];
                if (cand->type == Transaction::Type::Blocked) {
                    if (!conflictsWithOlder(queue_, i)) {

                        cand->type = Transaction::Type::Free;
                        toRun = cand;
                        queue_.erase(queue_.begin() + static_cast<std::ptrdiff_t>(i));
                        break;
                    }
                }
            }
        }

        if (toRun) {

            execute(toRun);
            FinishTransaction(toRun, store);
            continue;
        }

        {
            std::lock_guard<std::mutex> lg(mtx_);
            if (queue_.size() < maxQueueSize) {

            } else {

                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
        }

        txn_ptr req = getNewTxnRequest();
        if (!req) {

            if (shouldStop && shouldStop()) {
                std::lock_guard<std::mutex> lg(mtx_);
                if (queue_.empty()) return;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        BeginTransaction(req, store);

        if (req->type == Transaction::Type::Free) {

            execute(req);
            FinishTransaction(req, store);
        }
    }
}

}
