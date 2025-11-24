#include <atomic>
#include <chrono>
#include <condition_variable>
#include <algorithm>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <unordered_set>
#include <ctime>

#include "../src/core/vll_stman.h"
#include "../src/concurrency/vll.h"
#include "../src/concurrency/lock_manager_2pl.h"
#include "../src/transaction/transaction.h"

using namespace std::chrono_literals;

struct BenchConfig {
    int num_threads = 1;
    int duration_seconds = 5;

    int hot_keys = 100;
    int key_space = 1000000;
    double hot_ratio = 0.1;
    double write_ratio = 0.2;
    int reads_per_tx = 0;
    int writes_per_tx = 10;
    int work_us = 160;
};

static std::string key_name(int64_t idx) { return "k" + std::to_string(idx); }

struct TxSets { std::vector<std::string> reads; std::vector<std::string> writes; };

template <class URNG>
static TxSets gen_tx_sets(const BenchConfig& cfg, URNG& rng) {
    TxSets out;

    out.reads.reserve(cfg.reads_per_tx);
    out.writes.reserve(cfg.writes_per_tx);

    if (cfg.hot_keys > 0) {

        std::uniform_int_distribution<int> hot_dist(0, std::max(0, cfg.hot_keys - 1));
        int64_t hot_k = hot_dist(rng);
        out.writes.push_back(key_name(hot_k));

        int64_t cold_begin = static_cast<int64_t>(cfg.hot_keys);
        int64_t cold_end = std::max<int64_t>(cold_begin + 1, static_cast<int64_t>(cfg.key_space) - 1);
        std::uniform_int_distribution<int64_t> cold_dist(cold_begin, cold_end);

        for (int i = 1; i < cfg.writes_per_tx; ++i) {
            out.writes.push_back(key_name(cold_dist(rng)));
        }
        for (int i = 0; i < cfg.reads_per_tx; ++i) {
            out.reads.push_back(key_name(cold_dist(rng)));
        }
    } else {

        std::uniform_real_distribution<double> unif(0.0, 1.0);
        int hot_limit = static_cast<int>(cfg.key_space * cfg.hot_ratio);
        hot_limit = std::max(hot_limit, 0);
        std::uniform_int_distribution<int> hot_idx(0, std::max(0, hot_limit - 1));
        std::uniform_int_distribution<int> cold_idx(std::max(0, hot_limit), std::max(0, cfg.key_space - 1));
        for (int i = 0; i < cfg.reads_per_tx; ++i) {
            int idx = (unif(rng) < cfg.hot_ratio && hot_limit > 0) ? hot_idx(rng) : cold_idx(rng);
            out.reads.push_back(key_name(idx));
        }
        for (int i = 0; i < cfg.writes_per_tx; ++i) {
            int idx = (unif(rng) < cfg.hot_ratio && hot_limit > 0) ? hot_idx(rng) : cold_idx(rng);
            out.writes.push_back(key_name(idx));
        }
    }

    auto dedup = [](std::vector<std::string>& v){
        std::sort(v.begin(), v.end());
        v.erase(std::unique(v.begin(), v.end()), v.end());
    };
    dedup(out.reads);
    dedup(out.writes);
    {
        std::vector<std::string> only_reads;
        only_reads.reserve(out.reads.size());
        std::unordered_set<std::string> wset(out.writes.begin(), out.writes.end());
        for (auto &k : out.reads) if (wset.find(k) == wset.end()) only_reads.push_back(k);
        out.reads.swap(only_reads);
    }
    return out;
}

long run_2pl(const BenchConfig& cfg) {
    LockManager2PL lm;
    std::atomic<long> committed{0};
    std::atomic<bool> stop{false};

    std::vector<std::atomic<long>> per_thread_committed(cfg.num_threads);
    for (auto &c : per_thread_committed) c.store(0);

    auto worker = [&](int id){
        std::mt19937_64 rng(id + 123);

        while (!stop.load()) {

            auto sets = gen_tx_sets(cfg, rng);
            auto& reads = sets.reads;
            auto& writes = sets.writes;

            lm.acquire_all_atomically(reads, writes);

            std::this_thread::sleep_for(std::chrono::microseconds(cfg.work_us));

            lm.release_all(reads, writes);

            committed.fetch_add(1, std::memory_order_relaxed);
            per_thread_committed[id].fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) threads.emplace_back(worker, i);

    std::clock_t cpu_start = std::clock();

    std::thread monitor([&]{
        for (int s = 0; s < cfg.duration_seconds && !stop.load(); ++s) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            long total = committed.load();
            std::cout << "[2PL] elapsed=" << (s+1) << "s, committed=" << total << '\n';
            for (int i = 0; i < cfg.num_threads; ++i) {
                std::cout << "  t" << i << ": " << per_thread_committed[i].load() << '\n';
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_seconds));
    stop.store(true);
    for (auto &t : threads) t.join();
    monitor.join();

    std::clock_t cpu_end = std::clock();
    double cpu_seconds = double(cpu_end - cpu_start) / double(CLOCKS_PER_SEC);
    long committed_count = committed.load();
    if (committed_count > 0) {
        double ns_per_tx = (cpu_seconds / double(committed_count)) * 1e9;
        std::cout << "[2PL] CPU time=" << cpu_seconds << "s, per-tx=" << ns_per_tx << " ns\n";
    }

    return committed_count;
}

long run_vll(const BenchConfig& cfg) {
    storageManager store;
    ConcVLL::TxnQueue q;
    std::atomic<long> committed{0};
    std::atomic<bool> stop{false};

    for (int64_t i = 0; i < cfg.key_space; ++i) {
        store.insert(key_name(i), std::string());
    }

    auto wall_start = std::chrono::steady_clock::now();
    auto wall_end   = wall_start + std::chrono::seconds(cfg.duration_seconds);

    std::deque<ConcVLL::txn_ptr> reqs;
    std::mutex req_m;
    std::condition_variable req_cv;

    auto getNew = [&]() -> ConcVLL::txn_ptr {
        std::unique_lock<std::mutex> lk(req_m);

        if (std::chrono::steady_clock::now() > wall_end) return nullptr;
        req_cv.wait_for(lk, 50ms, [&]{ return !reqs.empty() || stop.load(); });
        if (reqs.empty()) return nullptr;
        auto t = reqs.front(); reqs.pop_front();
        return t;
    };

    auto exec = [&](ConcVLL::txn_ptr t){
        std::this_thread::sleep_for(std::chrono::microseconds(cfg.work_us));
        committed.fetch_add(1, std::memory_order_relaxed);
    };

    std::vector<std::thread> vll_threads;
    vll_threads.reserve(cfg.num_threads);
    for (int i = 0; i < cfg.num_threads; ++i) {
        vll_threads.emplace_back([&]{ q.VLLMainLoop(store, exec, getNew, [&]{ return stop.load(); }, 10000); });
    }

    auto worker = [&](int id){
        std::mt19937_64 rng(id + 456);

        while (!stop.load()) {
            auto tx = std::make_shared<ConcVLL::Transaction>(0);

            auto sets = gen_tx_sets(cfg, rng);
            tx->ReadSet = std::move(sets.reads);
            tx->WriteSet = std::move(sets.writes);

            {
                std::lock_guard<std::mutex> lg(req_m);
                reqs.push_back(tx);
            }
            req_cv.notify_one();

        }
    };

    std::vector<std::thread> producers;
    for (int i = 0; i < cfg.num_threads; ++i) producers.emplace_back(worker, i);

    std::clock_t cpu_start = std::clock();

    std::thread monitor([&]{
        for (int s = 0; s < cfg.duration_seconds && !stop.load(); ++s) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            long total = committed.load();
            std::cout << "[VLL] elapsed=" << (s+1) << "s, committed=" << total << ", queue=" << q.activeCount() << '\n';
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_seconds));
    stop.store(true);
    req_cv.notify_all();

    {
        std::lock_guard<std::mutex> lg(req_m);
        reqs.clear();
    }
    q.CancelAll(store);
    for (auto &p : producers) p.join();

    std::this_thread::sleep_for(200ms);

    monitor.join();
    for (auto &t : vll_threads) if (t.joinable()) t.join();

    std::clock_t cpu_end = std::clock();
    double cpu_seconds = double(cpu_end - cpu_start) / double(CLOCKS_PER_SEC);
    long committed_count = committed.load();
    if (committed_count > 0) {
        double ns_per_tx = (cpu_seconds / double(committed_count)) * 1e9;
        std::cout << "[VLL] CPU time=" << cpu_seconds << "s, per-tx=" << ns_per_tx << " ns\n";
    }

    return committed_count;
}

int main(int argc, char** argv) {
    BenchConfig cfg;
    std::cout << "Running microbenchmark: num_threads=" << cfg.num_threads << " duration=" << cfg.duration_seconds << "s\n";
    if (cfg.hot_keys > 0) {
        double contention_index = 1.0 / static_cast<double>(cfg.hot_keys);
        std::cout << "Contention index (1/H): H=" << cfg.hot_keys << ", CI=" << contention_index << "\n";
    } else {
        std::cout << "Contention index: N/A (legacy hot_ratio mode)\n";
    }

    std::cout << "Running 2PL...\n";
    auto c2 = run_2pl(cfg);
    std::cout << "2PL committed txns: " << c2 << " (" << (c2 / cfg.duration_seconds) << " tps)\n";

    std::cout << "Running VLL...\n";
    auto cv = run_vll(cfg);
    std::cout << "VLL committed txns: " << cv << " (" << (cv / cfg.duration_seconds) << " tps)\n";

    return 0;
}

