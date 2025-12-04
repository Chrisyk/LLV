#include <atomic>
#include <chrono>
#include <condition_variable>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <iomanip>
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

    int hot_keys = 100;         // Number of hot keys (Contention Index = 1 / hot_keys)
    int key_space = 1000000;    // Total number of keys in the database
    int reads_per_tx = 0;       // Number of reads per transaction
    int writes_per_tx = 10;      // Number of writes per transaction
    int work_us = 160;          // How long in microseconds each transaction "works"
    bool use_sca = true;        // Enable Selective Contention Analysis (per VLL paper Section 2.5)
    bool sweep = false;         // Run contention sweep for graphing
    std::string output_prefix = "benchmark_results";  // Output file prefix for sweep mode
    bool quiet = false;         // Suppress per-second output
};

static std::string key_name(int64_t idx) { return "k" + std::to_string(idx); }

struct TxSets { std::vector<std::string> reads; std::vector<std::string> writes; };

template <class URNG>
static TxSets gen_tx_sets(const BenchConfig& cfg, URNG& rng) {
    TxSets out;

    out.reads.reserve(cfg.reads_per_tx);
    out.writes.reserve(cfg.writes_per_tx);

    if (cfg.hot_keys > 0) {

        std::uniform_int_distribution<int> hot_dist(0, cfg.hot_keys - 1);
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
        std::uniform_int_distribution<int64_t> full_dist(0, std::max(0, cfg.key_space - 1));
        for (int i = 0; i < cfg.writes_per_tx; ++i) out.writes.push_back(key_name(full_dist(rng)));
        for (int i = 0; i < cfg.reads_per_tx; ++i) out.reads.push_back(key_name(full_dist(rng)));
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
            if (!cfg.quiet) {
                long total = committed.load();
                std::cout << "[2PL] elapsed=" << (s+1) << "s, committed=" << total << '\n';
                for (int i = 0; i < cfg.num_threads; ++i) {
                    std::cout << "  t" << i << ": " << per_thread_committed[i].load() << '\n';
                }
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
    if (committed_count > 0 && !cfg.quiet) {
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
        vll_threads.emplace_back([&]{ q.VLLMainLoop(store, exec, getNew, [&]{ return stop.load(); }, 10000, cfg.use_sca); });
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

    const char* vll_label = cfg.use_sca ? "[VLL+SCA]" : "[VLL]";
    std::thread monitor([&, vll_label]{
        for (int s = 0; s < cfg.duration_seconds && !stop.load(); ++s) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            if (!cfg.quiet) {
                long total = committed.load();
                std::cout << vll_label << " elapsed=" << (s+1) << "s, committed=" << total << ", queue=" << q.activeCount() << '\n';
            }
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
    if (committed_count > 0 && !cfg.quiet) {
        double ns_per_tx = (cpu_seconds / double(committed_count)) * 1e9;
        std::cout << vll_label << " CPU time=" << cpu_seconds << "s, per-tx=" << ns_per_tx << " ns\n";
    }

    return committed_count;
}

void run_sweep(BenchConfig& cfg) {
    std::vector<int> hot_keys_values = {
        10000, 5000, 2000, 1000, 500, 200, 100, 50, 20, 10, 5
    };

    std::string csv_2pl = cfg.output_prefix + "_2pl.csv";
    std::string csv_vll = cfg.output_prefix + "_vll.csv";
    std::string csv_vll_sca = cfg.output_prefix + "_vll_sca.csv";

    std::ofstream f_2pl(csv_2pl);
    std::ofstream f_vll(csv_vll);
    std::ofstream f_vll_sca(csv_vll_sca);

    f_2pl << "hot_keys,contention_index,throughput_tps,total_txns\n";
    f_vll << "hot_keys,contention_index,throughput_tps,total_txns\n";
    f_vll_sca << "hot_keys,contention_index,throughput_tps,total_txns\n";

    cfg.quiet = true;

    int total_runs = hot_keys_values.size() * 3;
    int current_run = 0;

    std::cout << "\n========================================\n";
    std::cout << "VLL Benchmark Contention Sweep\n";
    std::cout << "========================================\n";
    std::cout << "Threads: " << cfg.num_threads << "\n";
    std::cout << "Duration per test: " << cfg.duration_seconds << "s\n";
    std::cout << "Contention levels: " << hot_keys_values.size() << "\n";
    std::cout << "Total estimated time: ~" << (total_runs * cfg.duration_seconds) / 60 << " minutes\n";
    std::cout << "========================================\n\n";

    for (int hot_keys : hot_keys_values) {
        cfg.hot_keys = hot_keys;
        double ci = 1.0 / static_cast<double>(hot_keys);

        std::cout << std::fixed << std::setprecision(4);
        std::cout << "[" << ++current_run << "/" << total_runs << "] ";
        std::cout << "hot_keys=" << hot_keys << " (CI=" << ci << ") - 2PL... " << std::flush;

        long txns = run_2pl(cfg);
        double tps = static_cast<double>(txns) / cfg.duration_seconds;
        f_2pl << hot_keys << "," << ci << "," << tps << "," << txns << "\n";
        std::cout << tps << " tps\n";

        std::cout << "[" << ++current_run << "/" << total_runs << "] ";
        std::cout << "hot_keys=" << hot_keys << " (CI=" << ci << ") - VLL... " << std::flush;

        cfg.use_sca = false;
        txns = run_vll(cfg);
        tps = static_cast<double>(txns) / cfg.duration_seconds;
        f_vll << hot_keys << "," << ci << "," << tps << "," << txns << "\n";
        std::cout << tps << " tps\n";

        std::cout << "[" << ++current_run << "/" << total_runs << "] ";
        std::cout << "hot_keys=" << hot_keys << " (CI=" << ci << ") - VLL+SCA... " << std::flush;

        cfg.use_sca = true;
        txns = run_vll(cfg);
        tps = static_cast<double>(txns) / cfg.duration_seconds;
        f_vll_sca << hot_keys << "," << ci << "," << tps << "," << txns << "\n";
        std::cout << tps << " tps\n";

        std::cout << "\n";
    }

    f_2pl.close();
    f_vll.close();
    f_vll_sca.close();

    std::cout << "========================================\n";
    std::cout << "Sweep complete!\n";
    std::cout << "========================================\n";
    std::cout << "Output files:\n";
    std::cout << "  " << csv_2pl << "\n";
    std::cout << "  " << csv_vll << "\n";
    std::cout << "  " << csv_vll_sca << "\n";

    std::cout << "\nTo generate plots, run:\n";
    std::cout << "  python3 scripts/plot_results.py " << cfg.output_prefix << "\n";
}

int main(int argc, char** argv) {
    BenchConfig cfg;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--", 0) == 0) {
            std::string key = arg.substr(2);
            std::string val;
            size_t eq_pos = key.find('=');
            if (eq_pos != std::string::npos) {
                val = key.substr(eq_pos + 1);
                key = key.substr(0, eq_pos);
            }

            if (key == "num_threads") {
                cfg.num_threads = std::stoi(val);
            } else if (key == "duration_seconds") {
                cfg.duration_seconds = std::stoi(val);
            } else if (key == "hot_keys") {
                cfg.hot_keys = std::stoi(val);
            } else if (key == "key_space") {
                cfg.key_space = std::stoi(val);
            } else if (key == "reads_per_tx") {
                cfg.reads_per_tx = std::stoi(val);
            } else if (key == "writes_per_tx") {
                cfg.writes_per_tx = std::stoi(val);
            } else if (key == "work_us") {
                cfg.work_us = std::stoi(val);
            } else if (key == "use_sca") {
                cfg.use_sca = (val == "1" || val == "true" || val == "yes");
            } else if (key == "sweep") {
                cfg.sweep = (val.empty() || val == "1" || val == "true" || val == "yes");
            } else if (key == "output_prefix") {
                cfg.output_prefix = val;
            } else if (key == "quiet") {
                cfg.quiet = (val.empty() || val == "1" || val == "true" || val == "yes");
            } else if (key == "help") {
                std::cout << "VLL Microbenchmark\n\n";
                std::cout << "Usage: " << argv[0] << " [options]\n\n";
                std::cout << "Options:\n";
                std::cout << "  --num_threads=N        Number of worker threads (default: 1)\n";
                std::cout << "  --duration_seconds=N   Duration per benchmark (default: 5)\n";
                std::cout << "  --hot_keys=N           Number of hot keys (default: 100)\n";
                std::cout << "  --key_space=N          Total key space size (default: 1000000)\n";
                std::cout << "  --reads_per_tx=N       Reads per transaction (default: 0)\n";
                std::cout << "  --writes_per_tx=N      Writes per transaction (default: 10)\n";
                std::cout << "  --work_us=N            Simulated work microseconds (default: 160)\n";
                std::cout << "  --use_sca=BOOL         Enable SCA for VLL (default: true)\n";
                std::cout << "  --sweep                Run contention sweep and generate graphs\n";
                std::cout << "  --output_prefix=STR    Output file prefix for sweep (default: benchmark_results)\n";
                std::cout << "  --quiet                Suppress per-second output\n";
                std::cout << "  --help                 Show this help message\n";
                return 0;
            } else {
                std::cerr << "Unknown option: " << key << std::endl;
                std::cerr << "Use --help for usage information\n";
                return 1;
            }
        }
    }

    if (cfg.sweep) {
        run_sweep(cfg);
        return 0;
    }
    // Single run benchmark
    std::cout << "Running microbenchmark: num_threads=" << cfg.num_threads
              << " duration=" << cfg.duration_seconds << "s"
              << " hot_keys=" << cfg.hot_keys
              << " key_space=" << cfg.key_space
              << " reads_per_tx=" << cfg.reads_per_tx
              << " writes_per_tx=" << cfg.writes_per_tx
              << " work_us=" << cfg.work_us
              << " use_sca=" << (cfg.use_sca ? "true" : "false")
              << std::endl;

    if (cfg.hot_keys > 0) {
        double contention_index = 1.0 / static_cast<double>(cfg.hot_keys);
        std::cout << "Contention index (1/H): H=" << cfg.hot_keys << ", CI=" << contention_index << "\n";
    } else {
        std::cout << "Contention index: N/A (legacy hot_ratio mode)\n";
    }

    std::cout << "Running 2PL...\n";
    auto c2 = run_2pl(cfg);
    std::cout << "2PL committed txns: " << c2 << " (" << (c2 / cfg.duration_seconds) << " tps)\n";

    std::cout << "Running VLL" << (cfg.use_sca ? " with SCA" : " without SCA") << "...\n";
    auto cv = run_vll(cfg);
    std::cout << "VLL" << (cfg.use_sca ? "+SCA" : "") << " committed txns: " << cv << " (" << (cv / cfg.duration_seconds) << " tps)\n";

    return 0;
}

