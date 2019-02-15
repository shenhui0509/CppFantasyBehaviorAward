#include "MPMCQueue.hpp"

#include <vector>
#include <thread>
#include <atomic>
#include <iostream>
#include <cassert>

int main() {
    const uint64_t num_ops = 10000;
    const int num_threads = 10;
    lock_free::MPMCQueue<int> q(num_threads);
    std::atomic<bool> flag{false};
    std::vector<std::thread> threads;
    std::atomic<uint64_t> sum{0};

    for (uint64_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
                             while (!flag) {
                             }
                             for (auto j = i; j < num_ops; j += num_threads) {
                                q.push(j);
                             }
                             });
    }

    for (uint64_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
                             while (!flag) {}
                             uint64_t thread_sum = 0;
                             for (uint64_t j = i; j < num_ops; j += num_threads) {
                                int v;
                                q.pop(v);
                                thread_sum += v;
                             }
                             sum += thread_sum;
                             });
    }
    flag = true;
    for(auto& t : threads) {
        t.join();
    }
    assert(sum = num_ops * (num_ops + 1) / 2);
}
