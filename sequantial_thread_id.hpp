#ifndef SEQUANTIAL_THREAD_ID_HPP_FT1G7I2N
#define SEQUANTIAL_THREAD_ID_HPP_FT1G7I2N

#include <atomic>
#include <thread>

class SequantialThreadId {
public:
    static unsigned get() {
        auto rv = current_id_;
        if(__builtin_expect(rv == 0, false)){
            rv = current_id_ = ++prev_id_;
        }
        return rv;
    }

private:
    static std::atomic<unsigned> prev_id_;
    static thread_local unsigned current_id_;
};

std::atomic<unsigned> SequantialThreadId::prev_id_(0);
thread_local unsigned SequantialThreadId::current_id_(0);

#endif /* end of include guard: SEQUANTIAL_THREAD_ID_HPP_FT1G7I2N */
