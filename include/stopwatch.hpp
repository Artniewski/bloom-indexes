#ifndef STOPWATCH_HPP
#define STOPWATCH_HPP

#include <chrono>

class StopWatch {
public:
    StopWatch() = default;

    /**
     * @brief Starts (or restarts) the timer.
     */
    void start() {
        start_time_ = std::chrono::steady_clock::now();
    }

    /**
     * @brief Stops the timer.
     */
    void stop() {
        end_time_ = std::chrono::steady_clock::now();
    }

    /**
     * @brief Returns the elapsed time in microseconds.
     */
    long long elapsedMicros() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(end_time_ - start_time_).count();
    }

private:
    std::chrono::steady_clock::time_point start_time_{};
    std::chrono::steady_clock::time_point end_time_{};
};

#endif // STOPWATCH_HPP
