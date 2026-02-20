/*
==============================================================
   DISTRIBUTED TASK EXECUTION ENGINE v4.2
   - Thread Pool
   - Async Futures
   - Task Scheduler
   - Metrics Collector
   - Logging System
==============================================================
*/

#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <future>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <map>
#include <chrono>
#include <sstream>

using namespace std;

// ============================================================
// Logger
// ============================================================

class Logger {
private:
    mutex logMutex;

public:
    void info(const string& message) {
        lock_guard<mutex> lock(logMutex);
        cout << "[INFO] " << message << endl;
    }

    void warn(const string& message) {
        lock_guard<mutex> lock(logMutex);
        cout << "[WARN] " << message << endl;
    }

    void error(const string& message) {
        lock_guard<mutex> lock(logMutex);
        cout << "[ERROR] " << message << endl;
    }
};

Logger logger;


class Metrics {
private:
    atomic<int> tasksCompleted;
    atomic<int> tasksQueued;
    atomic<long long> totalExecutionTime;

public:
    Metrics() : tasksCompleted(0), tasksQueued(0), totalExecutionTime(0) {}

    void recordTaskQueued() {
        tasksQueued++;
    }

    void recordTaskCompleted(long long duration) {
        tasksCompleted++;
        totalExecutionTime += duration;
    }

    void print() {
        cout << "\n========= METRICS =========" << endl;
        cout << "Tasks Queued     : " << tasksQueued.load() << endl;
        cout << "Tasks Completed  : " << tasksCompleted.load() << endl;
        cout << "Avg Exec Time(ms): "
             << (tasksCompleted > 0 ? totalExecutionTime / tasksCompleted : 0)
             << endl;
    }
};

Metrics metrics;


class ThreadPool {
private:
    vector<thread> workers;
    queue<function<void()>> tasks;

    mutex queueMutex;
    condition_variable condition;
    bool stop;

public:
    ThreadPool(size_t threads) : stop(false) {
        for(size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                while(true) {
                    function<void()> task;

                    {
                        unique_lock<mutex> lock(this->queueMutex);
                        this->condition.wait(lock, [this] {
                            return this->stop || !this->tasks.empty();
                        });

                        if(this->stop && this->tasks.empty())
                            return;

                        task = move(this->tasks.front());
                        this->tasks.pop();
                    }

                    auto start = chrono::high_resolution_clock::now();
                    task();
                    auto end = chrono::high_resolution_clock::now();

                    long long duration =
                        chrono::duration_cast<chrono::milliseconds>(end - start).count();

                    metrics.recordTaskCompleted(duration);
                }
            });
        }
    }

    template<class F>
    auto enqueue(F&& f) -> future<typename result_of<F()>::type> {
        using return_type = typename result_of<F()>::type;

        auto task = make_shared<packaged_task<return_type()>>(forward<F>(f));
        future<return_type> res = task->get_future();

        {
            unique_lock<mutex> lock(queueMutex);
            if(stop)
                throw runtime_error("enqueue on stopped ThreadPool");

            tasks.emplace([task]() { (*task)(); });
            metrics.recordTaskQueued();
        }

        condition.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            unique_lock<mutex> lock(queueMutex);
            stop = true;
        }

        condition.notify_all();

        for(thread &worker : workers)
            worker.join();
    }
};



int heavyComputation(int input) {
    this_thread::sleep_for(chrono::milliseconds(100 + rand() % 400));

    int result = 0;
    for(int i = 0; i < 100000; ++i)
        result += (input * i) % 97;

    return result;
}

class Scheduler {
private:
    ThreadPool pool;

public:
    Scheduler(int threads) : pool(threads) {}

    void runBatch(int batchSize) {
        vector<future<int>> results;

        for(int i = 0; i < batchSize; ++i) {
            results.emplace_back(
                pool.enqueue([i] {
                    return heavyComputation(i);
                })
            );
        }

        for(auto &result : results) {
            logger.info("Task result: " + to_string(result.get()));
        }
    }
};


int main() {
    srand(time(nullptr));

    logger.info("Starting Distributed Execution Engine...");
    logger.info("Initializing Scheduler...");

    Scheduler scheduler(4);

    logger.info("Running Task Batch...");
    scheduler.runBatch(10);

    metrics.print();

    logger.info("Shutting down engine.");
    return 0;
}
