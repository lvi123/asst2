#include "tasksys.h"

#include <algorithm>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <functional>
#include <iomanip>
#include <sstream>
#include <thread>

void log(const char* func_name, const char* format, ...) {
    // Get current time
    auto now = std::chrono::system_clock::now();
    auto time_val = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::tm tm_buf;
    localtime_r(&time_val, &tm_buf);
    char timestamp[32];
    std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &tm_buf);
    
    va_list args;
    va_start(args, format);
    size_t thread_hash = std::hash<std::thread::id>()(std::this_thread::get_id());
    
    // Format the variable arguments into a buffer
    char message[1024];
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    
    // Single printf for everything
    printf("%s.%03lldL [%lx]: %s %s\n", 
        timestamp, static_cast<long long>(ms.count()),
        thread_hash,
        func_name,
        message);
}

#ifdef LOG_ENABLED
#define LOG(...) log(__func__, __VA_ARGS__)
#else
#define LOG(...)
#endif

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads): num_threads(num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    // printf("Running %d tasks in parallel\n", num_total_tasks);
    // printf("Using %d threads\n", num_threads);

    int num_tasks_per_thread = (num_total_tasks + num_threads - 1) / num_threads;
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.push_back(std::thread([runnable, i, num_tasks_per_thread, num_total_tasks]() {
            int start_task = i * num_tasks_per_thread;
            int end_task = std::min(start_task + num_tasks_per_thread, num_total_tasks);
            for (int j = start_task; j < end_task; j++) {
                runnable->runTask(j, num_total_tasks);
            }
        }));
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads):ITaskSystem(num_threads), 
 num_total_tasks(0), task_id_counter(0), num_tasks_completed(0), destroy_threads(false) {

        
    for (int i = 0; i < num_threads; i++) {
        threads.push_back(std::thread([this]() {
            while (true) {
                IRunnable* runnable = nullptr;
                int task_id = -1;
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (destroy_threads) {
                        break;
                    }
                    if (runnables.size() == 0) {
                        continue;
                    }
                    runnable = runnables.front();
                    runnables.pop_front();
                    task_id = task_id_counter++;
                }
                runnable->runTask(task_id, num_total_tasks);
                num_tasks_completed++;
            }
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    destroy_threads = true;
    for (auto& thread : threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    {
        std::lock_guard<std::mutex> lock(mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            runnables.push_back(runnable);
        }
        this->num_total_tasks = num_total_tasks;
        this->num_tasks_completed = 0;
        this->task_id_counter = 0;
    }

    while (num_tasks_completed < num_total_tasks) {
        std::this_thread::yield();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): 
ITaskSystem(num_threads), num_run_batch(0), num_tasks_completed(0), destroy_threads(false) {

   LOG("Creating thread pool with %d threads", num_threads);
   for (int i = 0; i < num_threads; i++) {
       threads.push_back(std::thread([this]() {
            LOG("Thread started");          
            while (true) {
                std::unique_lock<std::mutex> lock(mutex);
                cv_tasks.wait(lock, [this]() { return !tasks.empty() || destroy_threads; });
                if (destroy_threads) {
                    LOG("Thread destroying threads");
                    break;
                }
                Task task = tasks.front();
                tasks.pop_front();
                lock.unlock();

                LOG("Thread running batch:task %d:%d", task.run_batch_id, task.task_id);
                task.runnable->runTask(task.task_id, task.num_total_tasks);
                // LOG("Thread finished batch:task %d:%d", task.run_batch_id, task.task_id);

                lock.lock();
                num_tasks_completed++;
                // LOG("Thread incrementing num_tasks_completed to %d", num_tasks_completed.load());
                cv_tasks_completed.notify_one();
            }
            LOG("Thread finished"); 
        }));
   }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    std::unique_lock<std::mutex> lock(mutex);
    LOG("Destroying thread pool");
    destroy_threads = true;
    lock.unlock();
    cv_tasks.notify_all();
    LOG("Notified all threads to destroy");
    for (auto& thread : threads) {
        LOG("Joining thread");
        thread.join();
        LOG("Thread joined");
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    int batch_id = num_run_batch++;
    LOG("Running %d tasks for batch %d", num_total_tasks, batch_id);


    std::unique_lock<std::mutex> lock(mutex);

    for (int i = 0; i < num_total_tasks; i++) {
        LOG("Adding batch:task %d:%d to runnables", batch_id, i);
        Task task = {runnable, batch_id, i, num_total_tasks};
        tasks.push_back(task);
    }
    this->num_tasks_completed = 0;
    lock.unlock();
    cv_tasks.notify_all();
    LOG("Notified all threads to run tasks for batch %d", batch_id);

    std::unique_lock<std::mutex> lock2(mutex);
    LOG("Waiting for tasks for batch %d to complete ", batch_id);
    cv_tasks_completed.wait(lock2, [this, num_total_tasks]() {
        //// LOG("Checking if tasks for batch %d are complete: %d out of %d", batch_id, num_tasks_completed, num_total_tasks);
        return num_tasks_completed == num_total_tasks; });
    LOG("Tasks completed");
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
