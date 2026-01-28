#include "tasksys.h"

#include <algorithm>
#include <thread>

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
                    std::unique_lock<std::mutex> lock(mutex);
                    cv.wait(lock, [this]() { return runnables.size() > 0 || destroy_threads; });
                    if (destroy_threads) {
                        break;
                    }
                    runnable = runnables.front();
                    runnables.pop_front();
                    task_id = task_id_counter++;
                    cv.notify_all();
                }
                runnable->runTask(task_id, num_total_tasks);
                num_tasks_completed++;
                cv.notify_all();
            }
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // std::unique_lock<std::mutex> lock(mutex);
    // destroy_threads = true;
    // cv.notify_all();
    // lock.unlock();
    // for (auto& thread : threads) {
    //     thread.join();
    // }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    std::unique_lock<std::mutex> lock(mutex);

    for (int i = 0; i < num_total_tasks; i++) {
        runnables.push_back(runnable);
    }

    this->num_total_tasks = num_total_tasks;
    this->num_tasks_completed = 0;
    this->task_id_counter = 0;
    lock.unlock();
    cv.notify_all();

    while (num_tasks_completed < num_total_tasks) {
        // printf("Tasks completed: %d\n", this->num_tasks_completed.load());
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
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
