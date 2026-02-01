#include "tasksys.h"

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
    fflush(stdout);
}

#ifdef LOG_ENABLED
#define LOG(...) log(__func__, __VA_ARGS__)
#else
#define LOG(...)
#endif


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
ITaskSystem(num_threads), 
tbatch_id_counter(0), 
some_tbatch_completed(false),
destroy_threads(false) {

   LOG("Creating thread pool with %d threads", num_threads);
   for (int i = 0; i < num_threads; i++) {
       threads.push_back(std::thread([this]() {
            LOG("Task thread started");          
            while (true) {
                std::unique_lock<std::mutex> lock(mutex);

                #ifdef LOG_ENABLED
                if (tasks.empty()) {
                    LOG("Tasks queue is empty");
                }
                #endif

                cv_tasks.wait(lock, [this]() { return !tasks.empty() || destroy_threads; });
                if (destroy_threads) {
                    LOG("Destroying threads");
                    break;
                }
                Task task = tasks.front();
                tasks.pop_front();
                lock.unlock();

                LOG("Running batch:task %d:%d of %d", task.tbatch_id, task.task_id, task.num_total_tasks);
                task.runnable->runTask(task.task_id, task.num_total_tasks);
                LOG("Finished batch:task %d:%d", task.tbatch_id, task.task_id);

                lock.lock();
                auto it = tbatch_running.find(task.tbatch_id);
                if (it == tbatch_running.end()) {
                    LOG("Task batch %d not found in running map", task.tbatch_id);
                    continue;
                }
                TaskBatch& tbatch = it->second;
                tbatch.num_tasks_completed++;
                LOG("Incremented num_tasks_completed for batch %d to %d", task.tbatch_id, tbatch.num_tasks_completed);
                if (tbatch.num_tasks_completed == task.num_total_tasks) {
                    LOG("Task batch %d has all tasks completed", task.tbatch_id);
                    some_tbatch_completed = true;
                    lock.unlock();
                    cv_tbatch_completed.notify_all();
                }
            }
            LOG("Task thread finished"); 
        }));
   }

   LOG("Create thread for task batch dependency completion monitoring");
   threads.push_back(std::thread([this]() {
    while (true) {
        std::unique_lock<std::mutex> lock(mutex);
        cv_tbatch_completed.wait(lock, [this]() { return some_tbatch_completed || destroy_threads; });
        if (destroy_threads) {
            LOG("Destroying thread for task batch dependency completion monitoring");
            break;
        }
        some_tbatch_completed = false;

        // check which running task batches are completed
        LOG("Checking which running task batches have all tasks completed");
        for (auto it = tbatch_running.begin(); it != tbatch_running.end(); ) {
            TaskBatch& tbatch = it->second;
            if (tbatch.num_tasks_completed == tbatch.num_total_tasks) {
                LOG("Task batch %d has all tasks completed", tbatch.tbatch_id);
                completed_tbatches.insert(tbatch.tbatch_id);
                it = tbatch_running.erase(it);
            } else {
                it++;
            }
        }

        LOG("Checking which task batches are waiting for dependencies to complete");
        for (auto it = tbatch_waiting_for_deps.begin(); it != tbatch_waiting_for_deps.end(); ) {
            TaskBatch& tbatch = it->second;
            bool all_deps_completed = true;
            for (auto& dep : tbatch.deps) {
                if (completed_tbatches.find(dep) == completed_tbatches.end()) {
                    LOG("Task batch %d still waiting for dependency %d to complete", tbatch.tbatch_id, dep);
                    all_deps_completed = false;
                    break;
                }
            }
            if (all_deps_completed) {
                LOG("Task batch %d has all dependencies completed, running it", tbatch.tbatch_id);
                runTaskBatchLocked(tbatch);
                it = tbatch_waiting_for_deps.erase(it);
            } else {
                it++;
            }
        }

        #ifdef LOG_ENABLED
        if (tbatch_waiting_for_deps.empty()) {
            LOG("No task batches are waiting for dependencies to complete");
        }
        if (tbatch_running.empty()) {
            LOG("No task batches are running");
        }
        #endif

        lock.unlock();
        cv_tbatch_completed.notify_all();

    }
   }));
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    LOG("Destroying thread pool");
    destroy_threads = true;
    cv_tasks.notify_all();
    cv_tbatch_completed.notify_all();
    LOG("Notified all threads to destroy");
    for (auto& thread : threads) {
        LOG("Joining thread");
        thread.join();
        LOG("Thread joined");
    }
    LOG("Thread pool destroyed");
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    /* TaskID tbatch_id = */ runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

// add tasks to the queue for the given task batch
// this function is called with the mutex held
void TaskSystemParallelThreadPoolSleeping::runTaskBatchLocked(
    const TaskBatch& tbatch) {

    LOG("Running task batch %d with %d tasks", tbatch.tbatch_id, tbatch.num_total_tasks);

    tbatch_running.insert({tbatch.tbatch_id, tbatch});

    for (int i = 0; i < tbatch.num_total_tasks; i++) {
        Task task = {
            i /* task_id */,
            tbatch.runnable,
            tbatch.num_total_tasks,
            tbatch.tbatch_id};
        LOG("Adding task %d to batch %d", task.task_id, task.tbatch_id);
        tasks.push_back(task);
    }
    LOG("Notifying all threads to run tasks");
    cv_tasks.notify_all();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, 
    int num_total_tasks,
    const std::vector<TaskID>& deps) {
        
    LOG("Running async with deps: %d tasks", num_total_tasks);

    std::lock_guard<std::mutex> lock(mutex);

    TaskID tbatch_id = tbatch_id_counter++;

    TaskBatch tbatch = {
        tbatch_id, 
        runnable,
        num_total_tasks, 
        0 /* num_tasks_completed */,
        deps};

    bool all_deps_completed = true;
    for (auto& dep : deps) {
        if (completed_tbatches.find(dep) == completed_tbatches.end()) {
            LOG("Dependency %d not completed", dep);
            all_deps_completed = false;
            break;
        }
    }

    if (all_deps_completed) {
        runTaskBatchLocked(tbatch);
    } else {
        LOG("Not running task batch %d yet because dependencies are not completed", tbatch_id);
        tbatch_waiting_for_deps.insert({tbatch_id, tbatch});
    }

    return tbatch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    std::unique_lock<std::mutex> lock(mutex);
    cv_tbatch_completed.wait(lock, [this]() { return tbatch_running.empty() && tbatch_waiting_for_deps.empty(); });

    #ifdef LOG_ENABLED

    for (auto& tbatch_id : completed_tbatches) {
        LOG("Task batch %d completed", tbatch_id);
    }

    #endif

    return;
}
