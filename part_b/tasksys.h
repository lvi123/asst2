#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>
#include <mutex>
#include <atomic>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        struct TaskBatch {
            TaskID tbatch_id;
            IRunnable* runnable;
            int num_total_tasks;
            int num_tasks_completed;
            std::vector<TaskID> deps;
        };
        
        struct Task {
            int task_id;
            IRunnable* runnable;
            int num_total_tasks;
            TaskID tbatch_id;
        };

        void runTaskBatchLocked(const TaskBatch& tbatch);

        std::vector<std::thread> threads;

        std::mutex mutex;

        // counter for task batch ids
        TaskID tbatch_id_counter;
        // map of task batches which are waiting for dependencies to complete
        std::unordered_map<TaskID, TaskBatch> tbatch_waiting_for_deps;
        // map of task batches which are running
        std::unordered_map<TaskID, TaskBatch> tbatch_running;
        // map of task batches which are completed
        std::unordered_set<TaskID> completed_tbatches;
        // flag to indicate when one or more running task batches are completed
        bool some_tbatch_completed;
        // condition variable to notify when a running task batch is completed
        std::condition_variable cv_tbatch_completed;

        // queue of running tasks
        std::deque<Task> tasks;
        // condition variable to notify when a running task is added
        std::condition_variable cv_tasks;

        // flag to indicate when threads should be destroyed
        std::atomic<bool> destroy_threads;


};

#endif
