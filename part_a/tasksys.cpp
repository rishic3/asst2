#include "tasksys.h"
#include <thread>
#include <mutex>
#include <vector>
#include <stdio.h>
#include <assert.h>


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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // don't spawn unnecessary threads for underutilized launches
    int threads_to_use = num_threads;
    if (num_total_tasks < num_threads) {
        threads_to_use = num_total_tasks;
    }

    std::thread workers[threads_to_use-1];
    int tasks_per_worker = num_total_tasks / threads_to_use;
    int remainder = num_total_tasks % threads_to_use;

    // worker func - run num_worker_tasks in a loop
    auto worker_func = [runnable, num_total_tasks](int num_worker_tasks, int start_task_id) {
        for (int i = start_task_id; i < start_task_id + num_worker_tasks; ++i) {
            runnable->runTask(i, num_total_tasks);
        }
    };

    for (int i = 0; i < threads_to_use - 1; ++i) {
        // int num_worker_tasks = tasks_per_worker;
        // if (i == threads_to_use - 1) num_worker_tasks += remainder;  // last thread gets remainder
        workers[i] = std::thread(worker_func, tasks_per_worker, i * tasks_per_worker);
    }

    worker_func(tasks_per_worker + remainder,  (threads_to_use - 1) * tasks_per_worker);
    
    for (int i = 0; i < threads_to_use - 1; ++i) {
        workers[i].join();
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

void TaskSystemParallelThreadPoolSpinning::workerFunc(int thread_id) {
    int my_runnable_tasks;
    int start_task_id;
    while (!shutdown) {
        my_runnable_tasks = 0;
        start_task_id = -1;

        {
            std::lock_guard<std::mutex> lock(mtx);
            if (num_runnable_tasks > 0) {
                // there are some tasks to be run
                // take min between the pre-defined granularity or however many tasks are left
                my_runnable_tasks = std::min(num_runnable_tasks, granularity);

                // printf("thread %d sees %d runnable tasks, taking %d for itself\n", thread_id, num_runnable_tasks, my_runnable_tasks);
                start_task_id = num_total_tasks - num_runnable_tasks;
                num_runnable_tasks -= my_runnable_tasks;
            }
        }

        if (my_runnable_tasks > 0) {
            // printf("  thread %d is running %d tasks starting at task id %d\n", thread_id, my_runnable_tasks, start_task_id);
            assert(start_task_id >= 0);

            // run my_num_tasks instances of the current runnable
            for (int i = start_task_id; i < start_task_id + my_runnable_tasks; ++i) {
                runnable->runTask(i, num_total_tasks);
            }

            num_completed_tasks.fetch_add(my_runnable_tasks);
            // printf("  thread %d finished %d tasks\n", thread_id, my_runnable_tasks);
        }
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads) {
    num_runnable_tasks = 0;
    shutdown = false;
    for (int i = 0; i < num_threads; ++i) {
        workers.emplace_back(std::thread(&TaskSystemParallelThreadPoolSpinning::workerFunc, this, i));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdown = true;
    for (auto& worker : workers) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    {
        std::lock_guard<std::mutex> lock(mtx);
        this->num_total_tasks = num_total_tasks;
        this->runnable = runnable;
        num_runnable_tasks = num_total_tasks;
        num_completed_tasks = 0;

        // heuristic - tune granularity according to relative tasks per thread
        if (num_total_tasks <= num_threads * 2) {
            // make sure granularity is at least 1 if num_total_tasks < num_threads
            granularity = std::max(num_total_tasks / num_threads, 1);  
        } else {
            granularity = std::max(num_total_tasks / (num_threads * 4), 1);
        }
            
        // printf("run() - main thread set num_runnable_tasks to %d, granularity to %d\n", num_runnable_tasks, granularity);
    }

    bool done = false;
    while (!done) {
        if (num_completed_tasks == num_total_tasks) {
            done = true;
            // printf("main thread detected run is finished\n");
        }
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

void TaskSystemParallelThreadPoolSleeping::workerFunc(int thread_id) {
    int my_runnable_tasks;
    int start_task_id;
    std::unique_lock<std::mutex> lock(mtx);

    while (!shutdown) {
        has_work.wait(lock, [this]{ return num_runnable_tasks > 0 || shutdown; });
        if (shutdown) break;

        // there are some tasks to be run
        // take min between the pre-defined granularity or however many tasks are left
        my_runnable_tasks = std::min(num_runnable_tasks, granularity);
        // printf("thread %d sees %d runnable tasks, taking %d for itself\n", thread_id, num_runnable_tasks, my_runnable_tasks);
        start_task_id = num_total_tasks - num_runnable_tasks;
        num_runnable_tasks -= my_runnable_tasks;

        lock.unlock();

        // printf("  thread %d is running %d tasks starting at task id %d\n", thread_id, my_runnable_tasks, start_task_id);
        // run my_num_tasks instances of the current runnable
        for (int i = start_task_id; i < start_task_id + my_runnable_tasks; ++i) {
            runnable->runTask(i, num_total_tasks);
        }

        num_completed_tasks.fetch_add(my_runnable_tasks);
        // printf("  thread %d finished %d tasks\n", thread_id, my_runnable_tasks);
        
        lock.lock();
        // if all work is completed, awaken main thread
        if (num_completed_tasks == num_total_tasks) {
            all_done.notify_one();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads) {
    num_runnable_tasks = 0;
    shutdown = false;
    for (int i = 0; i < num_threads; ++i) {
        workers.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerFunc, this, i));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mtx);
        shutdown = true;
    }
    has_work.notify_all();
    for (auto& worker : workers) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::unique_lock<std::mutex> lock(mtx);
    // set up run
    this->num_total_tasks = num_total_tasks;
    this->runnable = runnable;
    num_runnable_tasks = num_total_tasks;
    num_completed_tasks = 0;

    // heuristic - tune granularity according to relative tasks per thread
    if (num_total_tasks <= num_threads * 2) {
        // make sure granularity is at least 1 if num_total_tasks < num_threads
        granularity = std::max(num_total_tasks / num_threads, 1);  
    } else {
        granularity = std::max(num_total_tasks / (num_threads * 4), 1);
    }    
    // printf("run() - main thread set num_runnable_tasks to %d, granularity to %d\n", num_runnable_tasks, granularity);
    lock.unlock();

    // wake threads
    has_work.notify_all();

    // main thread waits for completion
    lock.lock();
    all_done.wait(lock, [this]{ return num_completed_tasks == this->num_total_tasks; });
    lock.unlock();
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
