#include "tasksys.h"
#include <thread>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <condition_variable>

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

void TaskSystemParallelThreadPoolSleeping::workerFunc(int thread_id) {
    int my_runnable_tasks;
    int start_task_id;
    
    std::unique_lock<std::mutex> lock(mtx_);

    while (!shutdown_) {
        has_work_cv_.wait(lock, [this]{ return !runnable_tasks_.empty() || shutdown_; });
        if (shutdown_) break;

        // get runnable task from front of queue
        BulkTask& task = all_tasks_.at(runnable_tasks_.front());
        my_runnable_tasks = std::min(task.num_runnable_tasks, task.granularity);
        start_task_id = task.num_total_tasks - task.num_runnable_tasks;
        task.num_runnable_tasks -= my_runnable_tasks;

        // if no runnable work left for this task, remove from queue
        if (task.num_runnable_tasks == 0) runnable_tasks_.pop_front();

        lock.unlock();

        // run my_runnable_tasks instances of the current runnable
        for (int i = start_task_id; i < start_task_id + my_runnable_tasks; ++i) {
            task.runnable->runTask(i, task.num_total_tasks);
        }
        
        lock.lock();
        // update completion count
        task.num_completed_tasks += my_runnable_tasks;

        // check if task completed
        if (task.num_completed_tasks == task.num_total_tasks) {
            task.completed = true;
            num_incomplete_--;

            // check for waiting dependents
            auto it = dependents_.find(task.id);
            if (it != dependents_.end()) {
                // decrement unmet dependency count
                for (auto dep_id : it->second) {
                    BulkTask& dep_task = all_tasks_.at(dep_id);
                    dep_task.num_unmet_deps--;
                    // if the dependent is now unblocked, enqueue
                    if (dep_task.num_unmet_deps == 0) {
                        runnable_tasks_.push_back(dep_id);
                        has_work_cv_.notify_all();
                    }
                }
            }

            // if all tasks completed, notify completion
            if (num_incomplete_ == 0) all_done_cv_.notify_one();
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads) {

    next_task_id_ = 0;
    num_incomplete_ = 0;
    shutdown_ = false;
    for (int i = 0; i < num_threads; ++i) {
        workers_.emplace_back(std::thread(&TaskSystemParallelThreadPoolSleeping::workerFunc, this, i));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        shutdown_ = true;
    }
    has_work_cv_.notify_all();
    for (auto& worker : workers_) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> deps;
    runAsyncWithDeps(runnable, num_total_tasks, deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

    TaskID task_id = next_task_id_;
    int num_unmet_deps = static_cast<int>(deps.size());

    // compute granularity
    int granularity;
    if (num_total_tasks <= num_threads * 2) {
        granularity = std::max(num_total_tasks / num_threads, 1);  
    } else {
        granularity = std::max(num_total_tasks / (num_threads * 4), 1);
    }

    std::unique_lock<std::mutex> lock(mtx_);

    // check dependencies
    for (auto dep_id : deps) {
        if (all_tasks_.at(dep_id).completed) {
            // if dependency already completed, decrement dependencies
            num_unmet_deps--;
        } else {
            // otherwise, add to reverse dependency map
            auto it = dependents_.find(dep_id);
            if (it != dependents_.end()) {
                it->second.push_back(task_id);
            } else {
                dependents_[dep_id] = {task_id};
            }
        }   
    }

    // add to all_tasks
    BulkTask task = {task_id,runnable, granularity, num_total_tasks,
        num_total_tasks, num_unmet_deps, 0, false};
    all_tasks_[task_id] = task;
    num_incomplete_++;
    
    // if already runnable, enqueue
    if (num_unmet_deps == 0) {
        runnable_tasks_.push_back(task_id);
    }
    
    lock.unlock();

    next_task_id_++;

    // notify workers
    has_work_cv_.notify_all();
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mtx_);
    all_done_cv_.wait(lock, [this]{ return num_incomplete_ == 0; });
    
    // clear data structures
    all_tasks_.clear();
    dependents_.clear();
    runnable_tasks_.clear();
    next_task_id_ = 0;
}
