#include "tasksys.h"


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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {

    for (int i = 0; i < num_threads; i++) {
        workerTaskQueues.emplace_back(std::unique_ptr<ThreadSafeQueue>(new ThreadSafeQueue()));
        threadPool.emplace_back(&TaskSystemParallelThreadPoolSleeping::runWorkerThread, this, workerTaskQueues.back().get(), i);
    }

    workerCount = num_threads;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    kill = true;
    for (size_t i=0;i<threadPool.size(); ++i) {
        workerTaskQueues[i]->cv.notify_one();
        threadPool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    runAsyncWithDeps(runnable, num_total_tasks, nullptr);

    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // check if the task can be run immediately

    ++tasksLeft;

    bool scheduleTask = true;

    if (deps != nullptr) {
        for (size_t i=0; i<deps.size(); i++) {
            for (int j=0; j<workerCount; j++) {
                if (tasks[j]->workersFinished.second == false) {
                    scheduleTask = false;
                    break;
                }
            }
            if (!scheduleTask) break;
        }
    }

    // put the task info 
    tasks.emplace_back(std::unique_ptr<TaskInfo>(new TaskInfo(runnable, workerCount, num_total_tasks)));

    if (scheduleTask) {
        AssignTasksStatically(runnable, num_total_tasks, startingTaskWorker);
    }

    return tasks.size()-1;
}

void TaskSystemParallelThreadPoolSleeping::AssignTasksStatically(IRunnable* runnable, int num_total_tasks, int workerTaskStart) {
    
    int tasksPerWorker = num_total_tasks / workerCount;
    int extraTaskWorkers = num_total_tasks % workerCount;

    int taskNumber = num_total_tasks - 1;

    // Assign the tasks evenly to workers
    for (int i=0; i<workerCount; ++i) {
        int numTasks = tasksPerWorker;
        if (extraTaskWorkers > 0) {
            ++numTasks;
            --extraTaskWorkers;
        }
        tasks.back()->lastWorkerTasks[workerTaskStart] = taskNumber - (numTasks - 1);
        ThreadSafeQueue* wtq = workerTaskQueues[workerTaskStart].get();
        wtq->mtx.lock();
        for (int j=0; j<numTasks; ++j) {
            //std::cout << "Putting task number " << taskNumber << " on the queue!" << '\n';
            wtq->tasks.emplace_back(tasks.size()-1, taskNumber);
            --taskNumber;
        }
        wtq->mtx.unlock();
        wtq->cv.notify_one();
        
        if (++workerTaskStart == workerCount) workerTaskStart = 0;
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    std::mutex mtx;
    std::unique_lock<std::mutex> lock(mtx);

    tasksFinished.wait(lock, [this] { return (tasksLeft <= 0); });

    tasksLeft = 0;
    tasks.clear();

    return;
}


void TaskSystemParallelThreadPoolSleeping::runWorkerThread(ThreadSafeQueue* tsq, int id) {

    //std::cout << "Running worker thread! " << id << '\n';
    //try {
    std::pair<int,int> myTask = {-1,-1};
    std::unique_lock<std::mutex> lock(tsq->mtx, std::defer_lock);
    int nextWorker = (id + 1) % workerCount;

    while (1) {
        //std::cout << "Running worker thread top of loop " << id << '\n';
        lock.lock();
        tsq->cv.wait(lock, [tsq, this] { return !tsq->tasks.empty() || kill; });
        if (kill) return;
        //std::cout << "Running worker thread taking from the queue " << id << '\n';
        myTask = tsq->tasks.front();
        //std::cout << "Running worker thread popping " << myTask << " from the queue " << id << '\n';
        tsq->tasks.pop_front();
        lock.unlock();
        
        //std::cout << "Running worker thread task number to run " << id << " " << myTask << '\n';
        if (myTask != {-1,-1}) {
            TaskInfo* myTaskInfo = tasks[myTask.first].get();
            //std::cout << "Running worker thread running task " << id << " " << myTask << '\n';
            myTaskInfo->runTask(myTask.second, tasks[myTask.first].totalTasks); // run the task

            if (myTask.second == myTaskInfo->lastWorkerTasks[id]) {
                if (myTaskInfo->pendingWorkers.fetch_sub(1) == 1) {
                    for (size_t i=0;i<myTaskInfo->dependents.size(); i++) {
                        TaskInfo* dependTaskInfo = tasks[myTaskInfo->dependents[i]].get();
                        if (dependTaskInfo->refs.fetch_sub(1) == 1) {
                            AssignTasksStatically(dependTaskInfo->runnable, dependTaskInfo->totalTasks, nextWorker);
                        }
                    }
                    
                    // less contention than before here, but if there are a lot of small batches of tasks its still bad
                    tasks[myTask.first].reset();
                    if (tasksLeft.fetch_sub(1) == 1) {
                        tasksFinished.notify_one();
                    }
                }
            }
            myTask = {-1,-1};
        }
    }
    //}
    /*catch (const std::system_error& e) {
    std::cerr << "Thread error worker: " << id << e.what() << '\n';
    std::cerr << "Thread error: " << id << " " << e.what() << "\n";
    std::cerr << "Error code: " << id << " " << e.code().value() << "\n";
    std::cerr << "Error category: " << id << " " << e.code().category().name() << "\n";
    std::cerr << "Error message: " << id << " " << e.code().message() << "\n";
    }
    std::cout << "Worker thread exiting! " << id << '\n';*/
    return;
}
