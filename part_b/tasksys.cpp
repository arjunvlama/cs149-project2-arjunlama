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

    workerCount = num_threads;

    for (int i = 0; i < num_threads; i++) {
        workerTaskQueues.emplace_back(std::unique_ptr<ThreadSafeQueue>(new ThreadSafeQueue()));
        threadPool.emplace_back(&TaskSystemParallelThreadPoolSleeping::runWorkerThread, this, workerTaskQueues.back().get(), i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    kill = true;
    for (size_t i=0;i<threadPool.size(); ++i) {
        workerTaskQueues[i]->cv.notify_one();
        threadPool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    runAsyncWithDeps(runnable, num_total_tasks, DEFAULT_VECTOR);

    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    // check if the task can be run immediately

    ////std::cout << "Starting async run task with total task count " << num_total_tasks << " and deps count " << deps.size() << '\n';

    ++tasksLeft;
    // put the task info 
    tasks.emplace_back(std::unique_ptr<TaskInfo>(new TaskInfo(runnable, workerCount, num_total_tasks, deps.size())));
    bool scheduleTask = false;

    for (size_t i=0; i<deps.size(); i++) {
        bool checked = true;
        TaskInfo* depTaskInfo = tasks[deps[i]].get();
        depTaskInfo->depsMtx.lock();

        if (!depTaskInfo->depsChecked) {
            depTaskInfo->dependents.push_back(tasks.size()-1);
            checked = false;
        }
        depTaskInfo->depsMtx.unlock();

        // if a task was even close to having its deps checked, perform the decrement on its behalf
        if (checked && tasks.back()->refs.fetch_sub(1) == 1) {
            ////std::cout << "Schedule task == true!! " << '\n';
            scheduleTask = true;
            break;
        }
    }

    if (scheduleTask || deps.empty()) {
        assignTasksStatically(runnable, tasks.size()-1, startingTaskWorker);
    }

    ////std::cout << "Finished async run task with task list size " << tasks.size() << '\n';

    return tasks.size()-1;
}

void TaskSystemParallelThreadPoolSleeping::assignTasksStatically(IRunnable* runnable, int parentTaskNum, int workerTaskStart) {
    
    ////std::cout << "Assigning task " << parentTaskNum << " from worker start " << workerTaskStart << '\n';

    int numTotalTasks = tasks[parentTaskNum]->totalTasks;
    //////std::cout << "Number of sub tasks for parent " << parentTaskNum << " is " << numTotalTasks << '\n';  
    int tasksPerWorker = numTotalTasks / workerCount;
    int extraTaskWorkers = numTotalTasks % workerCount;
    int taskNumber = numTotalTasks - 1;

    if (numTotalTasks < workerCount) {
        tasks[parentTaskNum]->pendingWorkers = numTotalTasks;
    }
    else {
        tasks[parentTaskNum]->pendingWorkers = workerCount;
    }

    // Assign the tasks evenly to workers
    for (int i=0; i<workerCount; ++i) {
        int numTasks = tasksPerWorker;
        if (extraTaskWorkers > 0) {
            ++numTasks;
            --extraTaskWorkers;
        }

        if (numTasks == 0) continue;

        tasks[parentTaskNum]->lastWorkerTasks[workerTaskStart] = taskNumber - (numTasks - 1);
        ////std::cout << "Last worker task for task " << parentTaskNum << " set to " << taskNumber - (numTasks - 1) << " for worker " << workerTaskStart << '\n';
        ThreadSafeQueue* wtq = workerTaskQueues[workerTaskStart].get();
        wtq->mtx.lock();
        for (int j=0; j<numTasks; ++j) {
            ////std::cout << "Putting task number " << parentTaskNum << " item number " << taskNumber << " on the queue!" << '\n';
            wtq->tasks.emplace_back(parentTaskNum, taskNumber);
            --taskNumber;
        }
        wtq->mtx.unlock();
        wtq->cv.notify_one();
        
        if (++workerTaskStart == workerCount) workerTaskStart = 0;
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    ////std::cout << "Entering sync call!" << '\n';

    std::mutex mtx;
    std::unique_lock<std::mutex> lock(mtx);

    tasksFinished.wait(lock, [this] { return (tasksLeft <= 0); });

    ////std::cout << "Made it past sync wait!" << '\n';

    tasksLeft = 0;
    tasks.clear();

    return;
}


void TaskSystemParallelThreadPoolSleeping::runWorkerThread(ThreadSafeQueue* tsq, int id) {

    ////std::cout << "Running worker thread! " << id << '\n';
    //try {
    std::pair<int,int> defaultTask = {-1, -1};
    std::pair<int,int> myTask = {-1,-1};
    std::unique_lock<std::mutex> lock(tsq->mtx, std::defer_lock);
    int nextWorker = (id + 1) % workerCount;

    while (1) {
        ////std::cout << "Running worker thread top of loop " << id << '\n';
        lock.lock();
        tsq->cv.wait(lock, [tsq, this] { return !tsq->tasks.empty() || kill; });
        if (kill) return;
        ////std::cout << "Running worker thread taking from the queue " << id << '\n';
        myTask = tsq->tasks.front();
        ////std::cout << "Running worker thread popping " << myTask.first << " item " << myTask.second << " from the queue " << id << '\n';
        tsq->tasks.pop_front();
        lock.unlock();
        
        ////std::cout << "Running worker thread task number to run " << myTask.first << " item " << myTask.second << " " << id << '\n';
        if (myTask != defaultTask) {
            TaskInfo* myTaskInfo = tasks[myTask.first].get();
            ////std::cout << "Running worker thread running task " << myTask.first << " item " << myTask.second << " " << id << '\n';
            myTaskInfo->runnable->runTask(myTask.second, myTaskInfo->totalTasks); // run the task

            if (myTask.second == myTaskInfo->lastWorkerTasks[id]) {
                if (myTaskInfo->pendingWorkers.fetch_sub(1) == 1) {
                    //std::cout << "Finished task " << myTask.first << " item " << myTask.second << " " << id << '\n';
                    myTaskInfo->depsMtx.lock();
                    myTaskInfo->depsChecked = true;
                    myTaskInfo->depsMtx.unlock();

                    for (size_t i=0;i<myTaskInfo->dependents.size(); i++) {
                        TaskInfo* dependTaskInfo = tasks[myTaskInfo->dependents[i]].get();
                        if (dependTaskInfo->refs.fetch_sub(1) == 1) {
                            //std::cout << "Assigning task " << myTaskInfo->dependents[i] << " to worker " << nextWorker << " " << id << '\n';
                            assignTasksStatically(dependTaskInfo->runnable, myTaskInfo->dependents[i], nextWorker);
                            if (++nextWorker == workerCount) nextWorker = 0;
                        }
                    }
                    
                    // less contention than before here, but if there are a lot of small batches of tasks its still bad
                    if (tasksLeft.fetch_sub(1) == 1) {
                        //std::cout << "Notifying sync of task completion for: " << myTask.first << " " << id << '\n';
                        tasksFinished.notify_one();
                    }
                }
            }
            myTask = defaultTask;
        }
    }
    /*}
    catch (const std::system_error& e) {
    std::cerr << "Thread error worker: " << id << e.what() << '\n';
    std::cerr << "Thread error: " << id << " " << e.what() << "\n";
    std::cerr << "Error code: " << id << " " << e.code().value() << "\n";
    std::cerr << "Error category: " << id << " " << e.code().category().name() << "\n";
    std::cerr << "Error message: " << id << " " << e.code().message() << "\n";
    }
    //std::cout << "Worker thread exiting! " << id << '\n';*/
    return;
}
