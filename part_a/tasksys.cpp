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


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
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

    for (int i = 0; i < num_threads; i++) {
        workerTaskQueues.emplace_back(std::unique_ptr<ThreadSafeQueue>(new ThreadSafeQueue()));
        threadPool.emplace_back(&TaskSystemParallelThreadPoolSleeping::runWorkerThread, this, workerTaskQueues.back().get());
    }

    workerCount = num_threads;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    kill = true;
    for (size_t i=0;i<threadPool.size(); ++i) {
        workerTaskQueues[i]->cv.notify_one();
        threadPool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Run will have num_total_tasks to run, a worker will signal to this thread when to wake up because the tasks are done

    task = runnable;
    tasksLeft = num_total_tasks;
    totalTasks = num_total_tasks;

    std::mutex mtx;
    std::unique_lock<std::mutex> lock(mtx);

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
        ThreadSafeQueue* wtq = workerTaskQueues[i].get();
        wtq->mtx.lock();
        for (int j=0; j<numTasks; ++j) {
            wtq->tasks.push_back(taskNumber);
            --taskNumber;
        }
        wtq->mtx.unlock();
        wtq->cv.notify_one();
    }

    tasksFinished.wait(lock, [this] { return (tasksLeft <= 0); });
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

void TaskSystemParallelThreadPoolSleeping::runWorkerThread(ThreadSafeQueue* tsq) {

    int myTask = -1;
    std::unique_lock<std::mutex> lock(tsq->mtx);

    while (!kill) {
        tsq->cv.wait(lock, [tsq] { return !tsq->tasks.empty(); });
        myTask = tsq->tasks.front();
        tsq->tasks.pop_front();
        lock.unlock();
        
        while (myTask != -1) {
            task->runTask(myTask, totalTasks); // run the task

            // Tasks left idea might be overcontrived, 
            // there will be a lot of contention here

            if (tasksLeft.fetch_sub(1) <= 0) {
                tasksFinished.notify_one();
                break;
            }

            myTask = -1;
            lock.lock();
            if (!tsq->tasks.empty()) {
                myTask = tsq->tasks.front();
                tsq->tasks.pop_front();
            }
            lock.unlock();
        }
    }

    return;
}
