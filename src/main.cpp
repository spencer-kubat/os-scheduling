#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <chrono>
#include <thread>
#include <mutex>
#include <ncurses.h>
#include "configreader.h"
#include "process.h"

// Shared data for all cores
typedef struct SchedulerData {
    std::mutex queue_mutex;
    ScheduleAlgorithm algorithm;
    uint32_t context_switch;
    uint32_t time_slice;
    std::list<Process*> ready_queue;
    bool all_terminated;
} SchedulerData;

void coreRunProcesses(uint8_t core_id, SchedulerData *data);
void printProcessOutput(std::vector<Process*>& processes);
std::string makeProgressString(double percent, uint32_t width);
uint64_t currentTime();
std::string processStateToString(Process::State state);
void pushToReadyQueue(Process *process, SchedulerData *data);

int main(int argc, char *argv[])
{
    // Ensure user entered a command line parameter for configuration file name
    if (argc < 2)
    {
        std::cerr << "Error: must specify configuration file" << std::endl;
        exit(EXIT_FAILURE);
    }

    // Declare variables used throughout main
    int i;
    SchedulerData *shared_data = new SchedulerData();
    std::vector<Process*> processes;

    // Read configuration file for scheduling simulation
    SchedulerConfig *config = scr::readConfigFile(argv[1]);

    // Store number of cores in local variable for future access
    uint8_t num_cores = config->cores;

    // Store configuration parameters in shared data object
    shared_data->algorithm = config->algorithm;
    shared_data->context_switch = config->context_switch;
    shared_data->time_slice = config->time_slice;
    shared_data->all_terminated = false;

    // Create processes 
    uint64_t start = currentTime();
    for (i = 0; i < config->num_processes; i++)
    {
        Process *p = new Process(config->processes[i], start);
        processes.push_back(p);
        // If process should be launched immediately, add to ready queue
        if (p->getState() == Process::State::Ready)
        {
            shared_data->ready_queue.push_back(p);
        }
    }

    // Free configuration data from memory
    scr::deleteConfig(config);

    // Launch 1 scheduling thread per cpu core
    std::thread *schedule_threads = new std::thread[num_cores];
    for (i = 0; i < num_cores; i++)
    {
        schedule_threads[i] = std::thread(coreRunProcesses, i, shared_data);
    }

    // Main thread work goes here
    initscr();
    while (!(shared_data->all_terminated))
    {
        uint64_t current_time = currentTime();
        uint64_t elapsed_time = current_time - start;
        Process *lowest_priority_process_running = nullptr;
        bool all_processes_terminated = true;

        // MARIA: Solved the //todo - mutex
        // Added artificial scope blocks { } around the queue modification.
        // The closing brace forces the lock to release
        {
            std::lock_guard<std::mutex> lock(shared_data->queue_mutex);  

            for (i = 0; i < processes.size(); i++)
            {
                Process::State process_state = processes[i]->getState();
                uint64_t timeInCurrentBurst = current_time - processes[i]->getBurstStartTime();
                if (process_state == Process::State::Terminated) continue;
                else all_processes_terminated = false;
                
                if (process_state == Process::State::NotStarted && elapsed_time >= processes[i]->getStartTime()) 
                {
                    processes[i]->setState(Process::State::Ready, current_time);
                    pushToReadyQueue(processes[i], shared_data);
                }

                if (process_state == Process::State::IO)
                {
                    if (timeInCurrentBurst >= processes[i]->getCurrentBurstDuration())
                    {
                        processes[i]->setState(Process::State::Ready, current_time);
                        processes[i]->increaseBurst();
                        pushToReadyQueue(processes[i], shared_data);
                    }

                }

                // RR AND Time slice finished
                if (shared_data->algorithm == ScheduleAlgorithm::RR && process_state == Process::State::Running
                    && timeInCurrentBurst >= shared_data->time_slice)
                {
                    processes[i]->interrupt();
                }

                // find the lowest priority of all running processes (if any and if algorithm is PP)
                if (shared_data->algorithm == ScheduleAlgorithm::PP && process_state == Process::State::Running 
                    && (lowest_priority_process_running == nullptr || processes[i]->getPriority() > lowest_priority_process_running->getPriority()))
                {
                    lowest_priority_process_running = processes[i];
                }
            }

            // if higher priority in ready queue, interrupt lowest priority process running
            if (shared_data->algorithm == ScheduleAlgorithm::PP 
                && !shared_data->ready_queue.empty() 
                && lowest_priority_process_running != nullptr 
                && shared_data->ready_queue.front()->getPriority() < lowest_priority_process_running->getPriority())
            {
                lowest_priority_process_running->interrupt();
            }

            
            if (all_processes_terminated) shared_data->all_terminated = true;
        }
        
        // Do the following:
        //   - Get current time
        //   - *Check if any processes need to move from NotStarted to Ready (based on elapsed time), and if so put that process in the ready queue
        //   - *Check if any processes have finished their I/O burst, and if so put that process back in the ready queue
        //   - *Check if any running process need to be interrupted (RR time slice expires or newly ready process has higher priority)
        //     - NOTE: ensure processes are inserted into the ready queue at the proper position based on algorithm
        //   - Determine if all processes are in the terminated state
        //   - * = accesses shared data (ready queue), so be sure to use proper synchronization

        // Maybe simply print progress bar for all procs?
        printProcessOutput(processes);

        // sleep 50 ms
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // clear outout
        erase();
    }


    // wait for threads to finish
    for (i = 0; i < num_cores; i++)
    {
        schedule_threads[i].join();
    }

    // print final statistics (use `printw()` for each print, and `refresh()` after all prints)
    //  - CPU utilization
    //  - Throughput
    //     - Average for first 50% of processes finished
    //     - Average for second 50% of processes finished
    //     - Overall average
    //  - Average turnaround time
    //  - Average waiting time
    
    double total_turnaround = 0;
    double total_wait = 0;
    double total_cpu_time = 0;
    std::vector<double> termination_times;

    // iterate through processes to get stats
    for (int i = 0; i < processes.size(); i++)
    {
        total_turnaround += processes[i]->getTurnaroundTime();
        total_wait += processes[i]->getWaitTime();
        total_cpu_time += processes[i]->getCpuTime();
        
        double arrival_time_sec = processes[i]->getStartTime() / 1000.0;
        double term_time_sec = arrival_time_sec + processes[i]->getTurnaroundTime();
        termination_times.push_back(term_time_sec);
    }

    double avg_turnaround_time = total_turnaround / processes.size();
    double avg_waiting_time = total_wait / processes.size();

    // cpu utilization
    uint64_t end_time = currentTime();
    double total_sim_time_sec = (end_time - start) / 1000.0;
    double cpu_utilization = (total_cpu_time / (total_sim_time_sec * num_cores)) * 100.0;

    // sort termination times for first and second half of throughput
    std::sort(termination_times.begin(), termination_times.end());
    int mid_index = processes.size() / 2;
    
    // calculate first and second halves
    double time_first_half = termination_times[mid_index - 1];
    double throughput_first_50 = mid_index / time_first_half;
    double time_second_half = termination_times.back() - time_first_half;
    double throughput_second_50 = (processes.size() - mid_index) / time_second_half;
    
    // overall average
    double throughput_overall = processes.size() / termination_times.back();

    printw("\n");    
    printw("CPU Utilization:                            %.2f %%\n", cpu_utilization);
    printw("Throughput (First half of processes):       %.4f procs/sec\n", throughput_first_50);
    printw("Throughput (Second half of processes):      %.4f procs/sec\n", throughput_second_50);
    printw("Throughput (Overall Average):               %.4f procs/sec\n", throughput_overall);
    printw("Average Turnaround Time:                    %.2f sec\n", avg_turnaround_time);
    printw("Average Waiting Time:                       %.2f sec\n", avg_waiting_time);
    printw("\n");
    printw("Press any key to exit...\n");
    refresh();
    getch();

    // Clean up before quitting program
    processes.clear();
    endwin();

    return 0;
}

void coreRunProcesses(uint8_t core_id, SchedulerData *shared_data)
{
    while (!(shared_data->all_terminated))
    {        
        Process *current_process = nullptr;
        // MARIA: Solved the //todo - mutex
        {
            std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
            if (!shared_data->ready_queue.empty())
            {
                current_process = shared_data->ready_queue.front();
                shared_data->ready_queue.pop_front();
            }
        }

        if (current_process == nullptr)
        {
            // if empty - wait
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        else
        {
            // context switch time
            std::this_thread::sleep_for(std::chrono::milliseconds(shared_data->context_switch));
            uint64_t current_time = currentTime(); 

            bool process_running = true;
            current_process->setState(Process::State::Running, current_time); 
            current_process->setCpuCore(core_id);
            current_process->setBurstStartTime(currentTime());

            
            while (process_running)
            {                
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                current_time = currentTime();
                current_process->updateProcess(current_time);
                uint64_t timeInCurrentBurst = current_time - current_process->getBurstStartTime();

                if (current_process->getRemainingTime() <= 0) // terminated
                {
                    current_process->setState(Process::State::Terminated, current_time);
                    process_running = false;
                }
                else if (timeInCurrentBurst >= current_process->getCurrentBurstDuration()) // check if running burst finished
                {
                    current_process->setState(Process::State::IO, current_time);

                    // MARIA: Solved the //todo - update current_burst
                    // The I/O wait is finished, so we increase the index to point to the next CPU burst.
                    current_process->increaseBurst(); 
                    current_process->setBurstStartTime(current_time);
                    process_running = false;
                }
                else if (current_process->isInterrupted()) // interrupted
                {
                    current_process->setState(Process::State::Ready, current_time);
                    // MARIA: Solved the //todo - update CPU burst time
                    // Calculate the remaining time
                    uint32_t remaining_time = current_process->getCurrentBurstDuration() - timeInCurrentBurst;
                    // Overwrite the current burst duration with the new remaining time
                    current_process->updateBurstTime(current_process->getCurrentBurstIndex(), remaining_time);

                    {
                        std::lock_guard<std::mutex> lock(shared_data->queue_mutex);
                        pushToReadyQueue(current_process, shared_data);
                    }                                        
                    
                    current_process->interruptHandled();
                    process_running = false;
                }                
            }

            // context switch time
            std::this_thread::sleep_for(std::chrono::milliseconds(shared_data->context_switch));
        }
    }

    // Work to be done by each core idependent of the other cores
    // Repeat until all processes in terminated state:
    //   - *Get process at front of ready queue
    //   - IF READY QUEUE WAS NOT EMPTY
    //    - Wait context switching load time
    //    - Simulate the processes running (i.e. sleep for short bits, e.g. 5 ms, and call the processes `updateProcess()` method)
    //      until one of the following:
    //      - CPU burst time has elapsed
    //      - Interrupted (RR time slice has elapsed or process preempted by higher priority process)
    //   - Place the process back in the appropriate queue
    //      - I/O queue if CPU burst finished (and process not finished) -- no actual queue, simply set state to IO
    //      - Terminated if CPU burst finished and no more bursts remain -- set state to Terminated
    //      - *Ready queue if interrupted (be sure to modify the CPU burst time to now reflect the remaining time)
    //   - Wait context switching save time
    //  - IF READY QUEUE WAS EMPTY
    //   - Wait short bit (i.e. sleep 5 ms)
    //  - * = accesses shared data (ready queue), so be sure to use proper synchronization
}

void pushToReadyQueue(Process *process, SchedulerData *shared_data)
{
    // don't need mutex here since all calls to this will already be locked

    int queue_size = shared_data->ready_queue.size();
    if (shared_data->algorithm == ScheduleAlgorithm::PP)
    {
        for (auto itr = shared_data->ready_queue.begin(); itr != shared_data->ready_queue.end(); ++itr)
        {
            if (process->getPriority() < (*itr)->getPriority())
            {
                shared_data->ready_queue.insert(itr, process);
                return;
            }
        }
        shared_data->ready_queue.push_back(process);
    }
    else if (shared_data->algorithm == ScheduleAlgorithm::SJF)
    {
        for (auto itr = shared_data->ready_queue.begin(); itr != shared_data->ready_queue.end(); ++itr)
        {
            if (process->getRemainingTime() < (*itr)->getRemainingTime())
            {
                shared_data->ready_queue.insert(itr, process);
                return;
            }
        }
        shared_data->ready_queue.push_back(process);
    }
    else
    {
        shared_data->ready_queue.push_back(process);
    }
}

void printProcessOutput(std::vector<Process*>& processes)
{
    printw("|   PID | Priority |    State    | Core |               Progress               |\n"); // 36 chars for prog
    printw("+-------+----------+-------------+------+--------------------------------------+\n");
    for (int i = 0; i < processes.size(); i++)
    {
        if (processes[i]->getState() != Process::State::NotStarted)
        {
            uint16_t pid = processes[i]->getPid();
            uint8_t priority = processes[i]->getPriority();
            std::string process_state = processStateToString(processes[i]->getState());
            int8_t core = processes[i]->getCpuCore();
            std::string cpu_core = (core >= 0) ? std::to_string(core) : "--";
            double total_time = processes[i]->getTotalRunTime();
            double completed_time = total_time - processes[i]->getRemainingTime();
            std::string progress = makeProgressString(completed_time / total_time, 36);
            printw("| %5u | %8u | %11s | %4s | %36s |\n", pid, priority,
                   process_state.c_str(), cpu_core.c_str(), progress.c_str());
        }
    }
    refresh();
}

std::string makeProgressString(double percent, uint32_t width)
{
    uint32_t n_chars = percent * width;
    std::string progress_bar(n_chars, '#');
    progress_bar.resize(width, ' ');
    return progress_bar;
}

uint64_t currentTime()
{
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
    return ms;
}

std::string processStateToString(Process::State state)
{
    std::string str;
    switch (state)
    {
        case Process::State::NotStarted:
            str = "not started";
            break;
        case Process::State::Ready:
            str = "ready";
            break;
        case Process::State::Running:
            str = "running";
            break;
        case Process::State::IO:
            str = "i/o";
            break;
        case Process::State::Terminated:
            str = "terminated";
            break;
        default:
            str = "unknown";
            break;
    }
    return str;
}
