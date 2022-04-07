import multiprocessing
import time
import signal
import sys,termios

def interrupted(signum, frame):
    #print((signum,frame))
    raise Exception("end of time")

class job_plant():
    """
    This class allows a list of functions and their parameters to be run in parallell using the multiprocessing module

    Functions to be run can be added using add_job

    Start the process of running jobs accross multiple cores using start_jobs

    start_jobs can be stopped by pressing graceful_quit_key ('q' by default)
    Sometimes this has caused start jobs to hang and can be disabled.

    """
    def __init__(self,max_threads = 11):
        self.thread_count = 0
        self.thread_complete = 0
        self.max_threads = max_threads
        self.job_list = []
        self.finish_existing = False
        self.graceful_quit_enable = True
        self.graceful_quit_key = 'q'
    def print_msg(self,msg):
        print("[Job plant] - {}".format(msg))

    def input(self):
        try:
            foo = input()
            return foo
        except Exception as e:
            #print(e)
            return

    def check_jobs(self):
        self.thread_count = 0
        self.thread_complete = 0
        for p in self.processes:
            p.join(0.01/self.max_threads)
            if p.is_alive():
                self.thread_count+=1
            else:
                self.thread_complete+=1

    def add_job(self,job_func,job_param,from_front = False):
        if from_front:
            self.job_list.insert(0,(job_func,job_param))
        else:
            self.job_list.append((job_func,job_param))
    def start_jobs(self):
        self.start_time = time.time()
        job_index = 0
        self.processes = []
        print_counter = 0
        for job in self.job_list:

            total_time = time.time() - self.start_time

            if self.thread_complete == 0:
                avg_time =0
                est_time =0
            else:
                avg_time = total_time/self.thread_complete
                est_time = (len(self.job_list)-self.thread_complete)*avg_time

            if self.thread_count >= self.max_threads:
                self.print_msg("Waiting for threads (max {})...{}/{} jobs complete (avg time {:4.2f} mins| est time: {:4.1f} hours| total time {:4.1f} hours)".format(self.max_threads,self.thread_complete,len(self.job_list),avg_time/60.0,est_time/60.0/60.0,total_time/60.0/60.0))

            while(self.thread_count >= self.max_threads):
                self.check_jobs()

                if self.graceful_quit_enable:
                    TIMEOUT = 2
                    signal.signal(signal.SIGALRM, interrupted)
                    signal.alarm(TIMEOUT)
                    sys.stdout.flush()
                    s = self.input()
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_IGN)

                    if s == self.graceful_quit_key:
                        self.finish_existing = True
                        self.print_msg("Quit key pressed, waiting for jobs to finish...")

            if self.finish_existing:
                break
            else:
                p = multiprocessing.Process(target=job[0], args=job[1])
                p.start()

                self.processes.insert(0,p)
                job_index +=1
                self.thread_count+=1

        #all threads started wait to finish
        self.check_jobs() # run this at least one
        old_job_count = self.thread_count
        while(self.thread_count > 0):
            self.check_jobs()
            if old_job_count > self.thread_count:
                old_job_count = self.thread_count
                self.print_msg("Finishing up... {} threads remaining".format(self.thread_count))

        if self.thread_complete == len(self.job_list):
            self.print_msg("All jobs completed")
        else:
            self.print_msg("Exited early - Only {}/{} jobs completed".format(self.thread_complete,len(self.job_list)))
        self.processes = []
        signal.alarm(0)

        return self.thread_complete == len(self.job_list)
def test_foo():
    print("test foo")
def test_foo2(arg):
    print("test foo2 - {}".format(arg))
if __name__ == "__main__":

    num_cores = 11
    test_jobs = 100

    j = job_plant(num_cores)
    for i in range(test_jobs):
        j.add_job(test_foo,())
    j.start_jobs()


    j2 = job_plant(num_cores)
    for i in range(test_jobs):

        j2.add_job(test_foo2,(str(i),))
    j2.start_jobs()
