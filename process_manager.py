from multiprocessing import Process, Queue
from queue import Empty
import time
from dclass import Task
from scraper import Scraper
from writer import Writer

def run_scraper(scraper_id: int, task_queue: Queue, error_queue: Queue, result_queue: Queue):
    scraper = Scraper(scraper_id, task_queue, error_queue, result_queue)
    scraper.run()

def run_writer(result_queue: Queue, task_queue: Queue):
    writer = Writer(result_queue, task_queue)
    writer.run()


class ProcessManager:
    def __init__(self, process_count: int = 3):
        self.process_count = process_count
        self.processes = {}
        self.task_queue = Queue()
        self.error_queue = Queue()
        self.result_queue = Queue()
        self.writer = None

    def start(self):
        for i in range(self.process_count):
            self._start_process(i)
        self.writer = Process(target=run_writer, args=(self.result_queue, self.task_queue), daemon=True)
        self.writer.start()

    def _start_process(self, process_id: int):
        process = Process(target=run_scraper, args=(process_id, self.task_queue, self.error_queue, self.result_queue), daemon=True)
        process.start()
        self.processes[process_id] = process

    def stop(self):
        for process in self.processes.values():
            process.terminate()
        self.processes.clear()
        self.writer.terminate()
        self.writer.join()

    def add_task(self, task: Task):
        self.task_queue.put(task)
        
    def restart_process(self, process_id: int):
        try:
            self.processes[process_id].terminate()
            self._start_process(process_id)
            print(f"Process {process_id} restarted")
        except Exception as e:
            print(f"Error restarting process {process_id}: {e}")

    def monitor(self):
        empty_count = 0
        print("Monitor started")
        while True:
            if self.task_queue.qsize() == 0 and self.result_queue.qsize() == 0:
                empty_count += 1
                if empty_count > 10:
                    break
            else:
                empty_count = 0
            for i in range(self.process_count):
                if not self.processes[i].is_alive():
                    self.restart_process(i)

            print(f"Task queue size: {self.task_queue.qsize()}, Result queue size: {self.result_queue.qsize()}, Error queue size: {self.error_queue.qsize()}")
            try:
                task = self.error_queue.get_nowait()
            except Empty:
                time.sleep(0.5)
                continue
            print(f"Error: {task}")
            self.restart_process(task.scraper_id)
            new_task = Task(task.url, task.task_type)
            self.add_task(new_task)
            
        self.stop()
        print("ProcessManager stopped")
