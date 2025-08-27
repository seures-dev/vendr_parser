import os
from process_manager import ProcessManager
from dclass import Task, TaskType
from dotenv import load_dotenv
load_dotenv()


if __name__ == "__main__":
    process_manager = ProcessManager(process_count=int(os.getenv("PROCESS_COUNT")))
    process_manager.start()
    process_manager.add_task(Task(url=os.getenv("START_URL"), task_type=TaskType.ScrapeBookLinks))
    process_manager.monitor()
    
