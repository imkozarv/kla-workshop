from time import sleep
import logging

log = logging.getLogger("my-logger")
class Functions:
    def TimeFunction(name, task_name, x):
        log.error(f'{name} Executing TimeFunction {(task_name, x)}')
        # print(f'executing this function for {x} seconds')
        sleep(x)
        return