import logging
import yaml
from functions import Functions

log = logging.getLogger("my-logger")

class YamlParser:
    def __init__(self, file):
        self.file = file
        self.data = None
        self.tasks = []
        self.flow = ''

    def open(self):
        with open(self.file, 'r') as yaml_file:
            self.data = yaml.load(yaml_file, Loader=yaml.FullLoader)
        return self.data

    def parse(self, data=None, execution='Sequential'):
        if data is None:
            data = self.data
        if data is None:
            return

        for key, value in data.items():
            if value['Type'] == 'Flow':
                # print(key)
                self.flow = self.flow + "." + key
                log.error(f'{self.flow} Entry')                
                self.parse(value['Activities'], value['Execution'])                
                flow_name = self.flow.split('.')               
                log.error(f'{flow_name[1]} Exit')
            elif value['Type'] == 'Task':
                name = self.flow + "." + key
                log.error(f'{name} Entry')
                if execution == 'Concurrent':
                    self.tasks[-1].append(value)
                elif execution == 'Sequential':                    
                    Functions.TimeFunction(name, value['Inputs']['FunctionInput'], int(value['Inputs']['ExecutionTime']))                
                log.error(f'{name} Exit')
            else:
                print('Unknown Type:', value['Type'])
                return
            
