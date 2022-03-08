from functions import Functions

class Parsing:
    def parse_type(key, value):
        print(f'The type for {key} is {value["Type"]}')
        return value['Type']

    def parse_exec_func(key, value):
        print(f'The parse/exec for {key} is {value["Execution"]}')
        return value['Execution']

    def parse_activities(type, key, value):
        print(f'The activites for {key} are {value["Activities"]}')
        if(type == 'Flow'):
            Parsing.test(type, key, value["Activites"])        
        return value["Activites"]

    def test(type, key, value):
        if(type == 'Flow'):
            exec = Parsing.parse_exec_func(key, value)
            if(exec == 'Sequential'):
                Parsing.parse_activities(type, key, value)
        else:
            func = Parsing.parse_exec_func(key, value)
            if(func == 'TimeFunction'):
                Functions.TimeFunction(value['Time'])
