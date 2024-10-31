import importlib.util
import sys


def import_from_path(module_name:str, file_path:str):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # Register the module in sys.modules
    spec.loader.exec_module(module)  # Execute the module in its own namespace
    return module
if __name__ =='__main__':
    mod = import_from_path('transform', sys.argv[1])
    print('hello')