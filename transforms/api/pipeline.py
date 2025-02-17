import importlib.util
import pkgutil
from dataclasses import dataclass, field
from types import ModuleType

from .transform_df import Transform


@dataclass
class Pipeline:
    _transforms: list[Transform] = field(default_factory=list)

    def discover_transforms(self, *modules: ModuleType):
        
        for mod in modules:
            
            if not isinstance(mod, ModuleType):
                raise TypeError("modules must be of type ModuleType")
            submodules = pkgutil.iter_modules(mod.__path__)
            self._discover_transforms_path(mod.__path__)
            
            
    def _discover_transforms_path(self, path:list[str]):
        submodules = pkgutil.iter_modules(path)
        for submod in submodules:
            mod_path = submod.module_finder.path + "/" + submod.name

            if submod.ispkg:
                self._discover_transforms_path([mod_path])
            else:

                spec = importlib.util.spec_from_file_location('test', mod_path + ".py")
                module = importlib.util.module_from_spec(spec)  # type: ignore
                # sys.modules[module_name] = module  # Register the module in sys.modules
                spec.loader.exec_module(  # type:ignore
                    module
                )  # Execute the module in its own namespace
                
                
                for item in module.__dict__.values():
                    if isinstance(item, Transform):
                        self._transforms.append(item)
                pass        

    