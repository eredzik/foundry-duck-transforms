import importlib.util
import pkgutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType

from .transform_df import Transform


@dataclass
class Pipeline:
    _transforms: list[Transform] = field(default_factory=list)

    def discover_transforms(self, *modules: ModuleType):
        
        for mod in modules:
            self._discover_transforms_path(list(mod.__path__))
            
            
    def _discover_transforms_path(self, path:list[str]):
        for submod in pkgutil.iter_modules(path):
            finder_path = getattr(submod.module_finder, "path", None)
            if not isinstance(finder_path, str):
                continue
            mod_path: str = finder_path + "/" + submod.name

            if submod.ispkg:
                self._discover_transforms_path([mod_path])
            else:
                file_path = mod_path + ".py"
                # Use a stable, unique module name so functions get a meaningful __module__.
                stable_name = "pipeline_" + str(abs(hash(str(Path(file_path).resolve()))))
                spec = importlib.util.spec_from_file_location(stable_name, file_path)
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)  # type: ignore
                sys.modules[stable_name] = module
                spec.loader.exec_module(module)  # type:ignore
                
                
                for item in module.__dict__.values():
                    if isinstance(item, Transform):
                        self._transforms.append(item)
                pass        

    