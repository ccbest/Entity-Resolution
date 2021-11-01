
from importlib import machinery
import inspect
import os

from toolkit.path_manager import PathManager


class ImportManager(object):

    """
    Imports all custom functions and creates a "registry" that orchestrators can reference
    """

    FILE_BLACKLIST = ['__init__.py']

    def __init__(self):
        self.registry = {}
        self.register_data_source_modules()

        standardizers = PathManager("pre_resolve", "standardize").from_project_root()
        self.register_function(
            {"standardizers": self.get_functions_in_dir(standardizers)}
        )

        self.register_resolution_modules()

    def register_function(self, obj: dict):
        """
        Updates the registry

        Args:
            obj (dict): registry entries

        Returns:
            None
        """
        self.registry = self._merge_dict_structures(self.registry, obj)

    def get_function(self, *args, **kwargs):
        """
        Gets a function from the registry.

        Args:
            *args: registry keys (in sequence)
        Keyword Args:
             registry: Internal use only.

        Returns:

        """
        registry = kwargs.get("registry", self.registry)

        if not args:
            return registry

        if args[0] in registry:
            return self.get_function(*args[1:], registry=registry[args[0]])

        raise ValueError(f"Error: {args[0]} not found in custom function registry.")


    def register_data_source_modules(self):
        """
        Registers fetchers and mungers from the data sources.

        Returns:
            None
        """
        loc = PathManager("data_sources").from_project_root()
        for source in os.listdir(loc):
            if '__' not in source:
                self.register_fetchers(source, loc / source)
                self.register_mungers(source, loc / source)

    def register_fetchers(self, source_name, path):
        """
        Registers fetchers from the data sources
        Args:
            source_name: The name of the data source
            path: the path to the data source scripts

        Returns:

        """
        functions = self.get_functions_from_module('fetch', path / 'fetch.py')
        fetchers = []
        for function in functions:
            if function.__name__ == 'fetch':
                fetchers.append(function)

        if not fetchers:
            raise ImportError(f"Error: could not find function 'fetch' for {source_name}.")

        self.register_function({"fetchers": {source_name: fetchers}})

    def register_mungers(self, source_name, path):
        """
        Registers mungers from the data sources
        Args:
            source_name: The name of the data source
            path: the path to the data source scripts

        Returns:

        """
        functions = self.get_functions_from_module('munge', path / 'munge.py')
        mungers = []
        for function in functions:
            if function.__name__ == 'munge':
                mungers.append(function)

        if not mungers:
            raise ImportError(f"Error: could not find function 'munge' for {source_name}.")

        self.register_function({"mungers": {source_name: mungers}})

    def register_resolution_modules(self):
        """
        Registers the functions required for entity resolution.

        Returns:

        """
        resolve_loc = PathManager("resolution").from_project_root()
        similarity_funcs = self.get_functions_in_dir(resolve_loc / "transforms")
        self.register_function({"similarity": similarity_funcs})

        similarity_funcs = self.get_functions_in_dir(resolve_loc / "score_aggregation")
        self.register_function({"score_aggregation": similarity_funcs})

        similarity_funcs = self.get_functions_in_dir(resolve_loc / "comparison_metrics")
        self.register_function({"comparison_metrics": similarity_funcs})

        similarity_funcs = self.get_functions_in_dir(resolve_loc / "blockers")
        self.register_function({"blockers": similarity_funcs})



    @staticmethod
    def get_functions_from_module(module_name, path):
        """
        Pulls all of the functions out of a specified module.

        Args:
            module_name: The name of the module (usually the filename without '.py')
            path: the path to the module

        Returns:
            A list of functions that are defined in the module
        """
        module = machinery.SourceFileLoader(module_name, str(path)).load_module(module_name)
        functions = [getattr(module, item) for item in dir(module)
                     if inspect.isfunction(getattr(module, item)) and not item.startswith('_')]

        return functions

    def get_functions_in_dir(self, path):
        """
        Walks through every file in a dir (not recursively) and returns a list of all the functions
        Args:
            path: filepath to the dir

        Returns:
            (dict) {function_name : function_pointer} for every function founds
            TODO: should make sure there aren't multiple functions with the same name
        """
        functions = []
        for file in os.listdir(path):
            if os.path.isfile(path / file) and file not in self.FILE_BLACKLIST:
                functions += self.get_functions_from_module(file.replace('.py', ''), path / file)

        return {function.__name__: function for function in functions}

    def _merge_dict_structures(self, a, b, _path=None):
        """
        Merges two JSON structures

        Args:
            a: first JSON structure
            b: second JSON structure
            _path: Internal use only. Private variable to keep track of location in JSON

        Returns:
            Dictionary structure comprised of all key/values from both A and B
        """
        _path = _path or []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self._merge_dict_structures(a[key], b[key], _path + [str(key)])
                elif isinstance(a[key], list) and isinstance(b[key], list):
                    a[key] = a[key] + b[key]
                else:
                    a[key] = b[key]
            else:
                a[key] = b[key]
        return a


custom_function_registry = ImportManager()
