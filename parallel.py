import time
import pickle
import random
import hashlib
import inspect
import builtins
from collections import defaultdict
from functools import partial
from multiprocessing.pool import ThreadPool

def run_in_threads(funcs, args, kwargs):
    if not funcs:
        return []
    with ThreadPool(processes=min(150, len(funcs))) as pool:
        threads = []
        for i, func in enumerate(funcs):
            t = pool.apply_async(func, args=args[i], kwds=kwargs[i])
            threads.append(t)
        results = [t.get() for t in threads]
    return results

def hash_object(obj):
    obj_bytes = pickle.dumps(obj)
    return hashlib.md5(obj_bytes).hexdigest()

class Parallel:
    func_calls = defaultdict(dict)
    cached_calls = defaultdict(dict)
    cached_attrs = defaultdict(dict)

    def __init__(self, serial=False, _call_path="", _obj_reference=None):
        """
        Args:
            serial (bool): whether to run the function calls in parallel or serially
            _call_path (str): the full path to the function to be called
            obj_reference (object): the object to be parallelized
        """
        self._obj_reference = _obj_reference
        self._call_path = _call_path
        self.serial = serial
        self.iter_list = None
        self.iter = 0

    @staticmethod
    def reset():
        Parallel.func_calls = defaultdict(dict)

    def execute(self):
        parallel_objects, arguments = zip(*Parallel.func_calls.values())
        funcs = [parallel_object._obj_reference for parallel_object in parallel_objects]
        args_list = [arg[0] for arg in arguments]
        kwargs_list = [arg[1] for arg in arguments]
        results = run_in_threads(funcs, args_list, kwargs_list)
        for i, func in enumerate(funcs):
            obj_call_hash = hash_object([func, args_list[i], kwargs_list[i]])
            if func == range:  # range is a special case
                Parallel.cached_calls[obj_call_hash] = Parallel(serial=self.serial, _obj_reference=results[i])
            else:
                Parallel.cached_calls[obj_call_hash] = results[i]
        Parallel.reset()

    def _get_obj_from_attr(self, attr):
        # if this is an access of an existing object, pass current
        # object's attribute
        if self._obj_reference and attr in dir(self._obj_reference):
            return getattr(self._obj_reference, attr)
        # look for attribute in local/global scope of previous functions
        frame_infos = inspect.stack()
        for frame in frame_infos:
            frame = frame.frame
            if attr in frame.f_locals:
                return frame.f_locals[attr]
            elif attr in frame.f_globals:
                return frame.f_globals[attr]
        # if attribute still not found, look in builtins
        if attr in dir(builtins):
            return getattr(builtins, attr)
        return None

    def auto(self, func):
        def wrapper(*args, **kwargs):
            prev_func_queue, cur_func_queue = -1, len(self.func_calls)
            while True:
                try:
                    func(*args, **kwargs)
                    break
                except Exception as e:
                    prev_func_queue, cur_func_queue = cur_func_queue, len(self.func_calls)
                    if prev_func_queue == cur_func_queue:
                        self.execute()
                        prev_func_queue, cur_func_queue = -1, len(self.func_calls)
        return wrapper

    def __getattr__(self, name):
        if not self.serial:
            new_call_path = self._call_path + ("." if self._call_path else "") + name
            obj = self._get_obj_from_attr(name)
            obj_hash = hash_object(obj)
            if obj_hash not in Parallel.cached_attrs and obj:
                Parallel.cached_attrs[obj_hash] = Parallel(serial=self.serial, _call_path=new_call_path, _obj_reference=obj)
            return Parallel.cached_attrs[obj_hash]
        else:
            return globals()[name]

    def __call__(self, *args, **kwargs):
        if not self.serial:
            obj_call_hash = hash_object([self._obj_reference, args, kwargs])
            if obj_call_hash in Parallel.cached_calls:
                return Parallel.cached_calls[obj_call_hash]
            if obj_call_hash not in Parallel.func_calls:
                Parallel.func_calls[obj_call_hash] = (self, (args, kwargs))
        else:
            return self._obj_reference(*args, **kwargs)

    def __iter__(self):
        if not self.iter_list:
            self.iter_list = list(self._obj_reference)
        for _ in range(len(self.iter_list)):
            self.iter = self.iter + 1
            yield self.iter_list[(self.iter - 1) % len(self.iter_list)]

    def __eq__(self, other):
        raise NotImplementedError("Comparison operations are not supported for Parallel objects.")

    def __ne__(self, other):
        raise NotImplementedError("Comparison operations are not supported for Parallel objects.")

    def __lt__(self, other):
        raise NotImplementedError("Comparison operations are not supported for Parallel objects.")

    def __le__(self, other):
        raise NotImplementedError("Comparison operations are not supported for Parallel objects.")

    def __gt__(self, other):
        raise NotImplementedError("Comparison operations are not supported for Parallel objects.")

    def __ge__(self, other):
        raise NotImplementedError("Comparison operations are not supported for Parallel objects.")

    def __hash__(self):
        raise NotImplementedError("Hashing is not supported for Parallel objects.")