import uuid
import os
import json
import shutil
import subprocess
import shlex
import time

from pprint import pprint
from pathlib import Path
from filelock import FileLock, Timeout

"""
    This class is mainly for sanity checking rather than archiving information.
"""

class ExperimentUnit:
    def __copy_one_level_dict(src, dst):
        for key, val in src.items():
            dst[key] = val

    def __get_md5sum(filepath):
        filepath = str(filepath)
        process_info = subprocess.run(["md5sum", filepath], capture_output=True)
        md5sum = "-1"
        if process_info.returncode == 0:
            md5sum = process_info.stdout.strip().split()[0].decode()
        else:
            print("Warn: md5sum failed for", filepath)
        return md5sum

    def __init__(self, gem5_binary_path, gem5_config_path, gem5_output_path, gem5_params, config_params, env):
        self.gem5_binary_path = gem5_binary_path
        self.gem5_config_path = gem5_config_path
        self.gem5_output_path = gem5_output_path
        self.gem5_params = {}
        ExperimentUnit.__copy_one_level_dict(gem5_params, self.gem5_params)
        self.config_params = {}
        ExperimentUnit.__copy_one_level_dict(config_params, self.config_params)
        self.uuid = str(uuid.uuid4())
        self.metadata = {}
        self.return_code = -1
        self.launch_time = -1
        self.env = {}
        ExperimentUnit.__copy_one_level_dict(env, self.env)

        self.gem5_binary_hash = ExperimentUnit.__get_md5sum(self.gem5_binary_path)

    def init_from_ExperimentUnit(other):
        unit = ExperimentUnit(other.gem5_binary_path, other.gem5_config_path, other.gem5_output_path, other.gem5_params, other.config_params, env)
        unit.uuid = other.uuid
        unit.metadata = {}
        ExperimentUnit.__copy_one_level_dict(other.metadata, unit.metadata)
        unit.return_code = other.return_code
        unit.launch_time = other.launch_time

        unit.gem5_binary_hash = ExperimentUnit.__get_md5sum(unit.gem5_binary_path)

    def add_metadata(self, key, val):
        self.metadata[key] = val

    def __params_dict_to_list(params):
        params_list = []
        for key, val in params.items():
            params_list.append(key)
            if not val:
                pass
            else:
                params_list.append(val)
        return params_list

    def __launch(self):
        # remove old output dir
        dirpath = Path(self.gem5_output_path)
        if dirpath.exists() and dirpath.is_dir():
            shutil.rmtree(dirpath)
        elif dirpath.exists() and not dirpath.is_dir():
            print("Error:", dirpath, "exists and not a directory.")
            return False
        dirpath.mkdir(parents=True, exist_ok=True)

        self.status = "running"
        self.__dump_info()

        # launch the experiment
        gem5_params_list = ExperimentUnit.__params_dict_to_list(self.gem5_params)
        config_params_list = ExperimentUnit.__params_dict_to_list(self.config_params)

        stdout_path = Path(self.gem5_output_path) / 'run_stdout'
        stderr_path = Path(self.gem5_output_path) / 'run_stderr'
        assert(stdout_path != stderr_path)
        env = {**os.environ, **self.env}

        command = [self.gem5_binary_path] + gem5_params_list + [self.gem5_config_path] + config_params_list

        with open(stdout_path, "w") as f:
            with open(stderr_path, "w") as g:
                process_info = subprocess.run(command, stdout=f, stderr=g, env=env)
        self.return_code = process_info.returncode

        # dump information
        self.status = "finished"
        self.__dump_info()

    def __to_JSON_str(self):
        return json.dumps(self,
                          default=lambda o: o.__dict__,
                          sort_keys=True,
                          indent=4)

    def __dump_info(self):
        output_path = Path(self.gem5_output_path)
        info_file = output_path / 'info.json'
        info_json_path_lock = output_path / 'info.json.lock'
        try:
            lock = FileLock(info_json_path_lock)
            with lock.acquire(timeout=120):
                with open(info_file, "w") as f:
                    # https://stackoverflow.com/questions/3768895/how-to-make-a-class-json-serializable
                    json.dump(self, f,
                              default=lambda o: o.__dict__,
                              sort_keys=True,
                              indent=4)
                    lock.release()
        except Timeout:
            print("Error: __dump_info failed to acquire the lock for file", info_file)
            exit(1)

    def __is_runnable(self, run_if_failed):
        output_path = Path(self.gem5_output_path)
        info_json_path = output_path / 'info.json'
        info_json_path_lock = output_path / 'info.json.lock'

        if not output_path.exists():
            return True

        if not info_json_path.exists():
            return True

        try:
            lock = FileLock(info_json_path_lock)
            with lock.acquire(timeout=120):
                with open(info_json_path, "r") as f:
                    j = json.load(f)
                    if not "return_code" in j:
                        print("Warn: \"return_code\" not found for", info_json_path)
                        lock.release()
                        return True
                    if not "status" in j:
                        print("Warn: \"status\" not found for", info_json_path)
                        lock.release()
                        return True
                    if j["status"] == "running" or j["status"] == "finished":
                        lock.release()
                        return False
                    if j["return_code"] == "0":
                        diff = []
                        if not self.gem5_binary_path == j["gem5_binary_path"]:
                            diff.append("gem5_binary_path", self.gem5_binary_path, j["gem5_binary_path"])
                        elif not self.gem5_output_path == j["gem5_output_path"]:
                            diff.append("gem5_output_path", self.gem5_output_path, j["gem5_output_path"])
                        elif not self.gem5_params == j["gem5_params"]:
                            diff.append("gem5_params", self.gem5_params, j["gem5_params"])
                        elif not self.gem5_binary_hash == j["gem5_binary_hash"]:
                            diff.append("gem5_binary_hash", self.gem5_binary_hash, j["gem5_binary_hash"])
                        elif not self.metadata == j["metadata"]:
                            diff.append("metadata", self.metadata, j["metadata"])
                        if len(diff) > 0:
                            print("Warn: Not rerun an experiment but different information")
                            pprint(diff)
                            lock.release()
                        return False
                    else:
                        lock.release()
                        return run_if_failed
        except Timeout:
            print("Warn: failed to acquire the lock for file", info_json_path)
            return False

        return False

    def try_launch(self, run_if_failed = False, run_if_already_run = True):
        if run_if_already_run:
            return self.__launch()

        runnable = self.__is_runnable(run_if_failed)

        if runnable:
            return self.__launch()
        else:
            print("Info:", self.gem5_output_path, "is not launchable")

        return False
