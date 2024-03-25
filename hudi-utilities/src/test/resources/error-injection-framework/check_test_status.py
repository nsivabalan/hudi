import argparse
import sys
import os
import re
from collections import defaultdict
import subprocess
import json
from typing import DefaultDict


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--user"])


try:
    from rich import print, pretty
except:
    install("rich")
finally:
    from rich import print, pretty
    from rich.console import Console
    from rich.tree import Tree
    from rich import print
    from rich.columns import Columns
    from rich.panel import Panel

console = Console()


PASS_STATUS = "Succeeded"
FAIL_STATUS = "Failed"
RUNNING_STATUS = "Running"
PANEL = [
    ["status", "Test Status"],
    ["current_itr", "Current Dag Round"],
    ["failed_node", "Failed Node"],
    ["running_node", "Last Running Node"],
    ["ttype", "Table Type"],
    ["metadata_enable", "Metadata Enable"],
    ["yml", "YML Type Executed"],
    ["time_taken", "Run Time Minutes"],
    ["progress", "Spark Application Completed"],
    ["application_id", "Spark Application Id"],
    ["test_file_name", "Test Origin File"],
    ["log_file", "Logs Origin File"],
]
INFRA_TEST_FAILURE = """Exception in thread "main" org.apache.hudi.exception.HoodieException: Failed to run Test Suite"""
PANEL = {
    "delta": "Deltastreamer Job",
    "latest": "Validation Job for Latest files",
    "fully": "Validation Job for all files",
}


def get_matching_subtext(pattern, text):
    matched = re.compile(pattern).search(text)
    if matched:
        return matched.group(1).strip()


def create_panel(test_file_name, row):
    print(test_file_name)
    status = row["status"]
    color = "green"
    if status == FAIL_STATUS.upper():
        color = "red"
    elif status == RUNNING_STATUS.upper():
        color = "yellow"
    elif status is None:
        color = "blue"
    failed_node = row.get("failed_node")
    if test_file_name:
        test_name = test_file_name.replace("_spark_command.sh", "")
        test_args = [arg.upper() for arg in test_name.split("-")]
        _test = f"Infra Test: {' '.join(test_args)}"
        ttype, metadata_enable, yml = test_args
    else:
        ttype, metadata_enable, yml, _test = None, None, None, None
    row["ttype"] = ttype
    row["test_file_name"] = test_file_name
    row["metadata_enable"] = metadata_enable
    row["yml"] = yml
    row["failed_node"] = failed_node
    final = f"[b][{color}]{_test}[/b]"
    for attr, text in PANEL:
        final += f"\n[color]{text}: {row.get(attr)}"

    return final


def get_stats_for_test(file_path):
    node_dicts = []
    with open(file_path) as f:
        lines = f.readlines()
        deltastreamer_run_number = -1
        latest_file_validation_run = -1
        full_file_validation_run = -1
        data_dict = defaultdict(dict)
        for line in lines:
            line = line.strip()
            deltastreamer_run_number = (
                get_matching_subtext("Start deltastreamer run (.*) ...", line)
                or deltastreamer_run_number
            )
            deltastreamer_run_number = int(deltastreamer_run_number)
            if (
                deltastreamer_run_number >= 0
                and deltastreamer_run_number not in data_dict
            ):
                data_dict[deltastreamer_run_number] = defaultdict(dict)
                data_dict[deltastreamer_run_number]["delta"] = defaultdict(list)

                data_dict[deltastreamer_run_number]["fully"] = defaultdict(list)
            latest_file_validation_run = (
                get_matching_subtext(
                    "Validate latest file slides and base files run (.*) ...", line
                )
                or latest_file_validation_run
            )
            if (
                get_matching_subtext(
                    "Validate latest file slides and base files run (.*) ...", line
                )
                == latest_file_validation_run
            ):
                data_dict[deltastreamer_run_number]["latest"] = defaultdict(list)

            latest_file_validation_run = int(latest_file_validation_run)

            full_file_validation_run = (
                get_matching_subtext("Validate all file groups run (.*) ...", line)
                or full_file_validation_run
            )

            if (
                get_matching_subtext("Validate all file groups run (.*) ...", line)
                == latest_file_validation_run
            ):
                data_dict[deltastreamer_run_number]["fully"] = defaultdict(list)

            full_file_validation_run = int(full_file_validation_run)
            if latest_file_validation_run < deltastreamer_run_number:
                if (
                    "Exception" in line
                    or line.startswith("Error:")
                    or "No such file or directory" in line
                    or "OutOfMemoryError" in line
                ) and "Unable to attach Serviceability Agent" not in line:
                    if (
                        line
                        not in data_dict[deltastreamer_run_number]["delta"]["errors"]
                    ):
                        data_dict[deltastreamer_run_number]["delta"]["errors"].append(
                            line
                        )
                        # print(data_dict[deltastreamer_run_number]["delta"]["errors"])
                if "Killing the jvm at" in line:
                    if (
                        line
                        not in data_dict[deltastreamer_run_number]["delta"]["Killed"]
                    ):
                        data_dict[deltastreamer_run_number]["delta"]["Killed"].append(
                            line
                        )

            elif (
                latest_file_validation_run == deltastreamer_run_number
                and full_file_validation_run < latest_file_validation_run
            ):
                if (
                    "Exception" in line
                    or line.startswith("Error:")
                    or "No such file or directory" in line
                    or "OutOfMemoryError" in line
                ) and "Unable to attach Serviceability Agent" not in line:
                    if (
                        line
                        not in data_dict[deltastreamer_run_number]["latest"]["errors"]
                    ):
                        data_dict[deltastreamer_run_number]["latest"]["errors"].append(
                            line
                        )
                pass
                # print("latest_file_validation_run")
            elif (
                latest_file_validation_run
                == deltastreamer_run_number
                == full_file_validation_run
                and deltastreamer_run_number >= 0
            ):
                if (
                    "Exception" in line
                    or line.startswith("Error")
                    or "No such file or directory" in line
                    or "OutOfMemoryError" in line
                ) and "Unable to attach Serviceability Agent" not in line:
                    if (
                        line
                        not in data_dict[deltastreamer_run_number]["fully"]["errors"]
                    ):
                        data_dict[deltastreamer_run_number]["fully"]["errors"].append(
                            line
                        )
        # print(data_dict)
        if details:
            for run, data in data_dict.items():
                if run_id is None or run == run_id:
                    tree = Tree(f"[yellow]Run Number : {run}")
                    for _type, job_data in data.items():
                        if _type != "fully" or args.all_files:
                            _type = PANEL.get(_type, _type)
                            # print(_type, job_data)
                            # break
                            node_dict = {}
                            node_dict["Status"] = (
                                "Succeed"
                                if len(job_data["errors"]) == 0
                                and len(job_data["Killed"]) == 0
                                else "Failed"
                            )
                            color = "green"
                            if node_dict["Status"] == "Failed":
                                node_dict["Exceptions"] = job_data["errors"]
                                node_dict["Killed"] = job_data["Killed"]
                                color = "red"
                            _node = tree.add(f"[bold {color}]{_type}")
                            for k, v in node_dict.items():
                                if k == "Exceptions":
                                    k_node = _node.add(f"[{color}]{k}")
                                    [k_node.add(f"[{color}]{e}") for e in v]
                                elif k == "Killed":
                                    if len(v) > 0:
                                        e_node = _node.add(f"[yellow]{k}")
                                        [e_node.add(f"[yellow]{e}") for e in v]
                                    else:
                                        _node.add(f"[{color}]{k}: {False}")
                                else:
                                    _node.add(f"[{color}]{k}: {v}")
                    print(tree)
        # panel_row = {
        #     "status": application_status,
        #     "application_id": application_id,
        #     "time_taken": time_taken_minutes,
        #     "progress": f"{progress}%",
        #     "log_file": file_path.split("/")[-1],
        #     "current_itr": current_itr,
        #     "running_node": running_node,
        # }
        # if application_status == FAIL_STATUS.upper():
        #     panel_row["failed_node"] = node_status[-1][0]
        # test_panel = Panel(create_panel(test_file_name, panel_row))
        # return test_panel


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Upsert to RDS Mysql table.")
    parser.add_argument(
        "--log_file",
        type=str,
        help=" test log file.",
    )
    parser.add_argument(
        "--run_id",
        type=int,
        default=None,
        help=" Run number for loop.",
    )
    parser.add_argument(
        "--all_files",
        type=bool,
        default=False,
        help=" Run number for loop.",
    )
    args = parser.parse_args(sys.argv[1:])
    details = True
    run_id = args.run_id
    all_files = args.all_files
    r = get_stats_for_test(args.log_file)
    # parser.add_argument(
    #     "--log_dir",
    #     type=str,
    #     default="/Users/smittal/upwork_tasks/27217766/docker/.logs",
    #     help="infra test log directory.",
    # )
    # parser.add_argument(
    #     "--all",
    #     type=bool,
    #     default=False,
    #     help="get all files and create consolidated report",
    # )
    # parser.add_argument(
    #     "--details", type=bool, default=False, help="detailed information"
    # )
    # parser.add_argument(
    #     "--show_panels", type=bool, default=True, help="consolidated information"
    # )
    # args = parser.parse_args(sys.argv[1:])

    # print(args)

    # log_file = args.log_file
    # log_dir = args.log_dir
    # all = args.all
    # details = args.details
    # show_panels = args.show_panels
    # test_panels = []
    # if all:
    #     log_files = sorted(
    #         [
    #             os.path.join(log_dir, f)
    #             for f in os.listdir(log_dir)
    #             if os.path.isfile(os.path.join(log_dir, f))
    #         ],
    #         key=os.path.getctime,
    #     )
    #     for _log_file in log_files:
    #         test_panel = get_stats_for_test(os.path.join(log_dir, _log_file))
    #         test_panels.append(test_panel)
    # else:
    #     test_panels = [get_stats_for_test(os.path.join(log_dir, log_file))]
    # if show_panels:
    #     console.print(Columns(test_panels))
