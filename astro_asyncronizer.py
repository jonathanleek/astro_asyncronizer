import json
import os
import re

operator_pairs = json.loads("operator_pairs.json")


def asyncronize_dag(filename, async_filename):
    # Read file data to memory
    with open(filename, "r") as file:
        filedata = file.read()

        # Replace operators with async equivalents
        for operator_pair in operator_pairs["operator_pairs"]:
            match_location_sync_string = r"\b" + operator_pair["sync_location"] + r"\b"
            match_operator_sync_string = r"\b" + operator_pair["sync_operator"] + r"\b"
            match_location_async_string = (
                r"\b" + operator_pair["async_location"] + r"\b"
            )
            match_operator_async_string = (
                r"\b" + operator_pair["async_operator"] + r"\b"
            )

            filedata = re.sub(
                match_operator_sync_string, match_operator_async_string, filedata
            )
            filedata = re.sub(
                match_location_sync_string, match_location_async_string, filedata
            )

    # Write updated DAG to new file
    with open(async_filename, "w") as async_file:
        async_file.write(filedata)


def asyncronizer(dag_directory, output_dag_directory):
    file_list = os.listdir(dag_directory)
    for file in file_list:
        extension = file.split(".")[1]
        file_directory = os.path.join(dag_directory, file)
        newfile_name = "async_" + file
        newfile_path = os.path.join(output_dag_directory, newfile_name)
        if extension == ".py":
            asyncronize_dag(file_directory, newfile_path)
