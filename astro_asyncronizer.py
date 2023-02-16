import json
import os

operator_pairs = json.loads("operator_pairs.json")

def asyncronize_dag(filename, async_filename):
  # Read file data to memory
  with open(filename, 'r') as file:
    filedata = file.read()

  # Replace operators with async equivalents
  # TODO Identify and address edge cases (Where else might operator name show up? Case Sensitivity?)
  for operator_pair in operator_pairs['operator_pairs']:
    filedata = filedata.replace(operator_pair['sync_location'], operator_pair['async_location'])
    filedata = filedata.replace(operator_pair['sync_operator'], operator_pair['async_operator'])

  # Write updated DAG to new file
  with open(async_filename, 'w') as async_file:
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