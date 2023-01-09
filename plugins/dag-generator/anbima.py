import json
import os
import shutil
import fileinput

config_filepath = "plugins/dag-config/"
dag_template_filename = "plugins/templates/anbima.py"

for filename in os.listdir(config_filepath):
    with open(config_filepath + filename) as f:
        config = json.load(f)
        new_filename = "dags/" + config["DagId"] + ".py"
        shutil.copyfile(dag_template_filename, new_filename)

        with fileinput.input(new_filename, inplace=True) as file:
            for line in file:
                new_line = line.replace("dag_id", "'" + config["DagId"] + "'").replace(
                    "endpointtoreplace", config["endpoint"]
                )
                print(new_line,end='')
        