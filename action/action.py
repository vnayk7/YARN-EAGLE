import sys
import subprocess
import pandas as pd
import os
from pathlib import Path
import time
import datetime
import ast
from configparser import ConfigParser

'''Reading the Config  file from command line argument'''
parser = ConfigParser()
pd.set_option('display.max_columns', None)
config_file = sys.argv[1]
parser.read(config_file)
'''Printing the variables '''

data_path = parser.get('paths', 'data_path')
action_path = data_path + "/" + "action"
print("action_path is " + str(action_path))
apps_home_path = parser.get('paths', 'apps_home')
print("apps_home_path is " + str(apps_home_path))

'''Creating DF for apps to be tracked from config file'''
all_apps = parser.get('yarn_apps', 'appname')
list_apps = ast.literal_eval(all_apps)
# print("List of Apps are : " + str(list_apps))
df_apps = pd.DataFrame(list_apps, columns=['app_name', 'app_schedule', 'app_user', 'app_submit_file', 'app_type'])
print("df_apps are  " + str(df_apps))

filename = sys.argv[2]
a_path = Path(action_path)
modTimeEpoc = os.path.getmtime(action_path + "/" + filename)
currTimeEpoc = ts = datetime.datetime.now().timestamp()
td = round(round(currTimeEpoc - modTimeEpoc) / 60)

if td <= 5:
    print("Input config file is fresh ( " + str(td) + " minutes old ). Proceed to Action !!")
    app_home = Path(apps_home_path)
    a_path = Path(action_path)
    print("changing to the action_path directory" )
    os.chdir(a_path)
    print("current directory is " + str(Path.cwd()))
    with open(filename) as f:
        Line = f.readline().strip('\n').split('    ')[1].replace('[', '').replace(']', '').split(', ')
        grepstrng = '.+?(?=bin)'
        for x in Line:
            ''' preparing the bash commands for yarn '''
            print("Working on app name : " + x)
            ''' Finding the spark-submit file for current appname'''
            app_submit_file = df_apps.loc[df_apps['app_name'] == x]['app_submit_file'].values[0]
            print("app_submit_file for " + str(x) + "is :" + str(app_submit_file))
            # yarn_cmd = ['find', app_home, '-type', 'f', '-name', '*.sh']
            # grep_cmd_exe = ['xargs', 'grep', '-ril', str(x)]
            echo_cmd = ['echo', app_submit_file]
            grep_cmd_home = ['grep', '-oP', grepstrng]
            o1 = subprocess.run(echo_cmd, stdout=subprocess.PIPE)
            o2 = subprocess.run(grep_cmd_home, input=o1.stdout, stdout=subprocess.PIPE)
            # o3 = subprocess.run(grep_cmd_home, input=o2.stdout, stdout=subprocess.PIPE)
            # exe_file = str(o2.stdout).split("'")[1].strip('\\n')
            # print("Spark-submit file for appname " + str(x) + " is " + str(exe_file))
            exe_file_home = str(o2.stdout).split("'")[1].strip('\\n')
            print("Home directory for appname " + str(x) + " is " + str(exe_file_home))
            print("*******************************************************")
            print("Running the Spark Submit script for App name : " + str(x))
            print("*******************************************************")
            os.chdir(exe_file_home)
            subprocess.call(app_submit_file)
else:
    print("Looks like input config is too old( " + str(td) + " minutes old )  Aborting action !!")
