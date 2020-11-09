from configparser import ConfigParser
from croniter import croniter
from datetime import datetime, timedelta
import datetime
import sys
import subprocess
import pandas as pd
import logging
import os
from pathlib import Path

pd.options.mode.chained_assignment = None
pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_colwidth', None)
import numpy as np

from pandas import json_normalize
import ast
import os
import json
import pytz

'''Reading the Config  file from command line argument'''
parser = ConfigParser()
pd.set_option('display.max_columns', None)
config_file = sys.argv[1]
parser.read(config_file)

all_apps = parser.get('yarn_apps', 'appname')
'''Printing the variables '''
rm_url_path_dev = parser.get('paths', 'rm_url_path_dev')
print("rm_url_path_dev is " + str(rm_url_path_dev))
rm_url_path_prod = parser.get('paths', 'rm_url_path_prod')
print("rm_url_path_prod is " + str(rm_url_path_prod))
app_path = parser.get('paths', 'app_path')
print("app_path is " + str(app_path))
data_path = parser.get('paths', 'data_path')
print("data_path is " + str(data_path))
html_path = parser.get('paths', 'html_path')
print("html_path is  " + str(html_path))
logfile_path = parser.get('paths', 'logfile_path')
print("logfile_path is  " + str(logfile_path))
mail_from = parser.get('mail', 'from')
print("mail_from is " + str(mail_from))
mail_to = parser.get('mail', 'to')
print("mail_to is " + str(mail_to))
mail_cc = parser.get('mail', 'cc')
print("mail_cc is " + str(mail_cc))
mail_msg = parser.get('mail', 'msg')
print("mail_msg is " + str(mail_msg))
tm_zone = parser.get('misc', 'tmzone')
print("tm_zone is " + str(tm_zone))

''' getting the current hostname '''
dev_string = 'tstr'
prod_string = 'oser'
if dev_string in os.uname().nodename:
    print("Working with Dev cluster")
    rm_url_path = rm_url_path_dev
    cluster = 'DEV'
elif prod_string in os.uname().nodename:
    print("Working with Prod Cluster")
    rm_url_path = rm_url_path_prod
    cluster = 'PROD'

''' Logging : Gets or creates a logger '''
logger = logging.getLogger(__name__)

''' setting the log level '''
logger.setLevel(logging.INFO)

''' defining file handler and setting the formatter '''
file_handler = logging.FileHandler(logfile_path, 'w+')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
logging.basicConfig(filemode='w')
file_handler.setFormatter(formatter)

''' adding file handler to logger '''
logger.addHandler(file_handler)
'''Creating DF for apps to be tracked from config file'''
list_apps = ast.literal_eval(all_apps)
logger.info(str(("List of Apps are : ", list_apps)))
df_apps = pd.DataFrame(list_apps, columns=['app_name', 'app_schedule', 'app_user', 'app_submit_file', 'app_type'])
logger.info(str(("rm url path is  : ", rm_url_path)))
''' creating the string for searching yarn apps '''
list_appnames = df_apps['app_name'].to_list()
strapp = 'Application-Id'
list_appnames1 = [strapp] + list_appnames
grepstrng = '|'.join(list_appnames1)
print(grepstrng)

''' preparing the bash commands for yarn '''
yarn_cmd = ['yarn', 'application', '-list', '-appStates', 'RUNNING' + ' ' + 'FINISHED' + ' ' + 'KILLED']
tail_cmd = ['tail', '-n', '+2']
grep_cmd = ['grep', '-P', grepstrng]
df = pd.DataFrame()
o1 = subprocess.run(yarn_cmd, stdout=subprocess.PIPE)
o2 = subprocess.run(tail_cmd, input=o1.stdout, stdout=subprocess.PIPE)
o3 = subprocess.run(grep_cmd, input=o2.stdout, stdout=subprocess.PIPE)
if sys.version_info[0] < 3:
    from io import StringIO
else:
    from io import StringIO
b = StringIO(o3.stdout.decode('utf-8'))
''' creating a dataframe from the StringIO Object '''
df = pd.read_csv(b, sep="\t", skipinitialspace=True)
app_ids = df['Application-Id'].tolist()

data_out = []
for id in app_ids:
    print(id)
    proc = subprocess.run(["GET",
                           rm_url_path + id],
                          stdout=subprocess.PIPE, encoding='utf-8')
    proc_out = proc.stdout
    print(proc_out)
    df2 = pd.DataFrame([json.loads(proc_out)])
    df3 = json_normalize(df2['app'])
    data_out.append(df3)
if len(data_out) == 0:
   print("No data .. looks like no apps running on cluster that we are tracking ")
   sys.exit()
data_out = pd.concat(data_out)
data_out = data_out.drop(
    ['preemptedResourceMB', 'preemptedResourceVCores', 'numNonAMContainerPreempted',
     'numAMContainerPreempted', 'amNodeLabelExpression'], axis=1)
data_out = data_out[
    ['name', 'id', 'user', 'state', 'finalStatus', 'startedTime', 'finishedTime', 'elapsedTime', 'queue', 'progress',
     'trackingUI', 'trackingUrl', 'diagnostics', 'clusterId', 'applicationType', 'applicationTags', 'priority',
     'amContainerLogs', 'amHostHttpAddress', 'allocatedMB', 'allocatedVCores', 'runningContainers', 'memorySeconds',
     'vcoreSeconds', 'queueUsagePercentage', 'clusterUsagePercentage', 'logAggregationStatus', 'unmanagedApplication']]
data_out.columns = ['app_name', 'app_id', 'user', 'state', 'final_status', 'start_time', 'finish_time', 'elapsed_time',
                    'queue', 'progress', 'trackingUI', 'tracking_url', 'diagnostics', 'cluster_id', 'application_type',
                    'application_tags', 'priority', 'am_container_log', 'am_host_http_address', 'allocated_mb',
                    'allocated_vcores', 'running_containers', 'memory_seconds', 'vcor_seconds', 'queue_usage_percen',
                    'cluster_usage_percent', 'log_aggregation_status', 'unmanaged_application']
data_sched = pd.merge(data_out, df_apps, how='left', on='app_name')
logger.info(str(("Apps data from config file is : ", df_apps)))
# logger.info(str(("Data from Yarn before join is : ",data_out)))
app_start_times = data_sched.loc[data_sched['app_schedule'] == "* * * * *"]['start_time'].tolist()

''' function to calculate the times '''

def get_dt(t):
    to = pytz.timezone(tm_zone)
    start_time_dt = datetime.datetime.fromtimestamp(int(str(t['start_time'])[0:10]))
    start_time_dt = start_time_dt.replace(tzinfo=pytz.utc).astimezone(to)
    finish_time_dt = "Not Finished" if t['finish_time'] == 0 else datetime.datetime.fromtimestamp(
        int(str(t['finish_time'])[0:10])).replace(tzinfo=pytz.utc).astimezone(to)
    sched_time_prev = datetime.datetime.fromtimestamp(
        croniter(t['app_schedule'], start_time_dt + timedelta(hours=0)).get_prev()).replace(tzinfo=pytz.utc).astimezone(
        to)
    sched_time_next = datetime.datetime.fromtimestamp(
        croniter(t['app_schedule'], start_time_dt + timedelta(hours=0)).get_next()).replace(tzinfo=pytz.utc).astimezone(
        to)
    days, minutes = divmod(t['elapsed_time'] / 1000, 60 * 60 * 24)
    hours, remainder = divmod(minutes, 60 * 60)
    minutes, seconds = divmod(remainder, 60)
    total_run_time = '{:0.0f}d:{:0.0f}h:{:0.0f}m:{:0.0f}s'.format(days, hours, minutes, seconds)
    return start_time_dt, finish_time_dt, sched_time_prev, sched_time_next, total_run_time


''' function that calculates the complex statuses in single column eagle_check '''


def eagle_stat(s):
    to = pytz.timezone(tm_zone)
    start_duration_chk = abs(s['sched_time_prev'] - s['start_time_dt']).total_seconds()
    if (s['app_type'] == "batch") and (
            s['finish_time_dt'] == "Not Finished") and (s['state'] == "RUNNING") and (
            s['final_status'] == "UNDEFINED") and (
            1 <= start_duration_chk <= 600) and (
            s['sched_time_prev'] < s['start_time_dt'] < s['sched_time_next']):
        eagle_check = 'Batch App RUNNING on schedule,sd lt 10min'
    elif (s['app_type'] == "batch") and (
            s['finish_time_dt'] == "Not Finished") and (s['state'] == "RUNNING") and (
            s['final_status'] == "UNDEFINED") and (start_duration_chk > 600) and (
            s['sched_time_prev'] < s['start_time_dt'] < s['sched_time_next']):
        eagle_check = 'Batch App RUNNING on schedule,sd mt 10min'
    elif (s['app_type'] == "batch") and (
            s['finish_time_dt'] != "Not Finished") and (s['state'] == "FINISHED") and (
            s['final_status'] == "SUCCEEDED") and (s['sched_time_prev'] < s['start_time_dt'] < s['sched_time_next']):
        eagle_check = 'Batch App ' + s['final_status'] + '  on schedule'
    elif (s['app_type'] == "batch") and (
            s['finish_time_dt'] != "Not Finished") and (s['state'] == "FINISHED") and (
            s['final_status'] == "FAILED") and (s['sched_time_prev'] < s['start_time_dt'] < s['sched_time_next']):
        eagle_check = 'Batch App ' + s['final_status']
    elif (s['app_type'] == "batch") and (
            s['finish_time_dt'] != "Not Finished") and (s['state'] == "KILLED") and (
            s['final_status'] == "KILLED") and (s['sched_time_prev'] < s['start_time_dt'] < s['sched_time_next']):
        eagle_check = 'Batch App ' + s['final_status']
    elif (s['app_type'] == "streaming") and (
            s['finish_time_dt'] == "Not Finished") and (s['state'] == "RUNNING") and (s['final_status'] == "UNDEFINED"):
        eagle_check = 'Streaming App RUNNING'
    elif (s['app_type'] == "streaming") and (
            s['finish_time_dt'] != "Not Finished") and (s['state'] == "KILLED") and (s['final_status'] == "KILLED"):
        eagle_check = 'Streaming App KILLED'
    elif (s['app_type'] == "streaming") and (
            s['finish_time_dt'] != "Not Finished") and (s['state'] == "FINISHED") and (s['final_status'] == "FAILED"):
        eagle_check = 'Streaming App FAILED'
    else:
        eagle_check = 'Eagle check not available'
    return eagle_check


data_sched["start_time_dt"], data_sched["finish_time_dt"], data_sched["sched_time_prev"], data_sched["sched_time_next"], \
data_sched["total_run_time"] = zip(*data_sched.apply(get_dt, axis=1))
data_sched["eagle_check"] = data_sched.apply(eagle_stat, axis=1)
data_sched = data_sched.sort_values(by='app_name')

''' filtering the dataframe for columns of our interest '''
data_sched_f = data_sched.loc[:,
               ['app_name', 'app_id', 'user', 'trackingUI', 'tracking_url', 'app_schedule', 'app_submit_file',
                'app_type', 'start_time_dt',
                'finish_time_dt', 'sched_time_prev', 'sched_time_next', 'total_run_time', 'eagle_check']]
''' Finding the apps that we are tracking but are not running '''
''' Streaming Apps running '''
df_strm_app_running = data_sched_f.loc[data_sched_f['eagle_check'] == 'Streaming App RUNNING']
df_strm_app_running.drop(['sched_time_prev', 'sched_time_next'], axis=1, inplace=True)
''' Streaming Apps finished '''
df_strm_app_finished = data_sched_f.loc[
    (data_sched_f['eagle_check'] == 'Streaming App FAILED') | (data_sched_f['eagle_check'] == 'Streaming App KILLED')]
''' Streaming Apps finshed get only the most recent record for each Finished App '''

df_strm_app_finished["finish_time_dt_num"] = pd.to_datetime(df_strm_app_finished["finish_time_dt"]).astype(
    int) / 10 ** 9
df_strm_app_finished["rank"] = df_strm_app_finished.groupby(['app_name', 'eagle_check'])['finish_time_dt_num'].rank(
    method="first",
    ascending=False)
logger.info(str(("Streaming apps finished are : ", df_strm_app_finished)))
df_strm_app_finished = df_strm_app_finished.loc[df_strm_app_finished['rank'] == 1.0].drop(
    ['finish_time_dt_num', 'rank', 'sched_time_prev', 'sched_time_next'], axis=1)
''' Batch Apps running '''
df_batch_app_running = data_sched_f[data_sched_f['eagle_check'].str.contains('Batch App Running') == True]
'''  Batch Apps Finished '''
df_batch_app_finished = data_sched_f[
    data_sched_f['eagle_check'].str.contains('Batch App SUCCEEDED') == True].sort_values(['app_name', 'start_time_dt'],
                                                                                         ascending=[True, True])

df_strm_app_running_list = df_strm_app_running.app_name.unique().tolist()  # All streaming app names from yarn that we are tracking as a list
list_strm_appnames = df_apps.loc[df_apps['app_type'] == 'streaming'][
    'app_name'].unique().tolist()  # Streaming Apps from our list of apps to be tracked
strm_app_not_run = list(set(list_strm_appnames) - set(df_strm_app_running_list))  # Streaming Apps that are not running
logger.info(str(("Streaming apps not running are : ", strm_app_not_run)))
strm_app_failed = df_strm_app_finished.loc[df_strm_app_finished['eagle_check'] == 'Streaming App FAILED'][
    'app_name'].unique().tolist()
logger.info(str(("Streaming apps failed  are : ", strm_app_failed)))
df_strm_app_norun = df_apps[df_apps['app_name'].isin(strm_app_not_run)]
df_strm_app_torestart = df_strm_app_norun[df_strm_app_norun['app_name'].isin(strm_app_failed)]

df_strm_app_torestart = df_strm_app_torestart.loc[:, ['app_name', 'app_user', 'app_submit_file', 'app_type']]
logger.info(str(("Streaming apps to be restarted are : ", df_strm_app_torestart)))
strm_app_torestart_gpd = df_strm_app_torestart.groupby('app_user')['app_name'].apply(list).reset_index(name='apps')
''' creating the html file tp be written as report '''
html = "Streaming Apps Running are :" + "\n\n\n" + df_strm_app_running.to_html() + "<br><br>" + "Streaming Apps Finished Failed or Killed are :" + "\n\n\n" + df_strm_app_finished.to_html() + "<br><br>" + "Streaming Apps to be Restarted are :" + "\n" + df_strm_app_torestart.to_html() + "<br><br>" + "Batch Apps Running are :" + "\n\n\n" + df_batch_app_running.to_html() + "<br><br>" + "Batch Apps Finished are :" + df_batch_app_finished.to_html()

''' Adding the colored labels to the html file '''
text_file_w = open(html_path, "w")
text_file_w.write(html)
text_file_w.close()
text_file_r = open(html_path, "rt")
tfr = text_file_r.read()
tfr = tfr.replace('<td>Streaming App RUNNING', '<td bgcolor="dodgerblue"<td>Streaming App RUNNING')
tfr = tfr.replace('<td>Streaming App FAILED', '<td bgcolor="red"<td>Streaming App FAILED')
tfr = tfr.replace('<td>Streaming App KILLED', '<td bgcolor="red"<td>Streaming App KILLED')
tfr = tfr.replace('<td>Batch App SUCCEEDED  on schedule', '<td bgcolor="limegreen"<td>Batch App SUCCEEDED  on schedule')
tfr = tfr.replace('<td>Batch App RUNNING on schedule,sd lt 10min',
                  '<td bgcolor="yellow"<td>Batch App RUNNING on schedule,sd lt 10min')
tfr = tfr.replace('<td>Batch App RUNNING on schedule,sd mt 10min',
                  '<td bgcolor="yellow"<td>Batch App RUNNING on schedule,sd mt 10min')
tfr = tfr.replace('<td>Batch App KILLED', '<td bgcolor="red"<td>Batch App KILLED')
tfr = tfr.replace('<td>Batch App FAILED', '<td bgcolor="red"<td>Batch App FAILED')
text_file_r.close()
text_file_w = open(html_path, "wt")
''' overwrite the input file with the resulting data '''
text_file_w.write(tfr)
''' close the file '''
text_file_w.close()

'''  Function to send email '''
import subprocess
from email.message import EmailMessage


def sendEmail(from_addr, to_addrs, cc_addrs, msg_subject):
    with open(html_path) as fp:
        # Create a text/plain message
        msg = EmailMessage()
        msg.set_content(fp.read(), 'html')
        msg['From'] = from_addr
        msg['To'] = to_addrs
        msg['Cc'] = cc_addrs
        msg['Subject'] = msg_subject
        sendmail_location = "/usr/sbin/sendmail"
        subprocess.run([sendmail_location, "-t", "-oi"], input=msg.as_bytes())


''' notification email is sent only if the dt_to_be_restarted df is not empty  '''
if not df_strm_app_torestart.empty:
    logger.info("Looks like we might have to restart few apps ")
    logger.info(str(("List of Apps to be restarted are : ", df_strm_app_torestart)))
    sendEmail(mail_from, mail_to, mail_cc, mail_msg + " " + cluster)
    data_path=Path(data_path)
    for i, g in strm_app_torestart_gpd.groupby('app_user'):  # iterate through unique values in app_user
        file_name = f'{i}.config'  # create the empty content string
        file_path = data_path / "action" / file_name
        logger.info(str(("creating & writing the file  : ",file_path)))
        with open(file_path, 'w') as fp:  # open the file
            #fp.write(str(g.apps.replace(regex='(d)', value='apps=')))  # write content to file
            h=g.apps  # write content to file
            fp.write(str(h))
            #fp.write(str(g.apps).replace('\n.*', ''))  # write content to file
            logger.info(str(("Writing the config file  : ",file_name)))
            fp.close()
            os.chmod(file_path, 0o777)
