  <img src="https://github.com/lluminov/Yeagle/blob/master/yarn-eagle.jpg" width="256" title="YARN-EAGLE">
</p>
<p align="center">
  Monitor and restart Spark streaming yarn apps automatically ..be a YARN-EAGLE :eagle:
</p>

# YARN-EAGLE
**Monitor and restart Spark streaming yarn apps automatically**

## Demo
![](YARN-EAGLE_demo.gif)
 <img src="https://github.com/vnayk7/YARN-EAGLE/blob/master/YARN-EAGLE_demo.png" title="YARN-EAGLE_demo">
## Motivation
When you have lots of yarn applications running on your hadoop cluster , it gets overwhelming to track all batch and streaming appications and
you start wishing if you had an app which could track and also help restart your Spark streaming applications .
YARN-EAGLE helps you to :
1. Track yarn applications ( Spark batch , Spark Streaming , MapReduce, Hive queries )
2. Easily onboard new applications with just a config file
3. Email notifications ( only when something is wrong ) with a intuitive tabular data to analyze
4. Restart Spark streaminig applications autonomously

## Prerequisites
Before we make YARN-EAGLE work for us let's see what is needed <br>
A user on Edge node of your hadoop cluster which :

   1. Can access the yarn api/cli commands. How do you test that ? <br>
     a. Log on to the Edge node with your user <br>
     b. Make sure you have the active kerberos tgt ( if your cluster is kerberized). Type below command and enter your AD password <br>
          `
        kinit
        `
     <br>c. Check if you can access Yarn applications
            `yarn application -list `
         <br>
         If the above command returns the list of Yarn apps running on your hadoop cluster , your user has the necessary priveleges :thumbsup: , else contact your hadoop admin to get necessary priveleges for the user <br>
    2. Can run Python 3.8 + (if not , no worries as we will have it installed later in our Quickstart steps) <br>
    3. Has priveleges to crontab or any scheduling tool like Airflow, Oozie etc on the edge node  <br>
    4. Has enough priveleges to send out mail using sendmail . A very good resource to check is [here](https://clients.javapipe.com/knowledgebase/132/How-to-Test-Sendmail-From-Command-Line-on-Linux.html) <br>


## Quick Installation steps - Monitoring

   1. Log on to the Hadoop cluster edge node with the user that fulfils the above [prerequisites](#prerequisites)
       Move to your home directory
       `cd ~`
   2. Clone the repository in the directory of your choice on the edge node with below command. Follow this document if you are a beginner [cloning a repo](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository)

      `git clone "https://github.com/vnayk7/YARN-EAGLE.git"`

   3. change the directory
      `cd yarn-eagle`

   4. Update the file prep-env.sh with your corporate proxy server <br>
       `vi bin/prep-env.sh` <br>
        Update the value for http_proxy with your corporate proxy server with port,  format proxy_server:port

   5. Run the below command to prepare python environment
      `sh bin/prep-env.sh`

   6. Prepare the config file . Pre-filled template provided <br>
      Update the config file with information as per your cluster and edge node . <br>
      i.  Section [yarn-apps] <br>
         &nbsp;&nbsp;This section of the config file takes in below parameters as comma separated list:<br>
             &nbsp;&nbsp;&nbsp;&nbsp;a. yarn app name <br>
             &nbsp;&nbsp;&nbsp;&nbsp;b. cron schedule (* * * * * for streaming app and actual values for batch app ) . <br> &nbsp;&nbsp;&nbsp;&nbsp; If you have never worked with crontab follow this link [cron](https://en.wikipedia.org/wiki/Cron#CRON_expression) <br>
             &nbsp;&nbsp;&nbsp;&nbsp;c. user that runs this app on yarn  <br>
             &nbsp;&nbsp;&nbsp;&nbsp;d. full path to spark submit file <br> &nbsp;&nbsp;&nbsp;&nbsp; ( This will be used by yarn-eagle to restart the Spark streaming job when it fails) <br>
             &nbsp;&nbsp;&nbsp;&nbsp;e. app type (streaming  or batch) <br>

       ii. Section [paths] <br>
              &nbsp;&nbsp;This section of the config file takes in the urls and paths as below  <br>
               &nbsp;&nbsp;&nbsp;&nbsp;a. rm_url_path_dev = This is the Resource Manager URL of your Hadoop cluster <br> &nbsp;&nbsp;&nbsp;&nbsp; ( input here your development cluster RM URL  ) <br>
               &nbsp;&nbsp;&nbsp;&nbsp;b. rm_url_path_prod = This is the Resource Manager URL of your Hadoop cluster <br> &nbsp;&nbsp;&nbsp;&nbsp; ( input here your production cluster RM URL  ) <br>
               &nbsp;&nbsp;&nbsp;&nbsp;c. apps_home = The location on your Edge node where all Spark application code is deployed  <br>
               &nbsp;&nbsp;&nbsp;&nbsp;d. app_path = The location on your Edge node where YARN-EAGLE is downloaded , for example if you downloaded yarn eagle to your home directory it would be /u/users/youruserid/yarn-eagle <br>
               &nbsp;&nbsp;&nbsp;&nbsp;e. data_path = The location on your Edge node where YARN-EAGLE writes data  ,for example if you downloaded yarn eagle to your home directory it would be /u/users/youruserid/yarn-eagle<br>
               &nbsp;&nbsp;&nbsp;&nbsp;f. html_path = The location on your Edge node where YARN-EAGLE writes the html file prefixed with output ,for example if you downloaded yarn eagle to your home directory it would be /u/users/youruserid/yarn-eagle/output/index.html <br>
               &nbsp;&nbsp;&nbsp;&nbsp;g. logfile_path = The location on your Edge node where YARN-EAGLE writes the log file prefixed with output , for example if you downloaded yarn eagle to your home directory it would be /u/users/youruserid/yarn-eagle/output/logfile.log <br>

        iii. Section [mail] <br>
               &nbsp;&nbsp;This section of the config file takes in the parameters for email generation <br>
                 &nbsp;&nbsp;&nbsp;&nbsp;a. from = Default from user on your domain which sends out email , update the value of yourdomain.com <br>
                 &nbsp;&nbsp;&nbsp;&nbsp;b. to = your corporate email <br>
                 &nbsp;&nbsp;&nbsp;&nbsp;c. cc = corporate email of cc to be sent <br>
                 &nbsp;&nbsp;&nbsp;&nbsp;d. msg = Mail subject , default value YARN-EAGLE status check <br>

        iv. Section [misc]    <br>
              &nbsp;&nbsp;This section of config file takes in misclaneous values like  <br>
                 &nbsp;&nbsp;&nbsp;&nbsp;a. tmzone = Your local time zone in format Country/City , default value is America/Toronto <br>
   6.  Test and run the monitoring script  <br>
         `sh monitor/run_monitor_command.sh` <br>
       The monitoring script produces the below two outputs  <br>
         &nbsp;&nbsp;a. An HTML email - <br>
           &nbsp;&nbsp;<b>HTML Email trigger logic </b>: If you have any of the yarn applications which are in the status FINISHED FAILED ( i.e State = FINISHED and FinalStatus = FAILED) , &nbsp;&nbsp;YARN-EAGLE sends out email notification to the email address mentioned in cluster.config file <br>
           Below is the sample output of the email that is sent out when YARN-EAGLE detects that any Streaming app has FINISHED FAILED

         &nbsp;&nbsp;b. Output Config files that are read by the  action script - <br>
         &nbsp;&nbsp;&nbsp;The output config file has the list of application names that need to be restarted as they were detected in the status FINISHED FAILED ( i.e State = FINISHED and FinalStatus = FAILED)


## Quick Installation steps - Autonomous Action

   As mentioned YARN-EAGLE has the ability to autonomously restart the Spark Streaming yarn applications when it detects any of them in the FINISHED FAILED status . <br>

## Prerequisites


   The autonomous action requires few mandatory steps before you test it out : <br>
a). You have sudo or direct access to the user which is running one of the Spark Streaming applications . This also means that you have sudo access to the user on your cluster which runs the spark-submit command for your Spark Streaming applications
            If you do not have access to this user , you can contact your Hadoop admin to get sudo access or let the admin run this script manually for testing <br>
b). You have an example Spark streaming Application which has a status FINISHED FAILED , i.e State = FINISHED and FinalStatus = FAILED <br>
     Once the above prereqs are fulfilled , you can test the autonomous action following instructions as below

Steps to test autonomous action

  1. Log in to the Edge node with one of the user that runs the Spark Streaming applications  <br>

  2. Move to directory where yarn-eagle is installed , for example if you downloaded yarn eagle in your home directory on edge node  <br>
     `cd ~/yarn-eagle` <br>
  3. Open the below file  <br>
      `vi ~/yarn-eagle/action/run_action_command.sh`  <br>
     Update the arguments
      path_where_yarn-eagle_is_downloaded and failed_Spark_stremaing_app_user.config <br>
  3. Test and run the action script  <br>
      `sh monitor/run_action_command.sh` <br>
     The above command will run the action script which further will restart the Spark streaming application taking the Application submit path from the config file .
     For multiple users this script can be easily wrapped in a shell script that loops over the diffferent users that run the Spark streaming apps.



## Tech/framework used
 #Python , #hadoop , #yarn


## About Me
 Its not about me ... its about us :) <br>
 Let's discuss on my [Linkedin Profile](https://ca.linkedin.com/in/vnaykb7), if you have any questions

## Licence
MIT
