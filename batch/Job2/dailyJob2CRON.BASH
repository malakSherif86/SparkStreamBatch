#WRITE THIS SCRIPT IN crontab -e , also make sure to edit ur path
0 3 * * *  export PATH=/usr/bin:/bin:/opt/spark2/bin:/opt/hadoop/bin && spark-submit /data/sparkJobs/sparkJob2.py >> /data/sparkJobs/SparkJob2Logs.log 2>&1

# this job runs everyday at 3 Am
