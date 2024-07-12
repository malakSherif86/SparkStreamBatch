#WRITE THIS SCRIPT IN crontab -e , also make sure to edit ur path
30 * * * * export PATH=/usr/bin:/bin:/opt/spark2/bin:/opt/hadoop/bin && spark-submit /data/sparkJobs/sparkJob1.py >> /data/sparkJobs/SparkJob1Logs.log 2>&1
# schedules the job to run at 30 minutes past every hour.
