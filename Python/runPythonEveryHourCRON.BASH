#WRITE THIS SCRIPT IN crontab -e , also make sure to edit ur path
0 * * * * /usr/bin/python3 /data/movedata_test/movedata.py >> /data/movedata_test/movedata.log 2>&1
