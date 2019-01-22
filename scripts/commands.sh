#!/bin/bash

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\data\Santa Clara"
docker cp ..\data\ "cloudera_stack:/data/"

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\data\Hong Kong"
docker cp ..\data\ "cloudera_stack:/data/"

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\data\London"
docker cp ..\data\ "cloudera_stack:/data/"

docker exec -it cloudera_stack hadoop fs -put /data/*.csv  /user/root/

#hadoop fs -put /data/*.csv  /user/root/

#beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p pass

