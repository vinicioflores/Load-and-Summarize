

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\data\Santa Clara"
docker cp . "cloudera_stack:/data/"

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\data\Hong Kong"
docker cp . "cloudera_stack:/data/"

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\data\London"
docker cp . "cloudera_stack:/data/"

REM docker exec -it cloudera_stack hadoop fs -put /data/*.csv  /user/root/

REM hadoop fs -put /data/*.csv  /user/root/

REM netstat -ano | findstr :60030 | findstr LISTENING

REM beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p pass

REM vi /etc/yum.conf

REM add this line to conf file 
REM # The proxy server - proxy server:port number
REM proxy=http://proxy-chain.intel.com:911

REM yum install hive-webhcat-server

REM  sudo service hive-webhcat-server start

cd "C:\Users\viniciof\documents\Load_and_Summarize\Load-and-Summarize\scripts\"