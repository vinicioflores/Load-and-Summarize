FROM cloudera/quickstart

COPY cloudera-scm-server /etc/default

RUN export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
RUN export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
RUN export HADOOP_CLASSPATH=$(/usr/bin/hadoop classpath)

