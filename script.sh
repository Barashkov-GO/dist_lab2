mvn package
export HADOOP_CLASSPATH=target/hadoop-examples-1.0-SNAPSHOT.jar
hadoop FlightsApp 664600583_T_ONTIME_sample.csv L_AIRPORT_ID.csv output4


