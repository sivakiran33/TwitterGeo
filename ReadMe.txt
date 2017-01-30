Part A:

A Scala program for Word Cout, ScalaProgram.txt is present in the zip file.
The output file ScalaOutput.txt has outputs for different strings.
To run the program, go to hdn1001,
Run SPARK_REPL_OPTS="-XX:MaxPermSize=256m" spark-shell
and then copy the program in ScalaProgram.txt.
Change the inpstring for different input strings.

Part B:
A Java Program for Page Rank is named JavaProgram.java is present in the xip file.
The java file path is src/main/java/ttu/edu/JavaProgram.java
Compile using mvn package
execute using
YOUR_PROGRAM_JAR=src/main/java/ttu/edu/JavaProgram-1.0-0.9.0-incubating.jar
SPARK_SERVICE= local

Setting Environment Variables:
source /etc/spark/conf/spark-env.sh
CLASSPATH=/etc/hadoop/conf
CLASSPATH=$CLASSPATH:$HADOOP_HOME/*:$HADOOP_HOME/lib/*
CLASSPATH=$CLASSPATH:$HADOOP_HOME/../hadoop-mapreduce/*:$HADOOP_HOME/../hadoopmapreduce/
lib/*
CLASSPATH=$CLASSPATH:$HADOOP_HOME/../hadoop-yarn/*:$HADOOP_HOME/../hadoop-yarn/lib/*
CLASSPATH=$CLASSPATH:$HADOOP_HOME/../hadoop-hdfs/*:$HADOOP_HOME/../hadoop-hdfs/lib/*
CLASSPATH=$CLASSPATH:$SPARK_HOME/assembly/lib/*
CLASSPATH=$CLASSPATH:$YOUR_PROGRAM_JAR
CONFIG_OPTS="-Dspark.master=$SPARK_SERVICE -Dspark.jars=$YOUR_PROGRAM_JAR

Execution:

java -cp $CLASSPATH $CONFIG_OPTS ttu.edu.JavaProgram
$SPARK_SERVICE /CS5331_Examples/wiki-Vote.txt 5