# !/bin/sh

mainclass=eu.semagrow.stack.metadatagen.Main

classpath=target/metadatagen-1.0-SNAPSHOT.jar

for jar in lib/*.jar; do classpath=$classpath:$jar; done

# run main class with classpath setting
java -cp $classpath -Xmx6g $mainclass $* 

