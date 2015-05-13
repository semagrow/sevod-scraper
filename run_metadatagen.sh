# !/bin/sh

mainclass=eu.semagrow.stack.metadatagen.VoidGenerator

classpath=target/semagrow-stack-voidgenerator-1.0-SNAPSHOT.jar

for jar in lib/*.jar; do classpath=$classpath:$jar; done

# run main class with classpath setting
/usr/lib/jvm/java-1.7.0-openjdk-amd64/bin/java -cp $classpath -Xmx2g $mainclass $* 

