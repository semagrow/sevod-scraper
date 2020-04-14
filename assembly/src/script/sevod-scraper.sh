#! /bin/sh

SCRIPTPATH=$(dirname $0)

LIB=$SCRIPTPATH"/../lib"
JARS=`ls $LIB/*.jar | tr '\n' ':'`
JAVA=java
MAIN=org.semagrow.sevod.scraper.cli.Main

$JAVA -cp $JARS $MAIN $*
