#! /bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

LIB="$SCRIPTPATH/../lib"
JARS=`ls $LIB | grep *.jar | tr '\n' ':'`
JAVA=java

case $1 in
   cassandra) MAIN=org.semagrow.sevod.scraper.cql.CassandraScraper
   rdfdump) MAIN=org.semagrow.sevod.scraper.rdf.dump.RdfDumpScraper
esac

shift
$JAVA -cp $JARS $MAIN $*