#! /bin/sh

SCRIPTPATH=$(dirname $0)

LIB=$SCRIPTPATH"/../lib"
JARS=`ls $LIB/*.jar | tr '\n' ':'`
JAVA=java

case $1 in
   cassandra) MAIN=org.semagrow.sevod.scraper.cql.CassandraScraper    ;;
   rdfdump)   MAIN=org.semagrow.sevod.scraper.rdf.dump.RdfDumpScraper ;;
esac

if [ -e $MAIN ]
then
    echo "Usage:"
    echo "./sevod-scraper.sh rdfdump [dump_file] [endpoint_url] [-s|p|o|j] [output_file]"
    echo "./sevod-scraper.sh rdfdump [dump_file] [endpoint_url] [-s|p|o|j] [subjectTrieParameter] [objectTrieParameter] [output_file]"
    echo "./sevod-scraper.sh cassandra [cassandra-ip-address] [cassandra-port] [cassandra-keyspace] [base] [output_file]"
    exit
fi

shift
$JAVA -cp $JARS $MAIN $*
