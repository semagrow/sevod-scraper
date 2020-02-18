#! /bin/sh

SCRIPTPATH=$(dirname $0)

LIB=$SCRIPTPATH"/../lib"
JARS=`ls $LIB/*.jar | tr '\n' ':'`
JAVA=java

case $1 in
   cassandra) MAIN=org.semagrow.sevod.scraper.cql.CassandraScraper    ;;
   rdfdump)   MAIN=org.semagrow.sevod.scraper.rdf.dump.RdfDumpScraper ;;
   sparql)    MAIN=org.semagrow.sevod.scraper.sparql.SparqlScraper ;;
esac

if [ -e $MAIN ]
then
    echo "Usage:"
    echo "$0 rdfdump [dump_file] [endpoint_url] [-s|p|o|j] [output_file]"
    echo "$0 rdfdump [dump_file] [endpoint_url] [-s|p|o|j] [subjectTrieParameter] [objectTrieParameter] [output_file]"
    echo "$0 rdfdump [dump_file] [endpoint_url] [known_prefixes_path] [output_file]"
    echo "$0 sparql [endpoint_url] [main_graph] [known_prefixes_path] [output_file]"
    echo "$0 cassandra [cassandra-ip-address] [cassandra-port] [cassandra-keyspace] [base] [output_file]"
    exit
fi

shift
$JAVA -cp $JARS $MAIN $*
