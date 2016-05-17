#!/bin/bash

if [ "$1" == "cassandra" ]
then
	shift
	mvn exec:java -Dexec.mainClass="eu.semagrow.scraper.cassandra.CassandraScraper" -Dexec.args="$*"
fi

if [ "$1" == "rdfdump" ]
then
        shift
	mvn exec:java -Dexec.mainClass="eu.semagrow.scraper.rdf.RdfDumpScraper" -Dexec.args="$*"
fi

