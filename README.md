# sevod-scraper #

A tool to create dataset metadata for Semagrow.

## Build ##

Build with maven using the following command:
```
mvn clean package
```
Extract the *.tar.gz file that is contained in the assembly/target directory to run sevod scraper.
```
cd assembly/target
tar xzvf sevod-scraper-*-dist.tar.gz
cd bin
./sevod-scraper.sh
```

## Usage ##

```
usage: sevod_scraper.sh [OPTIONS]...
```

Option                  | Description
:-----------------------|:-----------------------------------------------------
 &nbsp;&nbsp;&nbsp;&nbsp; `--rdfdump`    |  input is a RDF file in ntriples format
 &nbsp;&nbsp;&nbsp;&nbsp; `--geordfdump` |  input is a geospatial RDF file in ntriples format
 &nbsp;&nbsp;&nbsp;&nbsp; `--cassandra`  |  input is a Cassandra keyspace
 &nbsp;&nbsp;&nbsp;&nbsp; `--sparql`     |  input is a SPARQL endpoint
 `-i,--input <arg>    `  |  input
 `-o,--output <arg>   `  |  output metadata file in turtle format
 `-e,--endpoint <arg> `  |  SPARQL endpoint URL (used for annotation only)
 `-p,--prefixes <arg> `  |  List of known URI prefixes (comma-separated)
 `-g,--graph <arg>    `  |  Graph (only for SPARQL endpoint)
 `-t,--extentType <arg>` |  Extent type (mbb-union-qtN, for geospatial RDF)
 `-P,--polygon <arg>  `  |  Known bounding polygon (for geospatial RDF files)
 `-n,--namespace <arg>`  |  Namespace for URI mappings (only for cassandra)
 `-h,--help           `  |  print a help message

## Examples ##

Here we will present some examples of sevod-scraper usages for several input scenarios.

#### RDF dump file

Suppose that your dataset is stored in input.nt and its SPARQL endpoint is http://localhost:8080/sparql.
To extract its metadata for Semagrow, issue the following command:

```
./sevod-scraper.sh --rdfdump -i input.nt -e http://localhost:8080/sparql -o output.ttl
```

The dump file should be in NTRIPLES format. Metadata are exported in TTL format.
The SPARQL endpoint will be not consulted, but it is going to be used only for annotation purposes.

Semagrow uses the URI prefixes for each dataset for performing a more refined source selection.
Therefore, if you know that all subject and object URIs of have one (or more) common prefixes,
 you can annotate this fact using the following command (the prefixes are comma-separated):

```
./sevod-scraper.sh --rdfdump -i input.nt -p http://semagrow.eu/p/,http://semagrow.eu/q/,http://www.example.org/ -e http://localhost:8080/sparql -o output.ttl
```

Otherwise, the output will contain metadata about subject and object authorities. 

#### Geospatial RDF file

If your RDF dump file is a geospatial dataset (i.e., it contains the geo:asWKT predicate),
you can issue the following command:

 ```
 ./sevod-scraper.sh --geordfdump -i input.nt -t EXTENT_TYPE -e http://localhost:8080/sparql -o output.ttl
 ```
The functionality is the same as for `rdfdump` option, except that the output file will contain
an annotation of the bounding polygon that contains of all WKT literals of the dataset.

Extent types can be one of the following:
* `mbb`, which exports the Minimum Bounding Box of all WKT literals
* `union`, which expors the spatial union of all WKT literals
* `qtN`, where `N` is an integer, which calculates an approximation of the union of all WKT literals
  using a [quadtree](https://en.wikipedia.org/wiki/Quadtree) of height `N`.  

If wou want to provide a manual spatial extent annotation, you can issue the following command:

```
./sevod-scraper.sh --geordfdump -i input.nt -P POLYGON_IN_WKT -e http://localhost:8080/sparql -o output.ttl
```

The dump file should be in NTRIPLES format. Metadata are exported in TTL format.
The SPARQL endpoint will be not consulted, but it is going to be used only for annotation purposes.

#### Cassandra keyspace

In order to create Semagrow metadata from a Cassandra keyspace, issue the following command:

```
./sevod-scraper.sh --cassandra -i IP_ADDRESS:PORT/KEYSPACE -n NAMESPACE_URI -o output.ttl
```

The Cassandra keyspace is defined using the following parameters:
IP_ADDRESS is the cassandra server ip address,
PORT is the cassandra server port,
KEYSPACE is the cassandra relevant keyspace.
Moreover, NAMESPACE_URI us a base string to generate URI predicates.
At this moment, it shoud contain the  "cassandra" substring.

Metadata are exported in TTL format.

#### SPARQL endpoint

In order to create Semagrow metadata from a SPARQL endpoint, issue the following command:

```
./sevod-scraper.sh --sparql -i http://localhost:8890/sparql -o output.ttl
```

Since many SPARQL endpoints (such as Virtuoso) contain additional system graphs,
you can specify the graph in which the dataset is contained. Example:

 ```
 ./sevod-scraper.sh --sparql -i http://localhost:8890/sparql -g http://localhost:8890/DAV  -o output.ttl
 ```
Metadata are exported in TTL format.

## Spark mode

In order to extract sevod metadata from a dump file using spark, issue the following command:
```
mvn clean package -P spark
```
Use spark-submit script (see https://spark.apache.org/docs/latest/submitting-applications.html) 
to submit your application in an existing spark cluster. 

* the application jar can be found in assembly/target/sevod-scraper-*-spark-onejar-jar-with-dependencies.jar
* the main class is org.semagrow.sevod.scraper.Scraper
* the arguments are the same as in the rdfdump mode.

For example configuration using docker containers cf. rdfdump-spark/src/main/resources
