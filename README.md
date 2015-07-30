# metadatagen #

a tool to create void/sevod metadata for SemaGrow.

## how to build ##

Build with maven using the following command:
```
mvn clean install
```

## usage ##

Usage: **./run_metadatagen.sh  [dump_file] [endpoint_url] [-s|p|o] [subjectTrieParameter] [objectTrieParameter] [output_file]**

* **[dump_file]** - data-dump file path. It was tested mainly for NQUADS and NTriples input. 
* **[endpoint_url]** - the url endpoint of the dataset. Used for annotation purposes (the tool relies exclusively on the dump file).
* **[-s|p|o|v]** - s: generate metadata for subject URI prefixes, p: generate metadata for properties and o: generate metadata for object URI prefixes, v: generate subject and object vocabularies for each predicate.
* **[subjectTrieParameter]**, **[objectTrieParameter]** are parameters for the subject and object path tries. Use 0 for the default values.
* **[output_file]** - output file path. Metadata are exported in n3 format.

The dump file should be in NQUADS or NTRIPLES format, the triples should be grouped by predicate URI.
If you want to convert the data dump in one of the two formats you could use a tool such as rdf2rdf (http://www.l3s.de/~minack/rdf2rdf/).
Also, if the dump is not sorted by predicate, you could use the sort command (e.g. sort -k 2 unsorted.nt > sorted.nt)

## Examples: ##

Generates metadata for properties (VoID metadata):
```
./run_metadatagen.sh [path-to-dump-file] [endpoint-url] -p 0 0 output.void.n3
```
Generates metadata for properties, with object and subject vocabularies:
```
./run_metadatagen.sh [path-to-dump-file] [endpoint-url] -pv 0 0 output.void.n3
```
Generates metadata for subject prefixes and properties:
```
./run_metadatagen.sh [path-to-dump-file] [endpoint-url] -sp 0 0 output.svd.n3
```
Generates metadata for subject and object prefixes and properties (with subj.obj. vocabularies):
```
./run_metadatagen.sh [path-to-dump-file] [endpoint-url] -spov 0 0 output.svd.n3
```