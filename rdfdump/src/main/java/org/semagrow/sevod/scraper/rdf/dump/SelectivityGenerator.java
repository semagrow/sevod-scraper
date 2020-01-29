package org.semagrow.sevod.scraper.rdf.dump;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.sail.nativerdf.NativeStore;
import org.semagrow.sevod.commons.vocabulary.SEVOD;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by antonis on 24/7/2017.
 */
public class SelectivityGenerator {

    final private Logger log = Logger.getLogger(SelectivityGenerator.class);

    private Map<URI, Resource> propertyPartitionMap = null;

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    final private Map<Pair<URI, URI>, Long> starJoins = new HashMap();
    final private Map<Pair<URI, URI>, Long> pathJoins = new HashMap();
    final private Map<Pair<URI, URI>, Long> sinkJoins = new HashMap();

    int batch_size = 10;

    public SelectivityGenerator(Map<URI, Resource> propertyPartitionMap) {
        this.propertyPartitionMap = propertyPartitionMap;
        log.debug("Found " + propertyPartitionMap.keySet().size() + " properties.");
    }

    public void calculateSelectivities(String endpoint) throws Exception {
        Repository repo = new SPARQLRepository(endpoint);
        repo.initialize();
        RepositoryConnection conn = repo.getConnection();
        try {
            calculateSelectivitiesInternalBatch(conn);
        }
        finally {
            conn.close();
        }
    }

    public void calculateSelectivities(final RDFFormat format, final File file) throws Exception {

        File dataDir = new File("/tmp/nativestore/");
        Repository repo = new SailRepository(new NativeStore(dataDir));
        repo.initialize();
        RepositoryConnection conn = repo.getConnection();
        try {
            conn.add(file, "", format);
            calculateSelectivitiesInternal(conn);
        }
        finally {
            conn.close();
        }
    }

    private void calculateSelectivitiesInternalBatch(RepositoryConnection conn) throws Exception {

        List<List<Pair<URI, URI>>> list = new ArrayList<>();
        List<Pair<URI, URI>> batch = new ArrayList<>();
        int count = 0;

        for (URI p1 : propertyPartitionMap.keySet()) {
            for (URI p2 : propertyPartitionMap.keySet()) {
                if (!(p1.equals(p2))) {
                    batch.add(Pair.of(p1, p2));
                    count++;
                    if (count < batch_size) {
                        list.add(batch);
                        batch = new ArrayList<>();
                    }
                }
            }
        }

        for (List<Pair<URI, URI>> ll: list) {

            String values = "   VALUES (?p1 ?p2) {\n";
            for (Pair<URI, URI> pp: ll) {
                URI p1 = pp.getLeft();
                URI p2 = pp.getRight();
                values += "      ( <" + p1.toString() + "> <" + p2.toString() + "> )\n";
            }
            values += "   }\n";

            String star_query = "" +
                    "SELECT ?p1 ?p2 (count(*) as ?result) \n" +
                    "WHERE { \n" +
                    "   ?s ?p1 ?o1 . \n" +
                    "   ?s ?p2 ?o2 . \n" + values +
                    "} \n" +
                    "GROUP BY ?p1 ?p2;";

            String sink_query = "" +
                    "SELECT (count(*) as ?result) \n" +
                    "WHERE { \n" +
                    "   ?s1 ?p1 ?o . \n" +
                    "   ?s2 ?p2 ?o . \n" + values +
                    "}";

            String path_query = "" +
                    "SELECT (count(*) as ?result) \n" +
                    "WHERE { \n" +
                    "   ?s ?p1 ?c . \n" +
                    "   ?c ?p2 ?o . \n" + values +
                    "}";

            List<Pair<Pair<URI, URI>, Long>> starbach = evaluateQueryBatch(conn, star_query, "p1", "p2", "result");
            List<Pair<Pair<URI, URI>, Long>> sinkbach = evaluateQueryBatch(conn, sink_query, "p1", "p2", "result");
            List<Pair<Pair<URI, URI>, Long>> pathbach = evaluateQueryBatch(conn, path_query, "p1", "p2", "result");

            for (Pair<Pair<URI, URI>, Long> pair: starbach) {
                Pair<URI, URI> uris = pair.getLeft();
                long result = pair.getRight();
                if (URIlessEq(uris.getLeft(), uris.getRight())) {
                    starJoins.put(uris, result);
                }
            }

            for (Pair<Pair<URI, URI>, Long> pair: sinkbach) {
                Pair<URI, URI> uris = pair.getLeft();
                long result = pair.getRight();
                if (URIlessEq(uris.getLeft(), uris.getRight())) {
                    sinkJoins.put(uris, result);
                }
            }

            for (Pair<Pair<URI, URI>, Long> pair: pathbach) {
                Pair<URI, URI> uris = pair.getLeft();
                long result = pair.getRight();
                pathJoins.put(uris, result);
            }
        }

    }

    private List<Pair<Pair<URI, URI>, Long>> evaluateQueryBatch(RepositoryConnection conn,
                String query, String leftVar, String rightVar, String resultVar)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        List<Pair<Pair<URI, URI>, Long>> list = new ArrayList<>();

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                Value left = bs.getValue(leftVar);
                Value right = bs.getValue(rightVar);

                if (left instanceof URI && right instanceof URI) {
                    long result = Integer.valueOf(bs.getValue(resultVar).stringValue());
                    list.add(Pair.of(Pair.of((URI) left, (URI) right), result));
                }
            }
        } finally {
            results.close();
        }
        return list;
    }

    private void calculateSelectivitiesInternal(RepositoryConnection conn) throws Exception {

        for (URI p1 : propertyPartitionMap.keySet()) {
            for (URI p2 : propertyPartitionMap.keySet()) {
                if (!(p1.equals(p2))) {

                    log.debug("Calculating selectivity between " + p1 + " and " + p2);

                    String star_query = "" +
                            "SELECT (count(*) as ?result) \n" +
                            "WHERE { \n" +
                            "   ?s <" + p1.toString() + "> ?o1 . \n" +
                            "   ?s <" + p2.toString() + "> ?o2 . \n" +
                            "}";

                    String sink_query = "" +
                            "SELECT (count(*) as ?result) \n" +
                            "WHERE { \n" +
                            "   ?s1 <" + p1.toString() + "> ?o . \n" +
                            "   ?s2 <" + p2.toString() + "> ?o . \n" +
                            "}";

                    String path_query = "" +
                            "SELECT (count(*) as ?result) \n" +
                            "WHERE { \n" +
                            "   ?s <" + p1.toString() + "> ?c . \n" +
                            "   ?c <" + p2.toString() + "> ?o . \n" +
                            "}";

                    if (URIlessEq(p1, p2)) {
                        long stars = evaluateQuery(conn, star_query, "result");

                        long sinks = evaluateQuery(conn, sink_query, "result");

                        if (stars > 0) {
                            starJoins.put(Pair.of(p1, p2), stars);
                        }
                        if (sinks > 0) {
                            sinkJoins.put(Pair.of(p1, p2), sinks);
                        }
                    }
                    long paths = evaluateQuery(conn, path_query, "result");

                    if (paths > 0) {
                        pathJoins.put(Pair.of(p1, p2), paths);
                    }
                }
            }
        }
    }

    private long evaluateQuery(RepositoryConnection conn, String query, String resultVar)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        long result = 0;
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                result = Integer.valueOf(bs.getValue(resultVar).stringValue());
            }
        } finally {
            results.close();
        }
        return result;
    }

    private boolean URIlessEq(URI u1, URI u2) {
        return (u1.toString().compareTo(u2.toString()) <= 0);
    }

    private void addJoin(Map<Pair<URI, URI>, Integer> join, URI p1, URI p2) {
        Pair<URI, URI> key = Pair.of(p1,p2);
        if (join.containsKey(key)) {
            join.put(key, join.get(key) + 1);
        }
        else {
            join.put(key, 1);
        }
    }

    public void writeSelectivities(RDFWriter writer) throws Exception {

        for (Pair<URI,URI> pair: starJoins.keySet()) {
            BNode j = vf.createBNode();
            BNode s = vf.createBNode();
            Resource left = propertyPartitionMap.get(pair.getLeft());
            Resource right = propertyPartitionMap.get(pair.getRight());
            Long selectivityValue = starJoins.get(pair);

            writer.handleStatement(vf.createStatement(j, RDF.TYPE , SEVOD.JOIN));
            writer.handleStatement(vf.createStatement(j, SEVOD.JOINSUBJECT, left));
            writer.handleStatement(vf.createStatement(j, SEVOD.JOINSUBJECT, right));
            writer.handleStatement(vf.createStatement(j, SEVOD.SELECTIVITY, s));
            writer.handleStatement(vf.createStatement(s, RDF.VALUE, vf.createLiteral(selectivityValue)));
        }

        for (Pair<URI,URI> pair: pathJoins.keySet()) {
            BNode j = vf.createBNode();
            BNode s = vf.createBNode();
            Resource left = propertyPartitionMap.get(pair.getLeft());
            Resource right = propertyPartitionMap.get(pair.getRight());
            Long selectivityValue = pathJoins.get(pair);

            writer.handleStatement(vf.createStatement(j, RDF.TYPE , SEVOD.JOIN));
            writer.handleStatement(vf.createStatement(j, SEVOD.JOINSUBJECT, left));
            writer.handleStatement(vf.createStatement(j, SEVOD.JOINOBJECT, right));
            writer.handleStatement(vf.createStatement(j, SEVOD.SELECTIVITY, s));
            writer.handleStatement(vf.createStatement(s, RDF.VALUE, vf.createLiteral(selectivityValue)));
        }

        for (Pair<URI,URI> pair: sinkJoins.keySet()) {
            BNode j = vf.createBNode();
            BNode s = vf.createBNode();
            Resource left = propertyPartitionMap.get(pair.getLeft());
            Resource right = propertyPartitionMap.get(pair.getRight());
            Long selectivityValue = sinkJoins.get(pair);

            writer.handleStatement(vf.createStatement(j, RDF.TYPE , SEVOD.JOIN));
            writer.handleStatement(vf.createStatement(j, SEVOD.JOINOBJECT, left));
            writer.handleStatement(vf.createStatement(j, SEVOD.JOINOBJECT, right));
            writer.handleStatement(vf.createStatement(j, SEVOD.SELECTIVITY, s));
            writer.handleStatement(vf.createStatement(s, RDF.VALUE, vf.createLiteral(selectivityValue)));
        }
    }
}
