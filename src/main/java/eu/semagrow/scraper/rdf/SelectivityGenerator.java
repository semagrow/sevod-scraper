package eu.semagrow.scraper.rdf;

import eu.semagrow.scraper.rdf.vocabulary.SEVOD;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
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

import java.io.File;
import java.util.HashMap;
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

    public SelectivityGenerator(Map<URI, Resource> propertyPartitionMap) {
        this.propertyPartitionMap = propertyPartitionMap;
        log.debug("Found " + propertyPartitionMap.keySet().size() + " properties.");
    }

    public void calculateSelectivities(String endpoint) throws Exception {
        Repository repo = new SPARQLRepository(endpoint);
        repo.initialize();
        RepositoryConnection conn = repo.getConnection();
        try {
            calculateSelectivitiesInternal(conn);
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

            writer.handleStatement(vf.createStatement(j, RDF.TYPE , vf.createURI(SEVOD.Join.toString())));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.joinSubject.toString()), left));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.joinSubject.toString()), right));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.selectivity.toString()), s));
            writer.handleStatement(vf.createStatement(s, RDF.VALUE, vf.createLiteral(selectivityValue)));
        }

        for (Pair<URI,URI> pair: pathJoins.keySet()) {
            BNode j = vf.createBNode();
            BNode s = vf.createBNode();
            Resource left = propertyPartitionMap.get(pair.getLeft());
            Resource right = propertyPartitionMap.get(pair.getRight());
            Long selectivityValue = pathJoins.get(pair);

            writer.handleStatement(vf.createStatement(j, RDF.TYPE , vf.createURI(SEVOD.Join.toString())));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.joinSubject.toString()), left));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.joinObject.toString()), right));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.selectivity.toString()), s));
            writer.handleStatement(vf.createStatement(s, RDF.VALUE, vf.createLiteral(selectivityValue)));
        }

        for (Pair<URI,URI> pair: sinkJoins.keySet()) {
            BNode j = vf.createBNode();
            BNode s = vf.createBNode();
            Resource left = propertyPartitionMap.get(pair.getLeft());
            Resource right = propertyPartitionMap.get(pair.getRight());
            Long selectivityValue = sinkJoins.get(pair);

            writer.handleStatement(vf.createStatement(j, RDF.TYPE , vf.createURI(SEVOD.Join.toString())));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.joinObject.toString()), left));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.joinObject.toString()), right));
            writer.handleStatement(vf.createStatement(j, vf.createURI(SEVOD.selectivity.toString()), s));
            writer.handleStatement(vf.createStatement(s, RDF.VALUE, vf.createLiteral(selectivityValue)));
        }
    }
}
