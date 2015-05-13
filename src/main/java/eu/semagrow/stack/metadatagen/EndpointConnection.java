package eu.semagrow.stack.metadatagen;

import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 * Created by antru on 27/4/2015.
 */
public class EndpointConnection {

    Repository repo;

    /*
    String getNumberOfTriples() {
        return "SELECT (COUNT(*) AS ?no) { ?s ?p ?o  }";
    }

    String getNumberOfDistinctSubjects() {
        return "SELECT (COUNT(DISTINCT ?s ) AS ?no) {  ?s ?p ?o }";
    }

    String getNumberOfDistinctObjects() {
        return "SELECT (COUNT(DISTINCT ?o ) AS ?no) {  ?s ?p ?o  filter(!isLiteral(?o)) }";
    }
    */

    public EndpointConnection(String url) throws RepositoryException {
        repo = new SPARQLRepository(url);
        repo.initialize();
    }

    public int getSubjectNumberOfTriples(String pattern) throws QueryEvaluationException {

        TupleQueryResult result = sendQuery("" +
                "SELECT (COUNT(*) AS ?no) " +
                "WHERE { " +
                "   ?s ?p ?o . " +
                "   FILTER(regex(?s, \"^" + pattern + "\")) " +
                "}");

        if (result.hasNext()) {
            BindingSet bindingSet = result.next();
            String no = bindingSet.getBinding("no").getValue().stringValue();
            return (Integer.parseInt(no));
        }
        return 0;
    }

    public int getObjectNumberOfTriples(String pattern) throws QueryEvaluationException {

        TupleQueryResult result = sendQuery("" +
                "SELECT (COUNT(*) AS ?no) " +
                "WHERE { " +
                "   ?s ?p ?o . " +
                "   FILTER(regex(?o, \"^" + pattern + "\")) " +
                "}");

        if (result.hasNext()) {
            BindingSet bindingSet = result.next();
            String no = bindingSet.getBinding("no").getValue().stringValue();
            return (Integer.parseInt(no));
        }
        return 0;
    }

    public int getSubjectNumberOfDistinctObjects(String pattern) throws QueryEvaluationException {

        TupleQueryResult result = sendQuery("" +
                "SELECT (COUNT(DISTINCT ?s ) AS ?no) " +
                "WHERE { " +
                "   ?s ?p ?o . " +
                "   FILTER(!isLiteral(?o)). " +
                "   FILTER(regex(?s, \"^" + pattern + "\")) " +
                "}");

        if (result.hasNext()) {
            BindingSet bindingSet = result.next();
            String no = bindingSet.getBinding("no").getValue().stringValue();
            return (Integer.parseInt(no));
        }
        return 0;
    }

    public int getObjectNumberOfDistinctSubjects(String pattern) throws QueryEvaluationException {

        TupleQueryResult result = sendQuery("" +
                "SELECT (COUNT(DISTINCT ?s ) AS ?no) " +
                "WHERE { " +
                "   ?s ?p ?o . " +
                "   FILTER(regex(?o, \"^" + pattern + "\")) " +
                "}");

        if (result.hasNext()) {
            BindingSet bindingSet = result.next();
            String no = bindingSet.getBinding("no").getValue().stringValue();
            return (Integer.parseInt(no));
        }
        return 0;
    }

    private TupleQueryResult sendQuery(String queryStr) {

        try {
            RepositoryConnection conn = repo.getConnection();

            try {
                TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
                try {
                    TupleQueryResult result = query.evaluate();
                    return result;

                } catch (QueryEvaluationException e) {
                    e.printStackTrace();
                }
            } catch (MalformedQueryException e) {
                e.printStackTrace();
            }
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
        return null;
    }

}
