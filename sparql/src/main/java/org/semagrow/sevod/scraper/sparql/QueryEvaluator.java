package org.semagrow.sevod.scraper.sparql;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by antonis on 29/7/2016.
 */
public class QueryEvaluator {

    final private Logger log = LoggerFactory.getLogger(QueryEvaluator.class);

    private String endpoint;

    public QueryEvaluator(String endpoint) {
        this.endpoint = endpoint;
    }

    private List<BindingSet> run(String qStr) {

        final List<BindingSet> result = new ArrayList<>();

        try {
            Repository repository = new SPARQLRepository(endpoint);
            repository.initialize();

            RepositoryConnection connection = repository.getConnection();
            TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, qStr);

            log.info("Query  {}", qStr);

            query.evaluate(new TupleQueryResultHandler() {

                @Override
                public void handleBoolean(boolean b) throws QueryResultHandlerException {

                }

                @Override
                public void handleLinks(List<String> list) throws QueryResultHandlerException {

                }

                @Override
                public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {

                }

                @Override
                public void endQueryResult() throws TupleQueryResultHandlerException {

                }

                @Override
                public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
                    result.add(bindingSet);
                }
            });

            repository.shutDown();

        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (TupleQueryResultHandlerException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }

        log.info("Result {}", result);

        return result;
    }

    public boolean ask(String qStr) {
        return !run(qStr).isEmpty();
    }

    public int count(String qStr) {
        Value result = run(qStr).get(0).getBinding(Queries.count_var.substring(1)).getValue();
        return Integer.parseInt(result.stringValue());
    }

    public List<IRI> iris(String qStr, String var) {
        List<IRI> result = new ArrayList<>();
        for (BindingSet bs: run(qStr)) {
            result.add((IRI) bs.getBinding(var.substring(1)).getValue());
        }
        return result;
    }
}
