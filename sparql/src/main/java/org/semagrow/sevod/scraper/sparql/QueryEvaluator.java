package org.semagrow.sevod.scraper.sparql;

import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
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

    public List<BindingSet> run(String query_str) {

        final List<BindingSet> result = new ArrayList<>();

        try {
            Repository repository = new SPARQLRepository(endpoint);
            repository.initialize();

            RepositoryConnection connection = repository.getConnection();
            TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, query_str);

            log.info("Query  {}", query_str);

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

}
