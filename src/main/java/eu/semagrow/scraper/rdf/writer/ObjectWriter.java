package eu.semagrow.scraper.rdf.writer;

import eu.semagrow.scraper.rdf.util.MyStringUtils;
import eu.semagrow.scraper.rdf.util.Statistics;
import eu.semagrow.scraper.rdf.vocabulary.SEVOD;
import eu.semagrow.scraper.rdf.vocabulary.VOID;
import org.apache.log4j.Logger;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by antonis on 15/5/2015.
 */
public class ObjectWriter extends RDFHandlerBase {

    final private Logger log = Logger.getLogger(ObjectWriter.class);

    ValueFactory vf = ValueFactoryImpl.getInstance();

    Map<String, Statistics> objectStats = new HashMap<>();

    public ObjectWriter(List<String> patterns) {
        for (String p : patterns) {
            Statistics stats = new Statistics();
            objectStats.put(p, stats);
        }
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        log.debug("Handling statement " + st.toString());
        if (st.getObject() instanceof URI) {
            String str = ((URI) st.getObject()).toString();
            for (String prefix: objectStats.keySet()) {
                if (str.startsWith(prefix)) {
                    Statistics stats = objectStats.get(prefix);
                    stats.addCount();
                    stats.addSubject(st.getSubject());
                    stats.addProperty(st.getPredicate());
                }
            }
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        super.endRDF();
    }

    public void writeSevodStats(RDFWriter writer, Resource dataset) {
        for (String pattern : objectStats.keySet()) {

            log.debug("Writing SEVOD statistics of object pattern " + pattern);

            BNode propPartition = vf.createBNode();
            Literal tripleCount = vf.createLiteral(objectStats.get(pattern).getCount());
            Literal nDistSubjects = vf.createLiteral(objectStats.get(pattern).getDistinctSubjects());
            Literal nProperties = vf.createLiteral(objectStats.get(pattern).getProperties());

            String patternEsc = MyStringUtils.forRegex(pattern);

            try {
                writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.subset.toString()), propPartition));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(SEVOD.objectRegexPattern.toString()), vf.createLiteral(patternEsc)));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.triples.toString()), tripleCount));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctSubjects.toString()), nDistSubjects));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.properties.toString()), nProperties));

            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
    }
}

