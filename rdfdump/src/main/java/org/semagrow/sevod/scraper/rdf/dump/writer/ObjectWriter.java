package org.semagrow.sevod.scraper.rdf.dump.writer;

import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.util.MyStringUtils;
import org.semagrow.sevod.scraper.rdf.dump.util.Statistics;
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
                writer.handleStatement(vf.createStatement(dataset, VOID.SUBSET, propPartition));
                writer.handleStatement(vf.createStatement(propPartition, SEVOD.OBJECTREGEXPATTERN, vf.createLiteral(patternEsc)));
                writer.handleStatement(vf.createStatement(propPartition, VOID.TRIPLES, tripleCount));
                writer.handleStatement(vf.createStatement(propPartition, VOID.DISTINCTSUBJECTS, nDistSubjects));
                writer.handleStatement(vf.createStatement(propPartition, VOID.PROPERTIES, nProperties));

            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
    }
}
