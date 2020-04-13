package org.semagrow.sevod.scraper.rdf.dump.writer;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.util.MyStringUtils;
import org.semagrow.sevod.scraper.rdf.dump.util.Statistics;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by antonis on 14/5/2015.
 */
public class SubjectWriter extends RDFHandlerBase {

    final private Logger log = Logger.getLogger(SubjectWriter.class);

    ValueFactory vf = SimpleValueFactory.getInstance();

    Map<String, Statistics> subjectStats = new HashMap<>();

    public SubjectWriter(List<String> patterns) {
        for (String p : patterns) {
            Statistics stats = new Statistics();
            subjectStats.put(p, stats);
        }
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        log.debug("Handling statement " + st.toString());
        if (st.getSubject() instanceof IRI) {
            String str = ((IRI) st.getSubject()).toString();
            for (String prefix: subjectStats.keySet()) {
                if (str.startsWith(prefix)) {
                    Statistics stats = subjectStats.get(prefix);
                    stats.addCount();
                    stats.addObject(st.getObject());
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
        for (String pattern : subjectStats.keySet()) {

            log.debug("Writing SEVOD statistics of subject pattern " + pattern);

            BNode propPartition = vf.createBNode();
            Literal tripleCount = vf.createLiteral(subjectStats.get(pattern).getCount());
            Literal nDistObjects = vf.createLiteral(subjectStats.get(pattern).getDistinctObjects());
            Literal nProperties = vf.createLiteral(subjectStats.get(pattern).getProperties());

            String patternEsc = MyStringUtils.forRegex(pattern);

            try {
                writer.handleStatement(vf.createStatement(dataset, VOID.SUBSET, propPartition));
                writer.handleStatement(vf.createStatement(propPartition, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(patternEsc)));
                writer.handleStatement(vf.createStatement(propPartition, VOID.PROPERTIES, tripleCount));
                writer.handleStatement(vf.createStatement(propPartition, VOID.DISTINCTOBJECTS, nDistObjects));
                writer.handleStatement(vf.createStatement(propPartition, VOID.PROPERTIES, nProperties));

            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
    }
}
