package eu.semagrow.stack.metadatagen;

import eu.semagrow.stack.metadatagen.util.HandlerBase;
import eu.semagrow.stack.metadatagen.util.Statistics;
import eu.semagrow.stack.metadatagen.vocabulary.SEVOD;
import eu.semagrow.stack.metadatagen.vocabulary.VOID;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;

import java.util.*;

/**
 * Created by antonis on 14/5/2015.
 */
public class SubjectWriter extends HandlerBase {

    ValueFactory vf = ValueFactoryImpl.getInstance();

    Map<String, Statistics> subjectStats = new HashMap<>();

    public SubjectWriter(List<String> patterns) {
        for (String p : patterns) {
            Statistics stats = new Statistics();
            subjectStats.put(p, stats);
        }
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        if (st.getSubject() instanceof URI) {
            String str = ((URI) st.getSubject()).toString();
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

    public void writeSevodStats(RDFWriter writer, Resource dataset) {
        for (String pattern : subjectStats.keySet()) {

            BNode propPartition = vf.createBNode();
            Literal tripleCount = vf.createLiteral(String.valueOf(subjectStats.get(pattern).getCount()));
            Literal nDistObjects = vf.createLiteral(String.valueOf(subjectStats.get(pattern).getDistinctObjects()));
            Literal nProperties = vf.createLiteral(String.valueOf(subjectStats.get(pattern).getProperties()));

            try {
                writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.subset.toString()), propPartition));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(SEVOD.subjectRegexPattern.toString()), vf.createURI(pattern)));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.triples.toString()), tripleCount));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctObjects.toString()), nDistObjects));
                writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.properties.toString()), nProperties));

            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
    }

}
