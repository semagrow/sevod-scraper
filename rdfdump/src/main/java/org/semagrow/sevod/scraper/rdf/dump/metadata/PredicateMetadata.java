package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.util.DistinctCounter;
import org.semagrow.sevod.scraper.rdf.dump.util.MapDBDistinctCounter;

import java.util.HashSet;
import java.util.Set;

public class PredicateMetadata implements Metadata {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private URI predicate;
    private Set<String> knownPrefixes;

    private int triples = 0;
    private DistinctCounter subjCount = new MapDBDistinctCounter();
    private DistinctCounter objCount = new MapDBDistinctCounter();
    private Set<String> subjPrefix = new HashSet<>();
    private Set<String> objPrefix = new HashSet<>();

    public PredicateMetadata(URI predicate, Set<String> knownPrefixes) {
        this.predicate = predicate;
        this.knownPrefixes = knownPrefixes;
    }

    @Override
    public void processStatement(Statement statement) {
        assert statement.getPredicate().equals(predicate);

        Value s = statement.getSubject();
        Value o = statement.getObject();

        subjCount.add(s);
        objCount.add(o);

        for (String prefix: knownPrefixes) {
            if (s.stringValue().startsWith(prefix)) {
                subjPrefix.add(prefix);
            }
            if (o.stringValue().startsWith(prefix)) {
                objPrefix.add(prefix);
            }
        }

        triples++;
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        BNode prop = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTYPARTITION, prop));
        writer.handleStatement(vf.createStatement(prop, VOID.PROPERTY, predicate));

        writer.handleStatement(vf.createStatement(prop, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTSUBJECTS, vf.createLiteral(subjCount.getDistinctCount())));
        writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTOBJECTS, vf.createLiteral(objCount.getDistinctCount())));

        for (String prefix: subjPrefix) {
            writer.handleStatement(vf.createStatement(prop, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }

        for (String prefix: objPrefix) {
            writer.handleStatement(vf.createStatement(prop, SEVOD.OBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }
    }
}
