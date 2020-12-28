package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.prefix.PrefixSet;
import org.semagrow.sevod.scraper.rdf.dump.prefix.SimplePrefixSet;
import org.semagrow.sevod.scraper.rdf.dump.util.DistinctCounter;
import org.semagrow.sevod.scraper.rdf.dump.util.MapDBDistinctCounter;

import java.util.Set;

public class PredicateMetadata implements Metadata {

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private IRI predicate;

    private int triples = 0;
    private DistinctCounter subjCount = new MapDBDistinctCounter();
    private DistinctCounter objCount = new MapDBDistinctCounter();

    private PrefixSet subjPrefix;
    private PrefixSet objPrefix;

    public PredicateMetadata(IRI predicate, Set<String> knownPrefixes) {
        this.predicate = predicate;
        subjPrefix = new SimplePrefixSet(knownPrefixes);
        objPrefix = new SimplePrefixSet(knownPrefixes);
    }

    @Override
    public void processStatement(Statement statement) {
        assert statement.getPredicate().equals(predicate);

        Value s = statement.getSubject();
        Value o = statement.getObject();

        subjCount.add(s);
        objCount.add(o);

        if (s instanceof IRI) {
            subjPrefix.handle((IRI) s);
        }
        if (o instanceof IRI) {
            objPrefix.handle((IRI) o);
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

        for (String prefix: subjPrefix.getPrefixSet()) {
            writer.handleStatement(vf.createStatement(prop, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }

        for (String prefix: objPrefix.getPrefixSet()) {
            writer.handleStatement(vf.createStatement(prop, SEVOD.OBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }
    }
}
