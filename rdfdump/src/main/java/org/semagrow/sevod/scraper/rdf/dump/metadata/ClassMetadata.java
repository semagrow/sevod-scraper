package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
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

public class ClassMetadata implements Metadata {

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private IRI clazz;

    private DistinctCounter entityCount = new MapDBDistinctCounter();
    private PrefixSet entityPrefix;

    public ClassMetadata(IRI clazz, Set<String> knownPrefixes) {
        this.clazz = clazz;
        entityPrefix = new SimplePrefixSet(knownPrefixes);
    }

    @Override
    public void processStatement(Statement statement) {
        assert statement.getPredicate().equals(RDF.TYPE);
        assert statement.getObject().equals(clazz);

        Value s = statement.getSubject();

        entityCount.add(s);

        if (s instanceof IRI) {
            entityPrefix.handle((IRI) s);
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        BNode clzp = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, VOID.CLASSPARTITION, clzp));
        writer.handleStatement(vf.createStatement(clzp, VOID.CLASS, clazz));
        writer.handleStatement(vf.createStatement(clzp, VOID.ENTITIES, vf.createLiteral(entityCount.getDistinctCount())));

        for (String prefix: entityPrefix.getPrefixSet()) {
            writer.handleStatement(vf.createStatement(clzp, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }
    }
}
