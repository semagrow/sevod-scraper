package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.util.DistinctCounter;
import org.semagrow.sevod.scraper.rdf.dump.util.MapDBDistinctCounter;

import java.util.HashSet;
import java.util.Set;

public class ClassMetadata implements Metadata {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private URI clazz;
    private Set<String> knownPrefixes;

    private DistinctCounter entityCount = new MapDBDistinctCounter();
    private Set<String> entityPrefix = new HashSet<>();

    public ClassMetadata(URI clazz, Set<String> knownPrefixes) {
        this.clazz = clazz;
        this.knownPrefixes = knownPrefixes;
    }

    @Override
    public void processStatement(Statement statement) {
        assert statement.getPredicate().equals(RDF.TYPE);
        assert statement.getObject().equals(clazz);

        Value s = statement.getSubject();

        entityCount.add(s);

        for (String prefix: knownPrefixes) {
            if (s.stringValue().startsWith(prefix)) {
                entityPrefix.add(prefix);
            }
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        BNode clzp = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, VOID.CLASSPARTITION, clzp));
        writer.handleStatement(vf.createStatement(clzp, VOID.CLASS, clazz));
        writer.handleStatement(vf.createStatement(clzp, VOID.ENTITIES, vf.createLiteral(entityCount.getDistinctCount())));

        for (String prefix: entityPrefix) {
            writer.handleStatement(vf.createStatement(clzp, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }
    }
}
