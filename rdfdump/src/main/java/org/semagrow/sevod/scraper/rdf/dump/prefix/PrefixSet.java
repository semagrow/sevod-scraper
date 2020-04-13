package org.semagrow.sevod.scraper.rdf.dump.prefix;

import org.eclipse.rdf4j.model.IRI;

import java.util.Set;

public interface PrefixSet {

    void handle(IRI uri);

    Set<String> getPrefixSet();
}
