package org.semagrow.sevod.scraper.rdf.dump.prefix;

import org.openrdf.model.URI;

import java.util.Set;

public interface PrefixSet {

    void handle(URI uri);

    Set<String> getPrefixSet();
}
