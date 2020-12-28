package org.semagrow.sevod.scraper.rdf.dump.util;

import org.eclipse.rdf4j.model.Value;

public interface DistinctCounter {

    void add(Value value);

    int getDistinctCount();
}
