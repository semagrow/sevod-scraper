package org.semagrow.sevod.scraper.rdf.dump.util;

import org.openrdf.model.Value;

public interface DistinctCounter {

    void add(Value value);

    int getDistinctCount();
}
