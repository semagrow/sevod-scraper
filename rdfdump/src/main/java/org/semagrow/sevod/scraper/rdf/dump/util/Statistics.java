package org.semagrow.sevod.scraper.rdf.dump.util;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * Created by antonis on 14/5/2015.
 */
public class Statistics {

    private long count = 0;
    private DistinctCounter distSubj = new DistinctCounter(null);
    private DistinctCounter prop = new DistinctCounter(null);
    private DistinctCounter distObj = new DistinctCounter(null);

    public void addSubject(Resource s) {
        distSubj.add(s.toString());
    }

    public void addProperty(URI p) {
        prop.add(p.toString());
    }

    public void addObject(Value o) {
        distObj.add(o.toString());
    }

    public void addCount() {
        count++;
    }

    public long getCount() {
        return count;
    }

    public long getDistinctObjects() {
        return distObj.getDistinctCount();
    }

    public long getDistinctSubjects() {
        return distSubj.getDistinctCount();
    }

    public long getProperties() {
        return prop.getDistinctCount();
    }
}
