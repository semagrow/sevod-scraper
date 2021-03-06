package org.semagrow.sevod.scraper.rdf.dump.util;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

/**
 * Created by antonis on 14/5/2015.
 */
public class Statistics {

    private long count = 0;
    private FileDistinctCounter distSubj = new FileDistinctCounter(null);
    private FileDistinctCounter prop = new FileDistinctCounter(null);
    private FileDistinctCounter distObj = new FileDistinctCounter(null);

    public void addSubject(Resource s) {
        distSubj.add(s);
    }

    public void addProperty(IRI p) {
        prop.add(p);
    }

    public void addObject(Value o) {
        distObj.add(o);
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
