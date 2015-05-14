package eu.semagrow.stack.metadatagen.util;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by antonis on 14/5/2015.
 */
public class Statistics {

    private int count = 0;
    private Set<Resource> distSubj = new HashSet<>();
    private Set<URI> prop = new HashSet<>();
    private  Set<Value> distObj = new HashSet<>();



    public void addSubject(Resource s) {
        distSubj.add(s);
    }

    public void addProperty(URI p) {
        prop.add(p);
    }

    public void addObject(Value o) {
        distObj.add(o);
    }

    public void addCount() {
        count++;
    }

    public int getCount() {
        return count;
    }

    public int getDistinctObjects() {
        return distObj.size();
    }

    public int getDistinctSubjects() {
        return distSubj.size();
    }

    public int getProperties() {
        return prop.size();
    }
}
