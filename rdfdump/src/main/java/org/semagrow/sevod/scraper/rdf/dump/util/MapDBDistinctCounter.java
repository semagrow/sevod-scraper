package org.semagrow.sevod.scraper.rdf.dump.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.eclipse.rdf4j.model.Value;

import java.util.Set;

public class MapDBDistinctCounter implements DistinctCounter {

    private Set<String> set;

    public MapDBDistinctCounter() {
        DB db = DBMaker.tempFileDB().make();
        set = db.hashSet("set")
                .serializer(Serializer.STRING)
                .createOrOpen();
    }

    public void add(Value value) {
        String md5hex = DigestUtils.md5Hex(value.toString());
        set.add(md5hex);
    }

    public int getDistinctCount() {
        return set.size();
    }
}
