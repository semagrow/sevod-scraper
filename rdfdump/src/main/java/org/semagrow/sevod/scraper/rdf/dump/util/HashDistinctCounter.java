package org.semagrow.sevod.scraper.rdf.dump.util;

import org.apache.commons.codec.digest.DigestUtils;

import org.openrdf.model.Value;
import org.semagrow.sevod.scraper.rdf.dump.api.DistinctCounter;

import java.util.HashSet;
import java.util.Set;

public class HashDistinctCounter implements DistinctCounter {

    private Set<String> set = new HashSet<>();

    public void add(Value v) {
        String md5hex = DigestUtils.md5Hex(v.toString());
        set.add(md5hex);
    }

    public int getDistinctCount() {
        return set.size();
    }
}
