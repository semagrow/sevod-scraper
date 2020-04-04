package org.semagrow.sevod.scraper.rdf.dump.prefix;

import org.openrdf.model.URI;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimplePrefixSet implements PrefixSet {

    private Set<String> knownPrefixes;
    private Set<String> prefixSet = new HashSet<>();

    public SimplePrefixSet(Set<String> knownPrefixes) {
        this.knownPrefixes = knownPrefixes;
    }

    @Override
    public void handle(URI uri) {
        boolean found = false;

        for (String prefix: knownPrefixes) {
           if (uri.stringValue().startsWith(prefix)) {
               prefixSet.add(prefix);
               found = true;
            }
        }
        if (!found) {
            String authority = getAuthority(uri);
            knownPrefixes.add(authority);
            prefixSet.add(authority);
        }
    }

    @Override
    public Set<String> getPrefixSet() {
        return prefixSet;
    }

    static private String getAuthority(URI uri) {
        // regex source: https://tools.ietf.org/html/rfc3986#appendix-B
        String regex = "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(uri.stringValue());

        if (matcher.find()) {
            return matcher.group(1) + matcher.group(3);
        }
        return null; // never reached
    }
}
