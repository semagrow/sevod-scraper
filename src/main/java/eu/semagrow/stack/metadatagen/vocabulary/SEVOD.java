package eu.semagrow.stack.metadatagen.vocabulary;

/**
 * Created by antonis on 4/5/2015.
 */

public enum SEVOD {

    subjectRegexPattern,
    objectRegexPattern,
    objectClass,
    subjectClass,
    intInterval,
    dateInterval,
    from,
    to;

    public static final String NAMESPACE = "http://rdf.iit.demokritos.gr/2013/sevod#";

    private final String uri;

    private SEVOD(String name) {
        this.uri = NAMESPACE + name;
    }

    private SEVOD() {
        this.uri = NAMESPACE + super.toString();
    }

    public String toString() {
        return this.uri;
    }

}

