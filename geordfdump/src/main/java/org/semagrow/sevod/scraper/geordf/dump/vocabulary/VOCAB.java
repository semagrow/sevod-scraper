package org.semagrow.sevod.scraper.geordf.dump.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.semagrow.sevod.commons.vocabulary.SEVOD;

public class VOCAB {

    public final static IRI DEFAULT_CRS;
    public final static IRI DATASET_BOUNDING_POLYGON;

    static {
        ValueFactory vf = SimpleValueFactory.getInstance();

        DEFAULT_CRS = vf.createIRI("http://www.opengis.net/def/crs/EPSG/4326");
        DATASET_BOUNDING_POLYGON = vf.createIRI(SEVOD.NAMESPACE + "datasetBoundingPolygon");
    }
}
