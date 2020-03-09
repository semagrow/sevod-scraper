package org.semagrow.sevod.scraper.geordf.dump.vocabulary;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.semagrow.sevod.commons.vocabulary.SEVOD;

public class VOCAB {

    public final static URI DEFAULT_CRS;
    public final static URI DATASET_BOUNDING_POLYGON;

    static {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        DEFAULT_CRS = vf.createURI("http://www.opengis.net/def/crs/EPSG/4326");
        DATASET_BOUNDING_POLYGON = vf.createURI(SEVOD.NAMESPACE + "datasetBoundingPolygon");
    }
}
