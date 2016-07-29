package org.semagrow.sevod.scraper.sparql;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;

import java.io.PrintStream;

/**
 * Created by antonis on 29/7/2016.
 */
public class MetadataWriter {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    public void writeMetadata(String endpoint, PrintStream stream) throws RDFHandlerException {

    }
}
