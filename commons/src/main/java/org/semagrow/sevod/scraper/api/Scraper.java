package org.semagrow.sevod.scraper.api;

import org.eclipse.rdf4j.rio.RDFHandlerException;

import java.io.IOException;

public interface Scraper {
    public void scrape(String input, String output) throws RDFHandlerException, IOException;
}
