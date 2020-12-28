package org.semagrow.sevod.scraper.cql;

import com.datastax.driver.core.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.scraper.cql.utils.CassandraClient;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;
import org.semagrow.sevod.scraper.cql.utils.RdfMapper;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Created by antonis on 29/3/2016.
 */
public class CassandraTriplesMetadataWriter implements MetadataWriter {

    private final Logger logger = LoggerFactory.getLogger(CassandraTriplesMetadataWriter.class);

    private CassandraClient client;
    private String base;
    private IRI endpoint;

    private ValueFactory vf = SimpleValueFactory.getInstance();

    @Override
    public void setClient(CassandraClient client) {
        this.client = client;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = vf.createIRI(endpoint);
    }

    @Override
    public void writeMetadata(PrintStream stream){

        try {
            Resource root = vf.createBNode();

            RDFWriter writer  = new CompactBNodeTurtleWriter(stream);

            writer.startRDF();

            writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
            writer.handleNamespace(SEVOD.PREFIX, SEVOD.NAMESPACE);
            writer.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);

            writer.handleStatement(vf.createStatement(root, RDF.TYPE, VOID.DATASET));

            long triples = 0;
            long subj = 0;

            for (TableMetadata tableMetadata: client.getTables()) {
                String tableName = tableMetadata.getName();
                for (ColumnMetadata columnMetadata: tableMetadata.getColumns()) {
                    if (columnMetadata.getParent().getPartitionKey().contains(columnMetadata)) {
                        triples += writePrimaryColumnMetadata(writer, root, base, columnMetadata.getName(), tableName);
                    } else {
                        triples += writeRegularColumnMetadata(writer, root, base, columnMetadata.getName(), tableName);
                    }
                }
                subj += client.executeCount("SELECT COUNT(*) FROM " + tableName + ";");
            }

            writer.handleStatement(vf.createStatement(root, VOID.SPARQLENDPOINT, endpoint));
            writer.handleStatement(vf.createStatement(root, VOID.URISPACE, vf.createLiteral(base)));
            writer.handleStatement(vf.createStatement(root, VOID.TRIPLES, vf.createLiteral(triples)));
            writer.handleStatement(vf.createStatement(root, VOID.DISTINCTSUBJECTS, vf.createLiteral(subj)));

            writer.endRDF();

        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private long writePrimaryColumnMetadata(RDFWriter writer, Resource root, String base, String columnName, String tableName) throws RDFHandlerException {

        long triples = client.executeCount("SELECT COUNT(*) FROM " + tableName + ";");

        Resource partition = vf.createBNode();
        IRI property = RdfMapper.getUriFromColumn(base, tableName, columnName);
        writer.handleStatement(vf.createStatement(root, VOID.PROPERTYPARTITION, partition));
        writer.handleStatement(vf.createStatement(partition, VOID.PROPERTY, property));
        writer.handleStatement(vf.createStatement(partition, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(partition, VOID.DISTINCTSUBJECTS, vf.createLiteral(triples)));

        return triples;
    }

    private long writeRegularColumnMetadata(RDFWriter writer, Resource root, String base, String columnName, String tableName) throws RDFHandlerException {

        long triples = client.executeCount("SELECT COUNT(" + columnName + ") FROM " + tableName + ";");

        Resource partition = vf.createBNode();
        IRI property = RdfMapper.getUriFromColumn(base, tableName, columnName);
        writer.handleStatement(vf.createStatement(root, VOID.PROPERTYPARTITION, partition));
        writer.handleStatement(vf.createStatement(partition, VOID.PROPERTY, property));
        writer.handleStatement(vf.createStatement(partition, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(partition, VOID.DISTINCTSUBJECTS, vf.createLiteral(triples)));

        return triples;
    }
}
