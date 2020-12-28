package org.semagrow.sevod.scraper.cql;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.TableMetadata;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;
import org.semagrow.sevod.scraper.cql.utils.CassandraClient;
import org.semagrow.sevod.scraper.cql.utils.RdfMapper;
import org.semagrow.sevod.scraper.cql.vocab.CDV;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Created by antonis on 7/4/2016.
 */
public class CassandraSchemaMetadataWriter implements MetadataWriter {

    private final Logger logger = LoggerFactory.getLogger(CassandraSchemaMetadataWriter.class);

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
    public void writeMetadata(PrintStream stream) {

        try {
            Resource root = vf.createBNode();

            RDFWriter writer  = new TurtleWriter(stream);

            writer.handleNamespace("cdv", CDV.NAMESPACE);
            writer.handleNamespace("void", VOID.NAMESPACE);
            writer.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");

            writer.startRDF();

            writer.handleStatement(vf.createStatement(root, RDF.TYPE, CDV.CASSANDRADB));

            writer.handleStatement(vf.createStatement(root, VOID.SPARQLENDPOINT, endpoint));
            writer.handleStatement(vf.createStatement(root, CDV.ADDRESS, vf.createLiteral(client.getAddress())));
            writer.handleStatement(vf.createStatement(root, CDV.PORT, vf.createLiteral(String.valueOf(client.getPort()))));
            writer.handleStatement(vf.createStatement(root, CDV.KEYSPACE, vf.createLiteral(client.getKeyspace())));
            writer.handleStatement(vf.createStatement(root, CDV.BASE, vf.createLiteral(base)));

            for (TableMetadata tableMetadata: client.getTables()) {
                Resource tables = vf.createBNode();
                writer.handleStatement(vf.createStatement(root, CDV.TABLES, tables));
                writeTableMetadata(writer, tables, tableMetadata);
            }

            writer.endRDF();

        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeTableMetadata(RDFWriter writer, Resource root, TableMetadata tableMetadata) throws RDFHandlerException {
        writer.handleStatement(vf.createStatement(root, CDV.NAME, vf.createLiteral(tableMetadata.getName())));

        Resource tableSchema = vf.createBNode();
        writer.handleStatement(vf.createStatement(root, CDV.TABLESCHEMA, tableSchema));
        int clusteringPosition = 0;

        for (ColumnMetadata columnMetadata: tableMetadata.getColumns()) {
            if (columnMetadata.getParent().getPartitionKey().contains(columnMetadata)) {
                writePartitionColumnMetadata(writer, tableSchema, columnMetadata);
            } else {
                if (columnMetadata.getParent().getClusteringColumns().contains(columnMetadata)) {
                    writeClusteringColumnMetadata(writer, tableSchema, columnMetadata, clusteringPosition);
                    clusteringPosition++;
                }
                else {
                    writeRegularColumnMetadata(writer, tableSchema, columnMetadata);
                }
            }
        }

        for (IndexMetadata indexMetadata: tableMetadata.getIndexes()) {
            writer.handleStatement(vf.createStatement(tableSchema, CDV.SECONDARYINDEX, vf.createLiteral(indexMetadata.getTarget())));
        }

        for (ColumnMetadata columnMetadata: tableMetadata.getPrimaryKey()) {
            writer.handleStatement(vf.createStatement(tableSchema, CDV.PRIMARYKEY, vf.createLiteral(columnMetadata.getName())));
        }
    }

    private void writePartitionColumnMetadata(RDFWriter writer, Resource root, ColumnMetadata columnMetadata) throws RDFHandlerException {
        Resource column = vf.createBNode();
        writer.handleStatement(vf.createStatement(root, CDV.COLUMNS, column));
        writer.handleStatement(vf.createStatement(column, CDV.NAME, vf.createLiteral(columnMetadata.getName())));
        writer.handleStatement(vf.createStatement(column, CDV.COLUMNTYPE, CDV.PARTITION));
        writer.handleStatement(vf.createStatement(column, CDV.DATATYPE, RdfMapper.getXsdFromColumnDatatype(columnMetadata.getType())));
    }

    private void writeClusteringColumnMetadata(RDFWriter writer, Resource root, ColumnMetadata columnMetadata, int clusterinPosition) throws RDFHandlerException {
        Resource column = vf.createBNode();
        writer.handleStatement(vf.createStatement(root, CDV.COLUMNS, column));
        writer.handleStatement(vf.createStatement(column, CDV.NAME, vf.createLiteral(columnMetadata.getName())));
        writer.handleStatement(vf.createStatement(column, CDV.COLUMNTYPE, CDV.CLUSTERING));
        writer.handleStatement(vf.createStatement(column, CDV.CLUSTERINGPOSITION, vf.createLiteral(clusterinPosition)));
        writer.handleStatement(vf.createStatement(column, CDV.CLUSTERINGORDER,
                vf.createLiteral(columnMetadata.getParent().getClusteringOrder().get(clusterinPosition).toString())));
        writer.handleStatement(vf.createStatement(column, CDV.DATATYPE, RdfMapper.getXsdFromColumnDatatype(columnMetadata.getType())));
    }

    private void writeRegularColumnMetadata(RDFWriter writer, Resource root, ColumnMetadata columnMetadata) throws RDFHandlerException {
        Resource column = vf.createBNode();
        writer.handleStatement(vf.createStatement(root, CDV.COLUMNS, column));
        writer.handleStatement(vf.createStatement(column, CDV.NAME, vf.createLiteral(columnMetadata.getName())));
        writer.handleStatement(vf.createStatement(column, CDV.COLUMNTYPE, CDV.REGULAR));
        writer.handleStatement(vf.createStatement(column, CDV.DATATYPE, RdfMapper.getXsdFromColumnDatatype(columnMetadata.getType())));
    }
}
