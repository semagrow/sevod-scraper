package eu.semagrow.scraper.cassandra.utils;

import com.datastax.driver.core.DataType;
import eu.semagrow.scraper.cassandra.vocab.CDT;
import eu.semagrow.scraper.cassandra.vocab.XSD;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 8/4/2016.
 */
public class RdfMapper {

    private static final ValueFactory vf = ValueFactoryImpl.getInstance();

    public static URI getUriFromTable(String base, String table) {
        String uriString = base + "/" + table;
        return vf.createURI(uriString);
    }

    public static URI getUriFromColumn(String base, String table, String column) {
        String uriString = base + "/" + table + "#" + column;
        return vf.createURI(uriString);
    }

    public static URI getXsdFromColumnDatatype(DataType dataType) {
        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text()) || dataType.equals(DataType.inet())) {
            return XSD.STRING;
        }
        if (dataType.equals(DataType.varint())) {
            return XSD.INTEGER;
        }
        if (dataType.equals(DataType.decimal())) {
            return XSD.DECIMAL;
        }
        if (dataType.equals(DataType.cint())) {
            return XSD.INT;
        }
        if (dataType.equals(DataType.bigint()) || dataType.equals(DataType.counter()) ) {
            return XSD.LONG;
        }
        if (dataType.equals(DataType.cdouble())) {
            return XSD.DOUBLE;
        }
        if (dataType.equals(DataType.cfloat())) {
            return XSD.FLOAT;
        }
        if (dataType.equals(DataType.cboolean())) {
            return XSD.BOOLEAN;
        }
        if (dataType.equals(DataType.blob())) {
            return CDT.BLOB;
        }
        if (dataType.equals(DataType.date())) {
            return CDT.DATE;
        }
        if (dataType.equals(DataType.time())) {
            return CDT.TIME;
        }
        if (dataType.equals(DataType.timestamp())) {
            return CDT.TIMESTAMP;
        }
        if (dataType.equals(DataType.timeuuid())) {
            return CDT.TIMEUUID;
        }
        if (dataType.isFrozen()) {
            if (dataType.getName().toString().equals("udt")) {
                return CDT.UDT;
            }
        }
        if (dataType.isCollection()) {
            if (dataType.getName().toString().equals("set")) {
                return CDT.SET;
            }
            if (dataType.getName().toString().equals("list")) {
                return CDT.LIST;
            }
            if (dataType.getName().toString().equals("map")) {
                return CDT.MAP;
            }
        }
        throw new RuntimeException();
    }

}
