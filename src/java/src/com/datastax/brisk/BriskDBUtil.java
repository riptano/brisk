package com.datastax.brisk;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.thrift.NotFoundException;

public class BriskDBUtil {
	
	/**
	 * Validates that the result is not empty and get the value for the <code>columnName</code> column.
	 * @param rows the raw result from StorageProxy.read(....)
	 * @param columnName column name
	 * @return the Column that was requested if it exists.
	 * @throws NotFoundException if the result doesn't exist (including if the value holds a tumbstone)
	 */
    public static  IColumn validateAndGetColumn(List<Row> rows, ByteBuffer columnName) throws NotFoundException {
        if(rows.isEmpty())
            throw new NotFoundException();

        if(rows.size() > 1)
            throw new RuntimeException("Block id returned more than one row");

        Row row = rows.get(0);
        if(row.cf == null)
            throw new NotFoundException();

        IColumn col = row.cf.getColumn(columnName);

        if(col == null || !col.isLive())
            throw new NotFoundException();

        return col;
    }

}
