package org.deidentifier.arx.io;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Import adapter for MongoDB
 *
 * This adapter can import data from Cassandra sources. The source itself is
 * described by an appropriate {@link ImportConfigurationCassandra} object.
 *
 * @author João Gonçalves
 */

public class ImportAdapterCassandra extends ImportAdapter
{
    /** The configuration describing the cluster being used. */
    private ImportConfigurationCassandra config;

    /**
     * ResultSet containing rows to return.
     *
     * @see {@link #next()}
     */
    private ResultSet results;


    /**
     * Iterator of the result set.
     *
     * @see {@link #next()}
     */
    private Iterator<Row> iterator;


    /** Indicates whether there is another row to return. */
    private boolean hasNext;

    /**
     * Indicates whether the first document has already been returned
     */
    private boolean headerReturned;

    /**
     * Number of documents that need to be processed in total.
     */
    private int totalDocs;


    /**
     * Creates a new instance of this object with given configuration.
     *
     * @param config {@link #config}
     * @throws IOException In case of communication errors with Cassandra driver
     */
    protected ImportAdapterCassandra(ImportConfigurationCassandra config) throws IOException
    {

        super(config);
        String statement;
        this.config = config;
        /* Preparation work */
        dataTypes = getColumnDatatypes();
        long totalDocs=0;
        try
        {
            statement = "select count(*) from " + config.getTable() + ";";
            results = config.getSession().execute(statement);
            totalDocs = results.one().getLong(0);
            if (totalDocs== 0)
            {
                closeResources();
                throw new IOException("Collection does not contain any documents.");
            }
            // Create header
            header = createHeader();
            statement = "select ";
            for(int i=0;i<header.length;i++)
            {
                if(i==header.length-1)
                    statement+= header[i] + " ";
                else
                    statement+= header[i] + ", ";
            }
            /* Query for actual data */
            statement += "from " + config.getTable() + " ;";
            results = config.getSession().execute(statement);
            iterator= results.iterator();
            hasNext = iterator.hasNext();
        } catch (Exception e)
        {
            closeResources();
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {return hasNext;}


    /*
    * (non-Javadoc)
    *
    * @see java.util.Iterator#next()
    */
    @Override
    public String[] next()
    {
        /* Return header in first iteration */
        if (!headerReturned)
        {
            headerReturned = true;
            return header;
        }
        try
        {
            String result[] = new String[header.length];
            Row row = iterator.next();
            for(int i=0;i<header.length;i++)
            {
                if(row.getObject(header[i])==null)
                    result[i] = "";
                else
                    result[i] = row.getObject(header[i]).toString();
            }
            /* Move cursor forward and assign result to {@link #hasNext} */
            hasNext = iterator.hasNext();
            if (!hasNext)
            {
                closeResources();
            }
            return result;

        } catch (Exception e) {
            closeResources();
            throw new RuntimeException("Couldn't retrieve data from database");
        }
    }

    /**
     * Dummy.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the JDBC resources.
     */
    private void closeResources()
    {
        try
        {
            if (config.isManageConnection())
            {
                config.getCluster().close();
                config.getSession().close();
            }
        } catch (Exception e) {
            /* Die silently */
        }
    }

    /**
     * Creates the header row
     *
     * This returns a string array with the names of the columns that will be
     * returned later on by iterating over this object. Depending upon whether
     * or not names have been assigned explicitly either the appropriate values
     * will be returned, or names from the Cassandra metadata will be used.
     *
     * @return
     */
    private String[] createHeader()
    {

        /* Initialization */
        String[] header = new String[config.getColumns().size()];
        List<ImportColumn> columns = config.getColumns();
        /* Create header */
        for (int i = 0, len = columns.size(); i < len; i++)
        {
            ImportColumn column = columns.get(i);
            header[i] = column.getAliasName();
        }
        /* Return header */
        return header;
    }

    @Override
    public ImportConfigurationCassandra getConfig() {return config;}

    public ResultSet getResults() {return results;}

    public boolean isHasNext() {return hasNext;}

    public boolean isHeaderReturned() {return headerReturned;}

    public int getTotalDocs() {return totalDocs;}

    public Iterator<Row> getIterator() {return iterator;}

    @Override
    public int getProgress() {
        return 0;
    }
}
