package org.deidentifier.arx.io;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.*;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Projections.*;

/**
 * Import adapter for MongoDB
 *
 * This adapter can import data from MongoDB sources. The source itself is
 * described by an appropriate {@link ImportConfigurationMongo} object.
 *
 * @author João Gonçalves
 */
public class ImportAdapterMongo extends ImportAdapter
{
    /** The configuration describing the CSV file being used. */
    private ImportConfigurationMongo config;

    /**
     * ResultSet containing documents to return.
     *
     * @see {@link #next()}
     */
    private MongoCursor<Document> mc;


    /** Indicates whether there is another documentto return. */
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
     * @throws IOException In case of communication errors with MongoDB driver
     */
    protected ImportAdapterMongo(ImportConfigurationMongo config) throws IOException
    {

        super(config);
        this.config = config;

        /* Preparation work */
        dataTypes = getColumnDatatypes();
        long totalDocs=0;
        try
        {
            MongoDatabase db = config.getMongoClient().getDatabase(config.getDatabase());
            MongoCollection<Document> auxCollection = db.getCollection(config.getCollection());
            totalDocs = auxCollection.count();
            if (totalDocs== 0)
            {
                    closeResources();
                    throw new IOException("Collection does not contain any documents.");
            }
            // Create header
            header = createHeader();
            List<String> includes = new ArrayList<>();
            for(int i=0;i<header.length;i++)
            {
                includes.add(header[i]);
            }
            /* Query for actual data */
            mc = auxCollection.find().projection(fields(include(includes))).iterator();
            hasNext = mc.hasNext();
        } catch (MongoException e)
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
            Document doc = mc.next();
            for(int i=0;i<header.length;i++)
            {
                result[i] = doc.get(header[i]).toString();
            }
            /* Move cursor forward and assign result to {@link #hasNext} */
            hasNext = mc.hasNext();
            if (!hasNext)
            {
                closeResources();
            }
            return result;

        } catch (MongoException e) {
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
            if (mc != null) {
                mc.close();
            }
        } catch (Exception e)
        {
            /* Ignore silently */
        }
        try {
            if (config.isManageConnection())
            {
                config.getMongoClient().close();
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
     * will be returned, or names from the MongoDB metadata will be used.
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
    public ImportConfigurationMongo getConfig() {return config;}

    @Override
    public int getProgress() {
        return 0;
    }

    public MongoCursor<Document> getMc() {return mc;}

    public boolean isHasNext() {return hasNext;}

    public boolean isHeaderReturned() {return headerReturned;}

    public int getTotalDocs() {return totalDocs;}
}
