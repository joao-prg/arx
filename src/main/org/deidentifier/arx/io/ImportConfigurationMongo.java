package org.deidentifier.arx.io;

import com.mongodb.MongoClient;

/**
 * Configuration describing a MongoDB source.
 *
 * @author João Gonçalves
 */

public class ImportConfigurationMongo extends ImportConfiguration
{

    /**
     * MongoDB connection to be used.
     *
     */
    private MongoClient mongoClient;

    /**
     * Name of database to be used.
     *
     */
    private String database;

    /**
     * Name of collection to be used.
     *
     */
    private String collection;

    /**
     * Determines whether we need to manage the Cassandra connection.
     */
    private final boolean manageConnection;


    /**
     * Creates a new instance of this object.
     *
     */
    public ImportConfigurationMongo(String host,int port,String database,String collection)
    {
        this.mongoClient = new MongoClient(host,port);
        this.database = database;
        this.collection = collection;
        this.manageConnection = true;
    }

    /**
     * Creates a new instance of this object.
     *
     */
    public ImportConfigurationMongo(String host,int port,String database,String collection,String user,String password)
    {
        this.mongoClient = new MongoClient(host,port);
        this.database = database;
        this.collection = collection;
        this.manageConnection = true;
    }

    /**
     * Adds a single item to import from
     *
     * This makes sure that only {@link ImportItemMongo} can be added,
     * otherwise an {@link IllegalArgumentException} will be thrown.
     * Does not make sense to use indexes, mongoDB documents are composed by
     * (not mandatory) items, not columns.
     *
     * @param column
     *            A single item to import from, {@link ImportItemMongo}
     */
    @Override
    public void addColumn(ImportColumn column) {

        if (!(column instanceof ImportItemMongo)) {
            throw new IllegalArgumentException("");
        }

        for (ImportColumn c : columns)
        {
            if (column.getAliasName() != null && c.getAliasName() != null &&
                    c.getAliasName().equals(column.getAliasName())) {
                throw new IllegalArgumentException("Item names need to be unique");
            }
        }
        this.columns.add(column);
    }


    /**
     * Closes any underlying Mongo connection that may have either been created by ARX or passed during construction.
     */
    public void close()
    {
        try {
            this.mongoClient.close();
        } catch (Exception e) {
            // Ignore
        }
    }


    public MongoClient getMongoClient() {return mongoClient;}

    public String getDatabase() {return database;}

    public String getCollection() {return collection;}

    public boolean isManageConnection() {return manageConnection;}
}
