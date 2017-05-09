package org.deidentifier.arx.io;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * Configuration describing a Cassandra source.
 *
 * @author João Gonçalves
 */

public class ImportConfigurationCassandra extends ImportConfiguration
{

    /**
     * Cassandra cluster to be used.
     *
     */
    private Cluster cluster;

    /**
     * Cassandra session to be used.
     *
     */
    private Session session;

    /**
     * Name of table to be used.
     *
     */
    private String table;

    /**
     * Determines whether we need to manage the Cassandra connection.
     */
    private final boolean manageConnection;


    /**
     * Creates a new instance of this object.
     *
     */
    public ImportConfigurationCassandra(String host,String clusterName,String table,String user,String password)
    {
        this.cluster = Cluster.builder().addContactPoint(host).withSocketOptions(new SocketOptions().setReadTimeoutMillis(3600000)).build();
        this.session = this.cluster.connect(clusterName);
        this.session.execute("USE " + clusterName);
        this.table = table;
        this.manageConnection = true;
    }


    /**
     * Creates a new instance of this object.
     *
     */
    public ImportConfigurationCassandra(String host,String clusterName,String table)
    {
        this.cluster = Cluster.builder().addContactPoint(host).withSocketOptions(new SocketOptions().setReadTimeoutMillis(3600000)).build();
        this.session = this.cluster.connect(clusterName);
        this.session.execute("USE " + clusterName);
        this.table = table;
        this.manageConnection = true;
    }

    /**
     * Adds a single column to import from
     *
     * This makes sure that only {@link ImportColumnCassandra} can be added,
     * otherwise an {@link IllegalArgumentException} will be thrown.
     *
     * @param column
     *            A single column to import from, {@link ImportColumnCassandra}
     */
    @Override
    public void addColumn(ImportColumn column) {

        if (!(column instanceof ImportColumnCassandra)) {
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
     * Closes any underlying Cassandra connection that may have either been created by ARX or passed during construction.
     */
    public void close()
    {
        try
        {
            this.session.close();
            this.cluster.close();
        } catch (Exception e) {
            // Ignore
        }
    }


    public Cluster getCluster() {return cluster;}

    public Session getSession() {return session;}

    public String getTable() {return table;}

    public boolean isManageConnection() {return manageConnection;}
}
