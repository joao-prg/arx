package org.deidentifier.arx.io;

import org.deidentifier.arx.DataType;

/**
 * Represents a single MongoDB data item
 *
 * MongoDB items can be referred to by name ({@link IImportColumnNamed}.
 *
 * @author João Gonçalves
 */
public class ImportItemMongo extends ImportColumn implements IImportColumnNamed
{
    /** Name this item refers to. */
    private String name;


    /**
     * Creates a new instance of this object with the given parameters.
     *
     * @param name
     * @param datatype
     */
    public ImportItemMongo(String name, DataType<?> datatype) {
        this(name, datatype, false);
    }

    /**
     * Creates a new instance of this object with the given parameters.
     *
     * @param name
     * @param datatype
     * @param cleansing
     */
    public ImportItemMongo(String name, DataType<?> datatype, boolean cleansing) {
        super(name, datatype, cleansing);
        setName(name);
    }

    /**
     * Creates a new instance of this object with the given parameters.
     *
     * @param name
     * @param aliasName
     * @param datatype
     */
    public ImportItemMongo(String name, String aliasName, DataType<?> datatype) {
        this(name, aliasName, datatype, false);
    }

    /**
     * Creates a new instance of this object with the given parameters.
     *
     * @param name
     * @param aliasName
     * @param datatype
     * @param cleansing
     */
    public ImportItemMongo(String name, String aliasName, DataType<?> datatype, boolean cleansing) {
        super(aliasName, datatype, cleansing);
        setName(name);
    }

    @Override
    public String getName() {return name;}

    @Override
    public void setName(String name) {this.name = name;}
}
