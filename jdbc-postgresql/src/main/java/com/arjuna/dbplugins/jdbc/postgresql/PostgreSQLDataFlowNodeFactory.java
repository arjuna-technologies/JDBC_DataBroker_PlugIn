/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.jdbc.postgresql;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataStore;
import com.arjuna.databroker.data.InvalidClassException;
import com.arjuna.databroker.data.InvalidMetaPropertyException;
import com.arjuna.databroker.data.InvalidNameException;
import com.arjuna.databroker.data.InvalidPropertyException;
import com.arjuna.databroker.data.MissingMetaPropertyException;
import com.arjuna.databroker.data.MissingPropertyException;

public class PostgreSQLDataFlowNodeFactory implements DataFlowNodeFactory
{
    public PostgreSQLDataFlowNodeFactory(String name, Map<String, String> properties)
    {
        _name       = name;
        _properties = properties;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return _properties;
    }

    @Override
    public List<Class<? extends DataFlowNode>> getClasses()
    {
        List<Class<? extends DataFlowNode>> classes = new LinkedList<Class<? extends DataFlowNode>>();
        
        classes.add(DataStore.class);
        
        return classes;
    }

    @Override
    public <T extends DataFlowNode> List<String> getMetaPropertyNames(Class<T> dataFlowNodeClass)
        throws InvalidClassException
    {
        if (dataFlowNodeClass.isAssignableFrom(PostgreSQLDataStore.class))
            return Collections.emptyList();
        else
            throw new InvalidClassException("Unsupported class", dataFlowNodeClass.getName());
    }

    @Override
    public <T extends DataFlowNode> List<String> getPropertyNames(Class<T> dataFlowNodeClass, Map<String, String> metaProperties)
        throws InvalidClassException, InvalidMetaPropertyException, MissingMetaPropertyException
    {
        if (dataFlowNodeClass.isAssignableFrom(PostgreSQLDataStore.class))
        {
            if (metaProperties.isEmpty())
            {
                List<String> propertyNames = new LinkedList<String>();

                propertyNames.add(PostgreSQLDataStore.DATABASE_METADATAID_PROPERTYNAME);
                propertyNames.add(PostgreSQLDataStore.DATABASE_METADATAPATH_PROPERTYNAME);

                return propertyNames;
            }
            else
                throw new InvalidMetaPropertyException("No metaproperties expected", null, null);
        }
        else
            throw new InvalidClassException("Unsupported class", dataFlowNodeClass.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DataFlowNode> T createDataFlowNode(String name, Class<T> dataFlowNodeClass, Map<String, String> metaProperties, Map<String, String> properties)
        throws InvalidNameException, InvalidClassException, InvalidMetaPropertyException, MissingMetaPropertyException, InvalidPropertyException, MissingPropertyException
    {
        if (dataFlowNodeClass.isAssignableFrom(PostgreSQLDataStore.class))
        {
            if (metaProperties.isEmpty())
            {
                if (! properties.containsKey(PostgreSQLDataStore.DATABASE_METADATAID_PROPERTYNAME))
                    throw new MissingPropertyException("Properties expected", PostgreSQLDataStore.DATABASE_METADATAID_PROPERTYNAME);
                else if (! properties.containsKey(PostgreSQLDataStore.DATABASE_METADATAPATH_PROPERTYNAME))
                    throw new MissingPropertyException("Properties expected", PostgreSQLDataStore.DATABASE_METADATAID_PROPERTYNAME);
                else if (properties.size() != 2)
                    throw new InvalidPropertyException("Unexpected properties", null, null);

                return (T) new PostgreSQLDataStore(name, properties);
            }
            else
                throw new InvalidMetaPropertyException("No metaproperties expected", null, null);
        }
        else
            throw new InvalidClassException("Unsupported class", dataFlowNodeClass.getName());
    }

    private String              _name;
    private Map<String, String> _properties;
}
