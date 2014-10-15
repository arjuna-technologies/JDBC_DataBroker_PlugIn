/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.jdbc.postgresql;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataStore;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PostDeactivated;
import com.arjuna.databroker.data.jee.annotation.PreActivated;
import com.arjuna.dbplugins.jdbc.postgresql.metadata.DatabaseView;

public class PostgreSQLDataStore implements DataStore
{
    private static final Logger logger = Logger.getLogger(PostgreSQLDataStore.class.getName());

    public PostgreSQLDataStore(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "PostgreSQLDataStore: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }
    
    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @PreActivated
    public void obtainDatabaseConnection()
    {
        DatabaseView databaseView = null;

        _connection = PostgreSQLUtil.obtainDatabaseConnection(databaseView);
    }

    @PostDeactivated
    public void createTables()
    {
        try
        {
            if (_connection != null)
                _connection.close();
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Failed to close connection", throwable);
        }
    }

    public void store(String data)
    {
        logger.log(Level.FINE, "PostgreSQLDataStore.store: data = " + data);
    }

    public void produceQueryReport()
    {
        logger.log(Level.FINE, "PostgreSQLDataStore.produceQueryReport");

//        _dataProvider.produce(data);
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(String.class);
        
        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);
        
        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private Connection _connection;
    
    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataConsumerInjection(methodName="store")
    private DataConsumer<String> _dataConsumer;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}
