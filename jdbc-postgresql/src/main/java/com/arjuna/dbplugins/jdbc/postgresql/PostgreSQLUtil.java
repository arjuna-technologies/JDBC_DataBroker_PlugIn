/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.jdbc.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.arjuna.dbplugins.jdbc.postgresql.metadata.DatabaseView;
import com.arjuna.dbplugins.jdbc.postgresql.metadata.TableView;

public class PostgreSQLUtil
{
    private static final Logger logger = Logger.getLogger(PostgreSQLUtil.class.getName());

    public Connection obtainDatabaseConnection(DatabaseView databaseView)
    {
        Connection connection = null;

        try
        {
            String     databaseURL = "jdbc:postgresql://" + databaseView.getHostName() + ":" + databaseView.getPostNumber() + "/" + databaseView.getName();
            Properties properties  = new Properties();
            properties.setProperty("user", databaseView.getUserName());
            properties.setProperty("password", databaseView.getPassword());

            connection = DriverManager.getConnection(databaseURL, properties);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Unable to obtain database connection", throwable);
        }

        return connection;
    }

    public Boolean insertTableRow(TableView tableView, Map<String, Object> data)
    {
        return null;
    }
}