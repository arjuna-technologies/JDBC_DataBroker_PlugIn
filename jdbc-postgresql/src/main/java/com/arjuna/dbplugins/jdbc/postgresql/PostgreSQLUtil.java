/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.jdbc.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.dbutils.metadata.jdbc.view.DatabaseView;
import com.arjuna.dbutils.metadata.jdbc.view.TableView;

public class PostgreSQLUtil
{
    private static final Logger logger = Logger.getLogger(PostgreSQLUtil.class.getName());

    public static Connection obtainDatabaseConnection(DatabaseView databaseView)
    {
        Connection connection = null;

        try
        {
            String     databaseURL = "jdbc:postgresql://" + databaseView.getHostName() + ":" + databaseView.getPortNumber() + "/" + databaseView.getName();
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

    public static Boolean insertTableRow(Connection connection, TableView tableView, String tableName, Map<String, Object> data)
    {
        try
        {
        	if (connection != null)
        	{
        		boolean      firstEntry = true;
        	    StringBuffer namesText  = new StringBuffer();
        	    StringBuffer valuesText = new StringBuffer();
        	    List<Object> values     = new LinkedList<Object>();
                for (Entry<String, Object> entry: data.entrySet())
                {
                	if (firstEntry)
                		firstEntry = false;
                	else
                	{
                		namesText.append(",");
                		valuesText.append(",");
                	}

                	namesText.append(entry.getKey());
                	valuesText.append("?");
                	values.add(entry.getValue());
                }

        		String            commandText       = "INSERT INTO " + tableName + " (" + namesText + ") VALUES (" +  valuesText + ")";
                PreparedStatement preparedStatement = connection.prepareStatement(commandText);
                for (int index = 1; index <= values.size(); index++)
                {
                	Object value = values.get(index - 1);
                	if (value instanceof String)
                		preparedStatement.setString(index, (String) value);
                	else if (value instanceof Integer)
                		preparedStatement.setInt(index, (Integer) value);
                	else if (value instanceof Short)
                		preparedStatement.setShort(index, (Short) value);
                	else if (value instanceof Long)
                		preparedStatement.setLong(index, (Long) value);
                	else
                		logger.log(Level.WARNING, "Unexpected value type: " + value.getClass().getName());
                }

        		int changeCount = 0;
				try
				{
					changeCount = preparedStatement.executeUpdate();
					preparedStatement.close();
				}
				catch (Throwable throwable)
				{
					logger.log(Level.WARNING, "Unable to insert row into database", throwable);
				}

        		return changeCount == 1;
        	}
        	else
        		return Boolean.FALSE;
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Unable to insert table row", throwable);
            return Boolean.FALSE;
        }
    }
}