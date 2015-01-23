/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.jdbc;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;
import com.arjuna.dbplugins.jdbc.postgresql.PostgreSQLDataFlowNodeFactory;

@Startup
@Singleton
public class JDBCDataFlowNodeFactoriesSetup
{
    private static final Logger logger = Logger.getLogger(JDBCDataFlowNodeFactoriesSetup.class.getName());

    @PostConstruct
    public void setup()
    {
        logger.log(Level.FINE, "Setup : PostgreSQL Data Flow Nodes Factory");

        DataFlowNodeFactory postgresqlDataFlowNodeFactory = new PostgreSQLDataFlowNodeFactory("PostgreSQL Data Flow Nodes Factory", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(postgresqlDataFlowNodeFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        logger.log(Level.FINE, "Cleanup : PostgreSQL Data Flow Nodes Factory");

        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("PostgreSQL Data Flow Nodes Factory");
    }

    @EJB(lookup="java:global/databroker/control-core/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
