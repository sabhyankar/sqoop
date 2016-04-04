/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.kudu;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.KuduClient;

import java.io.IOException;
import java.util.*;

/**
 * Creates a Kudu table based on the schema of the source
 * table being imported.
 */
public class KuduTableWriter {

    public static final Log LOG = LogFactory.getLog(
            KuduTableWriter.class.getName());

    private KuduClient kuduClient;
    private SqoopOptions opts;
    private ConnManager connMgr;
    private String inputTable;
    private String outputTable;
    private Configuration config;

    /**
     * Creates a new KuduTableWriter to create a Kudu table.
     * @param opts program-wide options
     * @param connMgr the connection manager used to describe the table.
     * @param inputTable the name of the table to load.
     * @param outputTable the name of the Kudu table to create.
     * @param config the Hadoop configuration to use to connect to the dfs
     */
    public KuduTableWriter(final SqoopOptions opts, final ConnManager connMgr,
                           final KuduClient kuduClient, final String inputTable,
                           final String outputTable, final Configuration config
    ) {
        this.opts = opts;
        this.connMgr = connMgr;
        this.kuduClient = kuduClient;
        this.inputTable = inputTable;
        this.outputTable = outputTable;
        this.config = config;
    }

    private Map<String, Integer> externalColTypes;

    /**
     * Set the column type map to be used.
     * (dependency injection for testing; not used in production.)
     */
    public void setColumnTypes(Map<String, Integer> colTypes) {
        this.externalColTypes = colTypes;
        LOG.debug("Using test-controlled type map");
    }

    /**
     * Get the column names to import.
     */
    private String [] getColumnNames() {
        String [] colNames = opts.getColumns();
        if (null != colNames) {
            return colNames; // user-specified column names.
        } else if (null != externalColTypes) {
            // Test-injection column mapping. Extract the col names from this.
            ArrayList<String> keyList = new ArrayList<String>();
            for (String key : externalColTypes.keySet()) {
                keyList.add(key);
            }

            return keyList.toArray(new String[keyList.size()]);
        } else if (null != inputTable) {
            return connMgr.getColumnNames(inputTable);
        } else {
            return connMgr.getColumnNamesForQuery(opts.getSqlQuery());
        }
    }

    /**
     * @return Schema for Kudu table
     */
    private Schema getTableSchema() throws IOException {
        Map<String, Integer> columnTypes;

        // TODO Add a MapColumnKudu
        Properties userMapping = opts.getMapColumnHive();

        if (externalColTypes != null) {
            // Use pre-defined column types.
            columnTypes = externalColTypes;
        } else {
            // Get these from the database.
            if (null != inputTable) {
                columnTypes = connMgr.getColumnTypes(inputTable);
            } else {
                columnTypes = connMgr.getColumnTypesForQuery(opts.getSqlQuery());
            }
        }

        String [] colNames = getColumnNames();

        // Check that all explicitly mapped columns are present in result set
        for(Object column : userMapping.keySet()) {
            boolean found = false;
            for(String c : colNames) {
                if (c.equals(column)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new IllegalArgumentException("No column by the name " + column
                        + "found while importing data");
            }
        }
        int numberOfCols = colNames.length;
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>(numberOfCols);

        for (String col : colNames) {

            Integer colType = columnTypes.get(col);
            Type kuduColType = null;

            // Does a mapping exist in user specified map?
            // TODO


            if (kuduColType == null) {
                kuduColType = connMgr.toKuduType(inputTable, col, colType);
            }

            if (null == kuduColType) {
                throw new IOException("Kudu does not support the SQL type for column "
                        + col);
            }

            if (KuduTypes.isKuduTypeImprovised(colType)) {
                LOG.warn(
                        "Column " + col + " had to be cast to a less precise type in Kudu");
            }

            // TODO add for compression type, encoding and key columns
            ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder(col,kuduColType)
                    .build();
            columns.add(columnSchema);
        }

        return new Schema(columns);
    }

    /**
     * Creates a new Kudu Table based on the Schema generated from the input source table/query
     * @throws IOException
     */
    public void createKuduTable() throws IOException{

        LOG.info("Creating Kudu table: " + outputTable);
        try {
            Schema schema = getTableSchema();
            if ( null != schema ) {
                LOG.debug("Table schema for kudu table " + schema.toString());
            }

            // TODO modify createTable to use the signature with CreateTableOptions
            kuduClient.createTable(outputTable,schema);

        } catch (Exception e) {
            LOG.error("Error creating Kudu table: " + this.outputTable);
            throw new IOException("Error creating Kudu table: " + this.outputTable +
            " with exception: " + e.getMessage());
        }
    }

}
