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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kududb.client.Insert;
import org.kududb.client.KuduTable;
import org.kududb.client.PartialRow;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;

public class KuduTypeMutationTransformer extends MutationTransformer {
	
	public static final Log LOG = LogFactory
			.getLog(KuduTypeMutationTransformer.class.getName());


	public KuduTypeMutationTransformer() {
	}

	@Override
	public List<Insert> getInsertCommand(Map<String, Object> fields) 
			throws IOException {
		
		KuduTable table = getKuduTable();
		Schema schema = table.getSchema();
		List<ColumnSchema> columnSchemaList = schema.getColumns();
		Map<String, Type> columnTypeMap = new HashMap<String, Type>();
		Insert insert = table.newInsert();
		PartialRow row = insert.getRow();
		
		for (ColumnSchema columnSchema: columnSchemaList) {
			columnTypeMap.put(columnSchema.getName(), columnSchema.getType());
		}
		
		for (Map.Entry<String, Object> fieldEntry: fields.entrySet()) {
			
			String colName = fieldEntry.getKey();
			if (!columnTypeMap.containsKey(colName)) {
				throw new IOException("Could not find column  " + colName +
				          " of type in table " + table.getName());
			}
			Type columnType = columnTypeMap.get(colName);
			Object val = fieldEntry.getValue();
			
	
			
			switch(columnType.getName()) {
			case "binary":
				LOG.info("Updating binary columnName:" + colName);
				row.addBinary(colName, (byte[])val);
				break;
			case "bool":
				LOG.info("Updating bool columnName:" + colName);
				row.addBoolean(colName, (boolean) val);
				break;
			case "double":
				LOG.info("Updating double columnName:" + colName);
				row.addDouble(colName, (Double) val);
				break;
			case "float":
				LOG.info("Updating float columnName:" + colName);
				row.addFloat(colName, (Float) val);
				break;
			case "int16":				
			case "int32":			
			case "int64":			
			case "int8":
				LOG.info("Updating int columnName:" + colName);
				row.addInt(colName, (int) val);
				break;
			case "string":
				LOG.info("Updating string columnName:" + colName);
				row.addString(colName, (String) val);
				break;
			case "timestamp":
				LOG.info("Updating timestamp columnName:" + colName);
				row.addLong(colName, (Long) val);
				break;
			default:
				LOG.error("No Mapping found for:" + colName + " for type: " + columnType.getName());
				
			}			
		}
		
		return Collections.singletonList(insert);
	}

	
}
