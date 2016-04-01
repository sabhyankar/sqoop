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
			case "BINARY":
				row.addBinary(colName, (byte[])val);
				break;
			case "BOOL":
				row.addBoolean(colName, (boolean) val);
				break;
			case "DOUBLE":
				row.addDouble(colName, (Double) val);
				break;
			case "FLOAT":
				row.addFloat(colName, (Float) val);
				break;
			case "INT16":
				row.addInt(colName, (int) val);
				break;
			case "INT32":
				row.addInt(colName, (int) val);
				break;
			case "INT64":
				row.addInt(colName, (int) val);
				break;
			case "INT8":
				row.addInt(colName, (int) val);
				break;
			case "STRING":
				row.addString(colName, (String) val);
				break;
			case "TIMESTAMP":
				row.addLong(colName, (Long) val);
				break;
			}			
		}
		
		return Collections.singletonList(insert);
	}

	
}
