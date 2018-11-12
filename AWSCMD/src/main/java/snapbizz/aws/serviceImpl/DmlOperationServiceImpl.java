package snapbizz.aws.serviceImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.google.gson.Gson;
import com.snapbizz.api.models.APIObjects.Status;

import snapbizz.aws.service.DmlOperationService;
import snapbizz.util.AwsConnection;

public class DmlOperationServiceImpl implements DmlOperationService {
	
	
	public void getItemByField(String tableName, Map<String, Object> parameters, String columnName) {
		System.out.println("************************getItemByField ************************");
		Table table = AwsConnection.getDynamoDB().getTable(tableName);
		List<Object> items = new ArrayList();
		Integer hashkey = (Integer) parameters.get("hashKey");
		
			QuerySpec spec = new QuerySpec()
					.withHashKey("store_id", hashkey)
					.withExclusiveStartKey("store_id", hashkey, parameters.get("rangeKey").toString(), parameters.get("rangeKeyValue"));
			ItemCollection<QueryOutcome> result = null;
			try {
				result = table.query(spec);		
				Iterator<Item> iterator = result.iterator();
				Item item = null;
				while (iterator.hasNext()) {
				    item = iterator.next();
				    System.out.println(item.get(columnName));
				    //items.add(item.get(columnName));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

	public void getItem(String tableName, Map<String, Object> parameters) {
		Item item = null;
		DynamoDB dynamoDB = AwsConnection.getDynamoDB();
		Integer hashKey = (Integer) parameters.get("hashKey");
		try {
			if (parameters.get("attributeKey") == null) {
				item = dynamoDB.getTable(tableName).getItem("store_id", hashKey, parameters.get("rangeKey").toString(), parameters.get("rangeKeyValue"));
			}
			else {
				item = dynamoDB.getTable(tableName).getItem("store_id", hashKey, parameters.get("rangeKey").toString(), parameters.get("rangeKeyValue"))
						.with(parameters.get("attributeKey").toString(), parameters.get("attributeKeyValue"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (item != null) {
			System.out.println("Following item has fetched...");
			System.out.println(item);
		}
		
	}

	public void getBatchItems(String tableName, Map<String, Object> parameters) {

		DynamoDB dynamoDB = AwsConnection.getDynamoDB();
		Table table = dynamoDB.getTable(tableName);
		String rangeKey = parameters.get("rangeKey").toString();
		Integer hashkey = (Integer) parameters.get("hashKey");
		
			QuerySpec spec = new QuerySpec()
					.withHashKey("store_id", hashkey)
					.withExclusiveStartKey("store_id", hashkey, rangeKey, parameters.get("rangeKeyValue"));
					
			ItemCollection<QueryOutcome> result = null;
			try {
				result = table.query(spec);
				Iterator<Item> iterator = result.iterator();
				
				Item item = null;
				while (iterator.hasNext()) {
				    item = iterator.next();
				    //item.removeAttribute("store_id");	
				    //System.out.println(item.toJSONPretty()); 
				    System.out.println(item);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				
			}
		}

	public void getBatchItems(String tableName, Map<String, Object> parameters,
			Map<String, Object> attributeKeyParameter) {
		
		DynamoDB dynamoDB = AwsConnection.getDynamoDB();
		Integer hashKey = (Integer) parameters.get("hashKey");
		Table table = dynamoDB.getTable(tableName);
		QuerySpec spec = new QuerySpec();
		ValueMap valueMap = new ValueMap();
						
		spec.withHashKey(new KeyAttribute("store_id", hashKey));
		if (parameters.get("rangeKey") != null)
			spec.withExclusiveStartKey("store_id", hashKey, parameters.get("rangeKey").toString(), parameters.get("rangeKeyValue"));
		Iterator attributeIterator = attributeKeyParameter.entrySet().iterator();
		while (attributeIterator.hasNext()) {
			Map.Entry keyPair = (Map.Entry)attributeIterator.next();
			spec.withFilterExpression("#key = :value").withNameMap(new NameMap()
					.with("#key", (String) keyPair.getKey()));
			valueMap.with(":value", keyPair.getValue());	    
		}
		
		spec.withValueMap(valueMap);
					
		ItemCollection<QueryOutcome> result = null;
		try {
			result = table.query(spec);

			Iterator<Item> iterator = result.iterator();
			Item item = null;
			while (iterator.hasNext()) {
			    item = iterator.next();
			    System.out.println(item.toJSONPretty());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
	}

	
	/*public String batchGetItems(Integer storeId, List<String> partitionTables, String tableName, Long startTime, Long endTime,
			String header, boolean report) {
		Status status = new Status();
		Gson gson = new Gson();
		String items = "";
		String strResponse = "";
		int tableCount = 0;
		boolean exit = false;
		for (int i = 0; i < 1000/100; i++) { //Since dynamo has a max fetch of 100	
			int resultSize = 0;
			int fetchSize;
			if (i > 0) //When i > 0, epoch_time >= :startTime will fetch the repeated item to avoid that we have DYNAMO_MAX_FETCH + 1
				fetchSize = 100 + 1;
			else
				fetchSize = 100;
			while(resultSize < 100) {
				ItemCollection<QueryOutcome> results = null;
				try {
					results = getItems(storeId, startTime, fetchSize, partitionTables.get(tableCount));
				} catch (Exception e) {
					e.printStackTrace();
					status.status = Status.STATUS_RETRIEVE_ERROR;
					return gson.toJson(status);
				}
				String table = tableName;
				if(tableName.contains("test_"))
					table = tableName.substring(tableName.indexOf("_") + 1);
				
				else
					items = getJson(items, results, fetchSize, endTime, table, "", report);
				int itemCount = results.getAccumulatedItemCount();
				resultSize += itemCount;
				if(resultSize < DYNAMO_MAX_FETCH) { //To fetch remaining data from other partitioned tables
					tableCount++;
					fetchSize = fetchSize - itemCount;
					if(tableCount < partitionTables.size()) {
						long tableTimestamp = Long.parseLong(partitionTables.get(tableCount).replace(tableName, ""));
						if (endTime != 0L && endTime <= tableTimestamp) {
							exit = true;
							break;
						}
					}
					else {
						exit = true;
						break;
					}
				}
			}
			if (exit || itemCount >= MAX_RESULTS)
				break;
			startTime = lastRetrievedTimestamp; //To get the next 100 items
		}
		if(tableName.contains("test_"))
			tableName = tableName.substring(tableName.indexOf("_") + 1);
		if(report) {
			if (itemCount >= MAX_RESULTS)
				return strResponse + "offset=" + lastRetrievedTimestamp;
			else 
				return strResponse + "offset=";
		}
		if (items.isEmpty())
			items = "{\"" + tableName + "List\" : [],\"offset\": null ";
		else if (itemCount >= MAX_RESULTS)
			items += "],\"offset\": " + lastRetrievedTimestamp;
		else
			items += "],\"offset\": null";
		items += ",\"status\":\"" + Status.STATUS_SUCCESS + "\"}";
		
		return items;
		return null;
	}
	
	private ItemCollection<QueryOutcome> getItems(Integer storeId, Long startTime, int fetchSize, String tableName){
		DynamoDB dynamoDB = AwsConnection.getDynamoDB();
		QuerySpec spec = new QuerySpec();
		spec.withKeyConditionExpression("store_id = :storeId AND epoch_time >= :startTime");
		spec.withValueMap(new ValueMap()
				.withLong(":startTime", startTime)
				.withLong(":storeId", storeId));
		spec.withConsistentRead(true);
		spec.withSelect(Select.ALL_ATTRIBUTES);
		spec.setMaxResultSize(fetchSize);
		ItemCollection<QueryOutcome> results = null;
		results = dynamoDB.getTable(tableName).getIndex("InvoiceByTime").query(spec);
		return results;
	}
	*/

}
