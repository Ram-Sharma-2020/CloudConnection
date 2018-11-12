package snapbizz.aws.service;

import java.util.List;
import java.util.Map;

public interface DmlOperationService {
	void getItem(String tableName, Map<String, Object> parameters);
	void getBatchItems(String tableName, Map<String, Object> parameters);
	void getBatchItems(String tableName, Map<String, Object> parameters, Map<String, Object> attributeKeyParameter);
	void getItemByField(String tableName, Map<String, Object> parameters, String columnName);
	//String batchGetItems(Integer storeId, List<String> partitionTables, String tableName, Long startTime, Long endTime, String header, boolean report);
}
