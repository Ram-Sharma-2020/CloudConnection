package snapbizz.aws.service;

public interface DdlOperationService {
	
	public void getAllTablesName();
	public void getTableInformation(String tableName);
	public void createTable(String tableName);
	public void createTable(String tableName, String partitionKeyName, String partitionKeyType, 
	        String sortKeyName, String sortKeyType, boolean localSecondaryIndex, long readCapacityUnits, long writeCapacityUnits);
	public void deleteTable (String tableName);
	public void alterTable(String tableName, long readCapacityUnits, long writeCapacityUnits); // 1L, 1L
}
