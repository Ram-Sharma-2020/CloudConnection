package snapbizz.aws.serviceImpl;

import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import snapbizz.aws.service.DdlOperationService;
import snapbizz.util.AwsConnection;

public class DdlOperationServiceImpl implements DdlOperationService {

	public void getAllTablesName() {
		TableCollection<ListTablesResult> tables = AwsConnection.getDynamoDB().listTables();
        Iterator<Table> iterator = tables.iterator();

        System.out.println("Listing table names");

        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println(table.getTableName());
        }
		
	}

	public void getTableInformation(String tableName) {
		 System.out.println("********************* getTableInformation ************************");

	        TableDescription tableDescription = AwsConnection.getDynamoDB().getTable(tableName).describe();
	        System.out.format("Name: %s:\n" + "Status: %s \n"
	                + "Provisioned Throughput (read capacity units/sec): %d \n"
	                + "Provisioned Throughput (write capacity units/sec): %d \n",
	        tableDescription.getTableName(), 
	        tableDescription.getTableStatus(), 
	        tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
	        tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
		
	}

	public void createTable(String tableName) {
		try {

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("Id")
                .withAttributeType("N"));

            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement()
                .withAttributeName("Id")
                .withKeyType(KeyType.HASH)); //Partition key

            CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                    .withReadCapacityUnits(1L)
                    .withWriteCapacityUnits(1L));

            System.out.println("Issuing CreateTable request for " + tableName);
            Table table = AwsConnection.getDynamoDB().createTable(request);

            System.out.println("Waiting for " + tableName
                + " to be created...this may take a while...");
            table.waitForActive();

            getTableInformation(tableName);

        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
		
	}

	public void createTable(String tableName, String partitionKeyName, String partitionKeyType, String sortKeyName,
			String sortKeyType, boolean localSecondaryIndex, long readCapacityUnits, long writeCapacityUnits) {
		 try {
	            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
	            keySchema.add(new KeySchemaElement()
	                .withAttributeName(partitionKeyName)
	                .withKeyType(KeyType.HASH)); //Partition key
	            
	            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
	            attributeDefinitions.add(new AttributeDefinition()
	                .withAttributeName(partitionKeyName)
	                .withAttributeType(partitionKeyType));

	            if (sortKeyName != null) {
	                keySchema.add(new KeySchemaElement()
	                    .withAttributeName(sortKeyName)
	                    .withKeyType(KeyType.RANGE)); //Sort key
	                attributeDefinitions.add(new AttributeDefinition()
	                    .withAttributeName(sortKeyName)
	                    .withAttributeType(sortKeyType));
	            }

	            CreateTableRequest request = new CreateTableRequest()
	                    .withTableName(tableName)
	                    .withKeySchema(keySchema)
	                    .withProvisionedThroughput( new ProvisionedThroughput()
	                        .withReadCapacityUnits(readCapacityUnits)
	                        .withWriteCapacityUnits(writeCapacityUnits));

	            // If this is the Reply table, define a local secondary index
	            if (localSecondaryIndex) {
	                
	                attributeDefinitions.add(new AttributeDefinition()
	                    .withAttributeName("PostedBy")
	                    .withAttributeType("S"));

	                ArrayList<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
	                localSecondaryIndexes.add(new LocalSecondaryIndex()
	                    .withIndexName("PostedBy-Index")
	                    .withKeySchema(
	                        new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH),  //Partition key
	                        new KeySchemaElement() .withAttributeName("PostedBy") .withKeyType(KeyType.RANGE))  //Sort key
	                    .withProjection(new Projection() .withProjectionType(ProjectionType.KEYS_ONLY)));

	                request.setLocalSecondaryIndexes(localSecondaryIndexes);
	            }

	            request.setAttributeDefinitions(attributeDefinitions);

	            System.out.println("Issuing CreateTable request for " + tableName);
	            Table table = AwsConnection.getDynamoDB().createTable(request);
	            System.out.println("Waiting for " + tableName + " to be created...this may take a while...");
	            table.waitForActive();

	        } catch (Exception e) {
	            System.err.println("CreateTable request failed for " + tableName);
	            System.err.println(e.getMessage());
	        }
		
	}

	public void deleteTable(String tableName) {
		Table table = AwsConnection.getDynamoDB().getTable(tableName);
        try {
            System.out.println("Issuing DeleteTable request for " + tableName);
            table.delete();

            System.out.println("Waiting for " + tableName
                + " to be deleted...this may take a while...");

            table.waitForDelete();
        } catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
		
	}

	public void alterTable(String tableName, long readCapacityUnits, long writeCapacityUnits) {
		Table table = AwsConnection.getDynamoDB().getTable(tableName);
        System.out.println("Modifying provisioned throughput for " + tableName);

        try {
            table.updateTable(new ProvisionedThroughput()
                .withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));

            table.waitForActive();
        } catch (Exception e) {
            System.err.println("UpdateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
		
	}

}
