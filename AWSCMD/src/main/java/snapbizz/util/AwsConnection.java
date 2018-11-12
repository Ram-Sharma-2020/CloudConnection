package snapbizz.util;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class AwsConnection {
	private static AWSCredentialsProvider credentialsProvider;
	private static AmazonDynamoDBClient amazondynamoDB;
	private static DynamoDB dynamoDB;
	
	static {
        try {
            // Create the SessionFactory from hibernate.cfg.xml
        	credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
    		//System.out.println("AccessKey: "+ credentialsProvider.getCredentials().getAWSSecretKey().toString());
    		amazondynamoDB = new AmazonDynamoDBClient(credentialsProvider).withRegion(Regions.AP_NORTHEAST_1);
    		dynamoDB = new DynamoDB(amazondynamoDB);
        } catch (Throwable ex) {
            // Make sure you log the exception, as it might be swallowed
            System.err.println("Initial SessionFactory creation failed." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

	public static DynamoDB getDynamoDB() {
		return dynamoDB;
	}
	
	
}
