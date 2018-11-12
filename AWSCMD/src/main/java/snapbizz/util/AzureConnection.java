package snapbizz.util;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;

public class AzureConnection {
	private static DocumentClient documentClient;
	
	public static DocumentClient getDocumentClient() {
	    
		if (documentClient == null) {
	        documentClient = new DocumentClient(AzureConstants.DOCUMENTDB_END_POINT, AzureConstants.DOCUMENTDB_KEY,
	                ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
	    }

	    return documentClient;
	}
}
