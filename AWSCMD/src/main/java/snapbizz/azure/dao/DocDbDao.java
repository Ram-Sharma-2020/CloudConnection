package snapbizz.azure.dao;

import com.microsoft.azure.documentdb.DocumentCollection;

public interface DocDbDao {
	public DocumentCollection getTodoCollection(String COLLECTION_ID);
}
