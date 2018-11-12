package snapbizz.azure.serviceImpl;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;

import snapbizz.azure.dao.DocDbDao;
import snapbizz.azure.daoImpl.DocDbDaoImpl;
import snapbizz.azure.service.DocDbService;
import snapbizz.model.TodoItem;
import snapbizz.util.AzureConnection;

public class DocDbServiceImpl implements DocDbService {

	private static Gson gson = new Gson();
	DocDbDao docDbDao = new DocDbDaoImpl();
	
	public TodoItem createTodoItem(TodoItem todoItem, String CollectionName) {
		// Serialize the TodoItem as a JSON Document.
	    Document todoItemDocument = new Document(gson.toJson(todoItem));

	    // Annotate the document as a TodoItem for retrieval (so that we can
	    // store multiple entity types in the collection).
	    todoItemDocument.set("entityType", "todoItem");

	    try {
	        // Persist the document using the DocumentClient.
	        todoItemDocument = AzureConnection.getDocumentClient().createDocument(docDbDao.getTodoCollection(CollectionName)
	.getSelfLink(), todoItemDocument, null,
	                false).getResource();
	    } catch (DocumentClientException e) {
	        e.printStackTrace();
	        return null;
	    }

	    return gson.fromJson(todoItemDocument.toString(), TodoItem.class);
	}

	@Override
	public Document getDocumentById(String id, String CollectionName) {
		List<Document> documentList = AzureConnection.getDocumentClient()
	            .queryDocuments(docDbDao.getTodoCollection(CollectionName).getSelfLink(),
	                    "SELECT * FROM root r WHERE r.id='" + id + "'", null)
	            .getQueryIterable().toList();

	    if (documentList.size() > 0) {
	        return documentList.get(0);
	    } else {
	        return null;
	    }
	}

	@Override
	public List<Document> getDocuments(String collectionName) {
	    // Retrieve the TodoItem documents
	    List<Document> documentList = AzureConnection.getDocumentClient()
	            .queryDocuments(docDbDao.getTodoCollection(collectionName).getSelfLink(),
	                    "SELECT * FROM root r WHERE r.entityType = 'todoItem'",
	                    null).getQueryIterable().toList();
	    
	    return documentList;
	}

	@Override
	public List<Document> getDocumentsByField(String collectionName, String fieldName) {
		List<Document> documentList = AzureConnection.getDocumentClient().queryDocuments(docDbDao.getTodoCollection(collectionName).getSelfLink(),
				"SELECT " + fieldName + " FROM root r WHERE r.entityType = 'todoItem' t",
                null).getQueryIterable().toList();	            
		return documentList;
	}

	@Override
	public boolean deleteItem(String collectionName, String id) {
		// Query for the document to retrieve the self link.
	    Document deleteItem = getDocumentById(id, collectionName);

	    try {
	        // Delete the document by self link.
	    	AzureConnection.getDocumentClient().deleteDocument(deleteItem.getSelfLink(), null);
	    } catch (DocumentClientException e) {
	        e.printStackTrace();
	        return false;
	    }

	    return true;
		
	}

}
