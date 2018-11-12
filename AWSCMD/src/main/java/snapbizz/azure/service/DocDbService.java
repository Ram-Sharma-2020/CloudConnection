package snapbizz.azure.service;

import java.util.List;

import com.microsoft.azure.documentdb.Document;

import snapbizz.model.TodoItem;

public interface DocDbService {
	public TodoItem createTodoItem(TodoItem todoItem, String CollectionName);
	public Document getDocumentById(String id, String CollectionName);
	public List<Document> getDocuments(String collectionName);
	public List<Document> getDocumentsByField(String collectionName, String fieldName);
	public boolean deleteItem(String collectionName, String id);
}
