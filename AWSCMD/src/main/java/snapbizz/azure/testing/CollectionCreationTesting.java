package snapbizz.azure.testing;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.Document;

import ch.qos.logback.core.net.SyslogOutputStream;
import snapbizz.azure.service.DocDbService;
import snapbizz.azure.serviceImpl.DocDbServiceImpl;
import snapbizz.model.TodoItem;

public class CollectionCreationTesting {
	
	private static Gson gson = new Gson();
	
	public static void main(String[] args) {
		DocDbService docDbService = new DocDbServiceImpl();
		
		//toDoCreate(docDbService);
		//readTodoItem(docDbService, "E003");
		System.out.println("*********************Started**************");
		readTodoItems(docDbService, "test_customers");
		//getDocumentsByField(docDbService, "test_TodoCollection", "name");
		//isDeleted(docDbService, "test_TodoCollection", "E003");
		System.out.println("*************** DONE *******************");

	}
	
	public static void toDoCreate(DocDbService docDbService) {
		TodoItem todoItem = new TodoItem();
		todoItem.id = "E003";
		todoItem.name = "pqr";
		todoItem.category = "Category-3";
		todoItem.complete = true;
		docDbService.createTodoItem(todoItem, "test_TodoCollection");
	}
	
	public static void readTodoItem(DocDbService docDbService, String itemId) {
		Document todoItemDocument = docDbService.getDocumentById(itemId, "test_TodoCollection");
		System.out.println("todoItemDocument: "+ todoItemDocument);
	    if (todoItemDocument != null) {
	        // De-serialize the document in to a TodoItem.
	    	TodoItem  todoItem = gson.fromJson(todoItemDocument.toString(), TodoItem.class);
	    	System.out.println(todoItem.id + "   "+ todoItem.name + "   "+ todoItem.category + "   "+ todoItem.complete);
	    
	    }
	}
	
	public static void readTodoItems(DocDbService docDbService, String collectionName) {
	    List<TodoItem> todoItems = new ArrayList<TodoItem>();

	    List<Document> documentList = docDbService.getDocuments(collectionName);

	    // De-serialize the documents in to TodoItems.
	    for (Document todoItemDocument : documentList) {
	        todoItems.add(gson.fromJson(todoItemDocument.toString(),
	                TodoItem.class));
	    }

	    for(TodoItem item: todoItems) {
	    	System.out.println(item.id + "  "+ item.name + "  "+ item.category + "  "+ item.complete);
	    }
	}
	
	public static void getDocumentsByField(DocDbService docDbService, String collectionName, String fieldName) {
		 List<String> todoItems = new ArrayList<String>();
		 List<Document> documentList = docDbService.getDocumentsByField(collectionName, fieldName);
		 for (Document todoItemDocument : documentList) {
		        todoItems.add(gson.fromJson(todoItemDocument.toString(),
		                String.class));
		    }
		 for (String item: todoItems) {
			 System.out.println("Item: "+ item);
		 }
	}
	
	public static void isDeleted(DocDbService docDbService, String collectionName, String id) {
		boolean status = docDbService.deleteItem(collectionName, id);
		System.out.println(id + " has deleted: "+ status );
	}

}
