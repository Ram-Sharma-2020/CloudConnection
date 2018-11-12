package snapbizz.model;

import java.io.Serializable;

public class TodoItem implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public String category;
    public boolean complete;
    public String id;
    public String name;
    
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public boolean isComplete() {
		return complete;
	}
	public void setComplete(boolean complete) {
		this.complete = complete;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
    
}
