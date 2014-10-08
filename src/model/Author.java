package model;
import java.lang.String;
import org.apache.log4j.Logger;

public class Author {
	
	static Logger LOG = Logger.getLogger(Author.class);
	
	protected String name = null;
    public String getName() { return name; }
    public void setName(final String name) { this.name = name; }

    protected String lastName = null;
    public String getLastName() { return lastName; }
    public void setLastName(final String lastName) { this.lastName = lastName; }

    protected String initials = null;
    public String getInitials() { return initials; }
    public void setInitials(final String initials) { this.initials = initials; }

    protected String firstName = null;
    public String getFirstName() { return firstName; }
    public void setFirstName(final String firstName) { this.firstName = firstName; }
    
    private int rank =0;
    public void setRank(int rank){	this.rank = rank;}
    public int getRank(){return this.rank;}
    
    private String affiliation = null;
    public void setAffiliation (String affiliation){ this.affiliation=affiliation; }
    public String getAffiliation(){
    	return affiliation;
    }
// Author can be "Person", "CollectiveName"
    protected String type = null;
    public String getType () {return type;}
    public void setType (String type){
    	this.type=type;
    }
    @Override public String toString() {
    	String toReturn = "";
    	if (type.equals("person")){
    		if(lastName!=null){
    			toReturn += lastName;
    		}
    		//if(firstName != null){
    		//	toReturn += " "+firstName;
    		//}
    		if(initials != null){
    			toReturn += " "+initials;
    		}
    		
    	} else {
    		toReturn = name;
    	}
    	if(affiliation != null){
    		toReturn += "\nAffiliation: "+affiliation;
    	}
    	return toReturn;
    }
}
