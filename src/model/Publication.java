package model;

import java.lang.String;
import java.util.*;

public class Publication {
	private String pages=null;
	private String month;
	private String date=null;
	private String volume=null;
	private String issue=null;
	private String type; 
	private String first=null;
	private String last=null;
	private String city = null;
	private String country = null;
	private String institute = null;
	private String number = null;
	private String publisher = null;
	private String db = null;
	private String name = null;
	private String locator = null;
	private Integer cvtermId = null;
	private String pmid = null;
	private int id;
	private String abstractText;
	private String title;
	private String doi;
	private String journal;
	private String firstAuthor;
	ArrayList<Author> authors = new ArrayList();
	private String authorsList = null;
	private String uniquename = null;
	 
	public int getCVTermId(){return cvtermId;}
	public void setCVTermId(int cvtermId){ this.cvtermId = cvtermId;}
	
	public String getLocator(){
		
		return locator;
	}
	public void setLocator(String locator){this.locator=locator;}
	public void setUniqueName (String uniquename){this.uniquename=uniquename;}
	public String getUniqueName (){return uniquename;}
	public String getName() {
		if(name == null && locator != null){
			// try locator
			name=locator;
		}
		return name;
	}
	public void setName(String name){this.name=name;}
	
	public String getCity() {return city;}
	public void setCity (String city ){this.city=city;}
	
	public String getCountry() {return country;}
	public void setCountry (String country ){this.country=country;}
	
	public String getInstitute() {return institute;}
	public void setInstitute (String institute ){this.institute=institute;}
	
	public String getNumber() {return number;}
	public void setNumber (String number ){this.number=number;}
	
	public String getPublisher() {return publisher;}
	public void setPublisher (String publisher ){this.publisher=publisher;}
	
	public String getDB() {return db;}
	public void setDB (String db ){this.db=db;}
			
	public String getType() {return type;}
	public void setType (String type ){this.type=type;}
	
	public String getPages() { 
		if (this.pages == null){
			if(first!=null && last !=null){
				pages = first+"-"+last;
			}
		}
		return pages;
	}
	public void setPages (String pages ){this.pages=pages;}
	// first and last pages
	public String getFirst () {return first;}
	public void setFirst (String first) {this.first = first;}
	
	public String getLast () {return last;}
	public void setLast (String last) {
		this.last = last;
		if (first != null && last != null){
			this.pages = first+"-"+last;
		}
	}
	
	public String getYear(){ return date;}
	public void setYear(String year){ this.date = year;}
	
	public String getVolume() {return volume;}
	public void setVolume (String volume ){this.volume=volume;}
	
	public String getIssue() {return issue;}
	public void setIssue (String issue ){this.issue=issue;}
	
    public String getMonth() { return month; }
    public void setMonth(String month) { this.month = month; }

    public String getPubMedId() { return pmid; }
    public void setPubMedId(String pubMedId) { 
    	this.pmid = pubMedId;
     }

    public String getDOI() { return doi; }
    public void setDOI(String doi) { this.doi = doi; }

    public String getJournal() { return journal; }
    public void setJournal(String journal) { this.journal = journal; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getFirstAuthor() { return firstAuthor; }
    public void setFirstAuthor(String firstAuthor) { this.firstAuthor = firstAuthor; }

    public String getAbstractText() { return abstractText; }
    public void setAbstractText( String abstractText) { this.abstractText = abstractText; }
    
    public java.lang.Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public ArrayList<Author> getAuthors() { return authors; }
    public void setAuthors(ArrayList<Author> authors) { this.authors = authors; }
    public void addAuthors(Author arg) { authors.add(arg); }

    public void setAuthorsList (String authorsList){
    	this.authorsList = authorsList;
    }
    public String getAuthorsList (){
    	int count = authors.size()-1;
    	if(authorsList == null){
    		for (int i = 0; i < count; i++){
    			authorsList +=  authors.get(i).getName()+", ";
    		}
    		authorsList += authors.get(count).getName()+". ";
    	}
    	
    	return authorsList;
    }
    @Override public String toString() { 
    	//return "Publication [type=\""+type+"\", abstractText=\"" + abstractText + "\", doi=\"" + doi + "\", firstAuthor=\"" + firstAuthor + "\", id=\"" + id + "\", issue=\"" + issue + "\", journal=\"" + journal + "\", month=\"" + month + "\", pages=\"" + first+"-"+last + "\", pubMedId=\"" + pubMedId + "\", title=\"" + title + "\", volume=\"" + volume + "\", year=\"" + date + "\"]"; 
    	
    	return pmid+"\t\""+type+"\"\t"+date+"\t\""+name+"\"\t\""+title+"\"\n";
    }
    
    	public void setFieldValue(String fieldName, final Object value) {
        if ("pages".equals(fieldName)) {
            pages = (java.lang.String) value;
        } else if ("last".equals(fieldName)){
        	last = (java.lang.String) value;
        } else if ("first".equals(fieldName)){
        	first = (java.lang.String) value;
        } else if ("date".equals(fieldName)) {
            date = (java.lang.String) value;
        } else if ("volume".equals(fieldName)) {
            volume = (java.lang.String) value;
        } else if ("issue".equals(fieldName)) {
            issue = (java.lang.String) value;
        } else if ("month".equals(fieldName)) {
            month = (java.lang.String) value;
        } else if ("pubMedId".equals(fieldName)) {
        	setPubMedId((java.lang.String) value);
            pmid = getPubMedId();
        } else if ("doi".equals(fieldName)) {
            doi = (java.lang.String) value;
        } else if ("journal".equals(fieldName)) {
            journal = (java.lang.String) value;
        } else if ("title".equals(fieldName)) {
            title = (java.lang.String) value;
        } else if ("firstAuthor".equals(fieldName)) {
            firstAuthor = (java.lang.String) value;
        } else if ("abstractText".equals(fieldName)) {
            abstractText = (java.lang.String) value;
        
        } else if ("authors".equals(fieldName)) {
            authors = (ArrayList<Author>) value;
        } else if ("id".equals(fieldName)) {
            id = (java.lang.Integer) value;
        } else if ("type".equals(fieldName)){
        	type = (java.lang.String) value;
        } else if ("db".equals(fieldName)){
        	db = (java.lang.String) value;
        } else if ("publisher".equals(fieldName)){
        	publisher = (java.lang.String) value;
        } else if ("city".equals(fieldName)){
        	city = (java.lang.String) value;
        } else if ("country".equals(fieldName)){
        	country = (java.lang.String) value;
        } else if ("institute".equals(fieldName)){
        	institute = (java.lang.String) value;
        } else if ("number".equals(fieldName)){
        	number = (java.lang.String) value;
        } else if ("name".equals(fieldName)){
        	name = (java.lang.String)value;
        } else {
            
           System.out.println("Unknown field " + fieldName);
        }
    }

}
