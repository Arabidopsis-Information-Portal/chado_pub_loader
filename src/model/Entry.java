package model;

import java.lang.String;
import java.util.*;

public class Entry {
	private String proteinAccession = null;
	private String proteinPrimaryID = null;
	private String geneLocus = null;
	private Set <Publication> pubs = new HashSet <Publication>();
	
	public Entry () {
		
	}
	
	public String getProteinAccession() { return proteinAccession; }
    public void setProteinAccession(String proteinAccession) {
    	this.proteinAccession = proteinAccession; 
    }
    
    public String getProteinPrimaryID() { return proteinPrimaryID; }
    public void setProteinPrimaryID(String proteinPrimaryID) { 
    	this.proteinPrimaryID = proteinPrimaryID; 
    }
    
    public String getGeneLocus() { return geneLocus; }
    public void setGeneLocus(String geneLocus) { this.geneLocus = geneLocus; }
    
    public Set <Publication> getPublications (){return pubs;}
    public void addPublication (Publication pub){pubs.add(pub);}
    public void setPublications (Set <Publication> pubs) {this.pubs=pubs;}


}
