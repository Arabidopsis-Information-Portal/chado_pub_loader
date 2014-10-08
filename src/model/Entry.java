package model;

import java.lang.String;
import java.util.*;
import org.apache.log4j.Logger;

public class Entry {
	private String proteinAccession = null;
	private String proteinPrimaryID = null;
	private String featureName = null;
	private String geneLocus = null;
	private String geneID = null;
	private Set <Publication> pubs = new HashSet <Publication>();
	private Set <Integer> pubmedids = new HashSet <Integer> ();
	static Logger LOG = Logger.getLogger(Entry.class);
	public Entry () {
		
	}
	public void setGeneID(String geneID){ this.geneID = geneID;}
	public String getGeneID (){ return geneID;}
	public String getProteinAccession() { return proteinAccession; }
    public void setProteinAccession(String proteinAccession) {
    	this.proteinAccession = proteinAccession; 
    }
    public String getFeatureName() { return featureName; }
    public void setFeatureName (String name ){
    	this.featureName = name;
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

    public Set <Integer> getPubMedIDs () {return pubmedids;}
    public void addPubMedID (Integer pmid){pubmedids.add(pmid);}
    public void setPubMedIDs (Set <Integer> ids ) {this.pubmedids = ids;}
    

}
