package loader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.io.*;
import java.util.LinkedHashMap;

import chado.*;
import model.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class PubLoader {
	private Connection conn;
	private int pubmedDBId;
	private int internalDBId;
	private ChadoDBxref dbxref = null;
	private ChadoCVFactory cvFactory;
	private final String defaultCV = "tripal_pub";
	private Map <String,Integer> tripalPubTerms = new HashMap();
	private PublicationFactory pubFactory;
	private ChadoFeatureFactory featureFactory;
	private int featureType=0;
	private int proteinFeatureType=0;
	public PubLoader (Connection conn)throws SQLException{
		this.conn = conn;
		// we may search between next CVs: pub_type, pub_property, pub_relationship, tripal_pub
		//18 pub_type	Contains types of publications. This can be used if the tripal_pub vocabulary (which is default for publications in Tripal) is not desired.
		//19 pub_property	Contains properties for publications. This can be used if the tripal_pub vocabulary (which is default for publications in Tripal) is not desired
		//20 pub_relationship	Contains types of relationships between publications.
		//17 tripal_pub
		// if you need to write new CV terms, use 
		//5 autocreated	Terms that are automatically inserted by loading software
		//
		this.cvFactory = new ChadoCVFactory(conn);
		ChadoCV cv = cvFactory.getChadoCV(defaultCV);
		Map<Integer,ChadoCVTerm> allTerms = cv.getTerms();
		// we need cvterm_id for the given term...
		for (Integer key: allTerms.keySet()){
			//There is a mess with upper/lower case in the naming of terms between tripal and uniprot
			String value = allTerms.get(key).getName().toLowerCase();
			tripalPubTerms.put(value, key);
			//System.out.println("type: "+value+", id=" + key);
		
		}
		this.dbxref = new ChadoDBxref(conn);
		dbxref.setDB();
		cvFactory.setDBXrefRef(dbxref);
		this.pubmedDBId = dbxref.getDBid("PMID");
		//System.out.println("db_id for PMID "+ this.pubmedDBId);
		this.internalDBId = dbxref.getDBid("internal");
		//System.out.println("db_id for \"internal\" "+ internalDBId);
		pubFactory = new PublicationFactory(conn);
		featureFactory = new ChadoFeatureFactory (conn);
		this.featureType = cvFactory.getIDForTerm("gene");
		this.proteinFeatureType=cvFactory.getIDForTerm("polypeptide");
		checkTerms(tripalPubTerms);
	}
	public int getFeatureType (String featName) throws SQLException { 
		int typeId = 0;
		if("gene".equalsIgnoreCase(featName)){
			typeId = featureType;
		} else if (featName.equalsIgnoreCase("protein") ||featName.equalsIgnoreCase("polypeptide") ){
			typeId = proteinFeatureType;
			
		} else {
			typeId = cvFactory.getIDForTerm(featName);
		}
		return typeId;
	}
	
	private void checkTerms(Map terms) throws SQLException {
		String[] termList = {"authors","doi","publication date","citation","publication dbxref"};
		for (int i=0; i<termList.length;i++){
			if(tripalPubTerms.get(termList[i]) == null){
				//check if another CV has that term
				int termID = cvFactory.getIDForTerm(termList[i]);
				if(termID == 0){
					termID = cvFactory.addTerm(internalDBId, "autocreated",termList[i]);
				}
				tripalPubTerms.put(termList[i], termID);
			}
		}
	}
	public Map<String,String> getCVTerms (String[] terms){
		Map<String,String> cvterms = new LinkedHashMap<String, String>();
		
		 return cvterms;
	}
	////public Integer getCVTerm(String term){
	//	return 5;
	//}
	public Entry processLine(String line) 
			throws SQLException {
		String[] toks = line.split("\t", -1);
		Publication pub = new Publication();
		Entry entry = new Entry ();
		if(!toks[3].equals("null")){
			pub.setPubMedId(toks[3]);
		}
		if(!toks[4].equals("null")){
			pub.setType(removeQuotes(toks[4]));
		}
		if(!toks[5].equals("null")){
			pub.setYear(toks[5]);
		}
		if(!toks[6].equals("null")){
			pub.setName(removeQuotes(toks[6]));
		}
		if(!toks[7].equals("null")){
			pub.setTitle(removeQuotes(toks[7]));
		}
		if(!toks[0].equals("null")){
			entry.setGeneLocus (toks[0]);
		}
		if(!toks[1].equals("null")){
			entry.setProteinAccession(toks[1]);
		}
		if(!toks[2].equals("null")){
			entry.setProteinPrimaryID(toks[2]);
		}
		entry.addPublication(pub);
		return entry;
	}
	//Processor for entry
	public int processEntry (Entry entry, boolean loadPM) throws SQLException {
		Set<Publication> pubs = entry.getPublications();
		int processed = 0;
		for (Publication pub: pubs){
			if (!loadPM && pub.getPubMedId() != null && !pub.getPubMedId().isEmpty() ){
				System.out.println("Skipping PubMed ID="+pub.getPubMedId());
			} else {
				int pubID=0;
				//System.out.println("Processing Title:"+pub.getTitle());
				pubID = updatePublication(pub);	
				processed++;
				int featureID = featureFactory.getFeatureId(featureType, entry.getGeneLocus() );
				//make link with gene for pubId (existing or just created)
				if (featureID > 0 && pubID > 0) {
					int featurePubID = featureFactory.setFeaturePubRecord(featureID, pubID);
					if(featurePubID > 0){
						//System.out.println("Feature_pub for "+entry.getGeneLocus()+ " and pub_id="+pubID);
					} else {
						System.out.println("Can't create feature_pub for "+entry.getGeneLocus()+ " and pub_id="+pubID);
					}
				} else {
					System.out.println("No feature_pub record for "+ entry.getGeneLocus() +" and pub_id="+pubID);
				}
				int proteinFeatureID = featureFactory.getFeatureId(proteinFeatureType, entry.getProteinAccession() );
				
				if (proteinFeatureID > 0 && pubID > 0) {
					int featurePubID = featureFactory.setFeaturePubRecord(proteinFeatureID, pubID);
					if(featurePubID > 0){
						//System.out.println("Feature_pub for "+entry.getProteinAccession()+ " and pub_id="+pubID);
					} else {
						//System.out.println("Can't create feature_pub for "+entry.getProteinAccession()+ " and pub_id="+pubID);
					}
				} else {
					System.out.println("No feature_pub record for "+ entry.getProteinAccession() +" and pub_id="+pubID);
				}
				//link protein with gene 
				//if(featureID>0 && proteinFeatureID>0){
					//in gmod chado automatically generated proteins  (gff file load) 
				//are linked with mRNAs in the feature_relationship table
				//type of relationship = "derives_from", object is mRNA and subject is polypeptide
				// other fields: rank=0; value=NULL
				//generated protein will have name=polypeptide_auto<feature_id>
				//uniquename=auto<feature_id>
				//}
			}
		}
		return processed;
	}
	
	public int updatePublication (Publication pub) throws SQLException {
		int pubID = 0;
		if(tripalPubTerms.containsKey(pub.getType())){
			pub.setCVTermId(tripalPubTerms.get(pub.getType()));
		} else {
		//check if that term exists under different CV:
			String term = prepareTerm(pub.getType());
			int cvTermID = cvFactory.getIDForTerm(term);
			if(cvTermID == 0){
				// we need to create new record for term
				//let's do it under cv name "autocreated"
				cvTermID = cvFactory.addTerm(internalDBId, "autocreated", term);
				tripalPubTerms.put(pub.getType().toLowerCase(), cvTermID);
			}
			pub.setCVTermId(cvTermID);
		}
		pubID = pubFactory.getPubId(pub, dbxref);
		if(pubID > 0){
			System.out.println("exists record for "+pub.toString());
		} else {	
			System.out.println("Create new record for \""+pub.toString()+"\"");
			//make new pub record
			pubID = pubFactory.addPub(pub);
		}
		if(pub.getPubMedId() != null){
			int pmDBxref = pubFactory.addPubDBXref(dbxref, "PMID",pub.getPubMedId(), pubID);
			if(tripalPubTerms.get("publication dbxref") == null){
				System.out.println("dbxref term is not found");
				for (String term: tripalPubTerms.keySet()){
					System.out.println(tripalPubTerms.get(term)+": "+term);
				}
			}
			String pubPropPMID = "PMID:"+pub.getPubMedId();
			pubFactory.updatePubProp(tripalPubTerms.get("publication dbxref"), pubID, pubPropPMID);
			//pubFactory.updatePubProp(tripalPubTerms.get("publication dbxref"), pubID, pub.getPubMedId());
		}
		if(pub.getDOI() != null){
			pubFactory.updatePubProp(tripalPubTerms.get("doi"), pubID, pub.getDOI());
		}
		ArrayList<Author> authors = pub.getAuthors();		
		if(pub.getAuthorsList() != null){
			pubFactory.addAuthors(authors, pubID);
			pubFactory.updatePubProp(tripalPubTerms.get("authors"), pubID, pub.getAuthorsList());
		}
		if(pub.getYear() !=null){
			pubFactory.updatePubProp(tripalPubTerms.get("publication date"), pubID, pub.getYear());
		}
		if(pub.getUniqueName()!=null){
			pubFactory.updatePubProp(tripalPubTerms.get("citation"), pubID, pub.getUniqueName());
		}
		return pubID;
	}
	//make first letter in every word upper case
	public String prepareTerm (String term){
		StringBuffer res = new StringBuffer();

	    String[] strArr = term.split(" ");
	    for (String str : strArr) {
	        char[] stringArray = str.trim().toCharArray();
	        stringArray[0] = Character.toUpperCase(stringArray[0]);
	        str = new String(stringArray);

	        res.append(str).append(" ");
	    }
	    return res.toString().trim();
	}
	public String removeQuotes(String s){
		s=s.replace("\"","");
		return s;
	}

	public static void main (String[] args) throws Exception {
		
		String user = null; 
		String pass = null; 
		String db = null; 
		String  server = null; 
		Options options = new Options ();
		options.addOption("file", true, "input file");
		options.addOption("D", true, "chado database name,tripal flavor");
		options.addOption("S", true, "database server");
		options.addOption("U", true, "user, able to write data into database");
		options.addOption("P", true, "password for db user");
		options.addOption("h", false, "help");
		options.addOption("pubmed",false, "load publication even it has PMID");
		CommandLineParser cmparser = new PosixParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = cmparser.parse(options, args);
		String filename = null;
		boolean loadPM = false;
		if(cmd.hasOption("h")){
			formatter.printHelp("PubLoader", options);
			System.exit(0);
		} else if (cmd.hasOption("file")){
			filename = cmd.getOptionValue("file");
			File file = new File (filename);
			if(file.exists() && !file.isDirectory()){
				System.out.println("File to process: "+filename);
			} else {
				System.out.println("There is a problem with file: "+ filename );
				formatter.printHelp("PubLoader", options);
				System.exit(0);
			}
		} else {
			System.out.println("Uniprot file to process is required");
			formatter.printHelp("PubLoader", options);
			System.exit(0);
		}
		if(!cmd.hasOption("D") || !cmd.hasOption("S") || !cmd.hasOption("U") || !cmd.hasOption("P")){
			System.out.println("Database name, server, user and password are required if you want data to be loaded into db.");
			formatter.printHelp("UniprotConverter", options);
			System.exit(0);
		}else{
			db = cmd.getOptionValue("D");
			server = cmd.getOptionValue("S");
			user = cmd.getOptionValue("U");
			pass = cmd.getOptionValue("P");
		}
		if(cmd.hasOption("pubmed")){
			loadPM=true;
		}
		BufferedReader br = null; 
		Connection conn = DatabaseUtil.connect(server, db, user, pass, null);
		
		if (conn != null){
			System.out.println ("connection to "+db+ " is available");
			PubLoader pubLoader = new PubLoader(conn);
			
			try { 
		        br = new BufferedReader(new FileReader(filename)); 
		        String line;
		        while ((line = br.readLine()) != null) { 
		          if(line.trim().length() == 0){
		        	  continue;
		          }
		          
		          String[] toks = line.split("\t", -1); // don't truncate empty fields
		          if(toks.length != 8){
		        	  System.out.println("Wrong format for the file:"+args[0]);
		        	  System.out.println("Number of fields="+toks.length);
		        	  System.exit(1);
		          }
		          Entry entry = pubLoader.processLine(line);
		          System.out.println("Processing entry: "+entry.getProteinAccession());
		          pubLoader.processEntry(entry, loadPM);
		          //pubLoader.writePublication(entry);
		          //Create Publication object
		          //Publication pub = new Publication(toks);
		          
		          //for (int i=0; i<toks.length;i++){
		        	//  System.out.print(toks[i]+"\t");
		          //}
		         // System.out.println();
		          //m.put(toks[0], toks[1]); 
		        } 
		      } finally { 
		        if (br != null) br.close(); // don't throw an NPE because the file wasn't found.
		      } 

		
			
			System.out.println("Close connection for now");
			conn.close();
		}
	}

}
