package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import loader.DatabaseUtil;
import loader.PubLoader;
import loader.PublicationFactory;
import model.Entry;
import model.Publication;
import model.Author;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import chado.ChadoFeatureFactory;

import javax.xml.parsers.*;
/**
 * Class to load publications into chado database
 * 
 * @author svetlana
 *
 */
public class PubMedXMLParser extends DefaultHandler {
	private Stack<String> stack = new Stack<String>();
    private String attName = null;
    private StringBuffer attValue = null;
    private Publication pub = null;
    private Author author = null;
    private int rank=0;
    private PubLoader pubLoader = null;
    private boolean loadDB = false;
    static Logger LOG = Logger.getLogger(PubMedXMLParser.class);
    private Hashtable<String, Integer> processedPMIDs = new Hashtable<String, Integer> ();
    
    
	public PubMedXMLParser () {
		super();
		
	}
	public void setPubLoader (PubLoader pubLoader) throws SQLException {
		this.pubLoader = pubLoader;
	}
	public void setLoadDB(boolean loadDB){
		this.loadDB = loadDB;
	}
	public void startDocument () throws SAXException {
		Date date = new Date ();
		LOG.info("Start document "+ date.toString());
	}
	@Override
	public void startElement(String namespaceURI, String localName, String qName,  Attributes attrs)
		throws SAXException {
		String previousQName = null;
		attName = null;
		if (!stack.isEmpty()) {
            previousQName = stack.peek();
        }
		if("PubmedArticle".equals(qName)){
			pub = new Publication();
			rank = 0;
		} else if ("PublicationType".equals(qName) && "PublicationTypeList".equals(previousQName)){
			attName = "type";
		}else if ("ArticleId".equals(qName) && "doi".equals(attrs.getValue("IdType"))) {
            attName = "doi";
        } else if ("DescriptorName".equals(qName)) {
            attName = "meshTerm";
        }else if ("AuthorList".equals(previousQName) && "Author".equals(qName) ){
        	author = new Author();
        } else if ("Author".equals(previousQName) && stack.search("AuthorList")==2){
        	attName=qName; //LastName, ForeName, Initials, Affilliation, CollectiveName
        	
        }
		super.startElement(namespaceURI, localName, qName, attrs);
        stack.push(qName);
        LOG.debug("Stack startElement: "+stack);
        attValue = new StringBuffer();
	}
	public void endElement(String uri, String localName, String qName)
		     throws SAXException  {
		super.endElement(uri, localName, qName);
		stack.pop();
		String previousQName = null;
		if(!stack.isEmpty()){
			previousQName = stack.peek();
		}
		if ("ERROR".equals(attName)) {
			LOG.error("Unable to retrieve pubmed record: " + attValue);
        } else if ("PMID".equals(qName) && "MedlineCitation".equals(previousQName)) {
            String pubMedId = attValue.toString();
            Integer pubMedIdInteger;
            try {
                pubMedIdInteger = Integer.valueOf(pubMedId);
                pub.setPubMedId(pubMedId);
            } catch (NumberFormatException e) {
                throw new RuntimeException("got non-integer pubmed id from NCBI: " + pubMedId);
            }
        } else if ("PublicationType".equals(qName) && attName.equals("type")){
        	
        	if (pub.getType() == null){
        		pub.setType(attValue.toString());
        		LOG.debug("Type="+attValue.toString());
        	}
        } else if (!stack.isEmpty() && "PubDate".equals(stack.peek())) {
            if ("Year".equalsIgnoreCase(qName)){
            	String year = attValue.toString();
            	pub.setYear(year);
            } else if ("Month".equalsIgnoreCase(qName)){
            	String month = attValue.toString();
            	pub.setMonth(month);
            } else if ("Day".equalsIgnoreCase(qName)){
            	String day = attValue.toString();
            	pub.setDay(day);
            }
        } else if ("Journal".equals(previousQName)) {
        	if("Title".equals(qName)){
        		pub.setName(attValue.toString());
        	}
        	if("ISOAbbreviation".equals(qName)){
        		if(pub.getName() == null){
        			pub.setName(attValue.toString());
        		}
        	}
        }else if ("Volume".equals(qName)){
        	pub.setVolume(attValue.toString());
        } else if ("Issue".equals(qName)){
        	pub.setIssue(attValue.toString());
        	
        } else if ("ArticleTitle".equals(qName)) {
            pub.setTitle(attValue.toString());
        
        } else if ("MedlinePgn".equals(qName)) {
            pub.setPages(attValue.toString());
        } else if ("AbstractText".equals(qName)) {
            String abstractText = (String) pub.getAbstractText();
            if (StringUtils.isEmpty(abstractText)) {
                abstractText = attValue.toString();
            } else {
                abstractText += " " + attValue.toString();
            }
            pub.setAbstractText(abstractText);
        } else if ("doi".equals(attName) && "ArticleId".equals(qName)) {
            pub.setDOI(attValue.toString());
        } else if ("Author".equals(previousQName) && stack.search("AuthorList")==2){
        	if("LastName".equals(qName)){
        		author.setLastName(attValue.toString());
        		author.setType("person");
        	} else if ("ForeName".equals(qName)){
        		author.setFirstName(attValue.toString());
        	} else if ("Initials".equals(qName)){
        		author.setInitials(attValue.toString());
        	} else if ("Affiliation".equals(qName)){
        		author.setAffiliation(attValue.toString());
        	} else if ("CollectiveName".equals(qName)){
        		author.setType("CollectiveName");
        		author.setName(attValue.toString());
        	} else {
        		LOG.warn ("Not known author's attribute: " + qName);
        	}
        }  else if ("Author".equals(qName) && "AuthorList".equals(previousQName)) {
        	
            author.setRank(rank);
            
            if(author.getName() == null){
            	String authorName=author.getLastName();
            	if(authorName != null && author.getInitials() != null){
            		authorName += " "+author.getInitials();
            	}
            	author.setName(authorName);
            }
            
            pub.addAuthors(author);
            rank++;
        } else if ("MedlineCitation".equals(qName)){

        	try {
        		processPublication(pub);
        	} catch (SQLException sqle){
        		LOG.error("processPublication Exception: "+ sqle);
        	}
        }
        attName = null;
	}
	public void characters  (char ch[], int start, int length) throws SAXException {
		
		int st = start;
	    int l = length;
	   // if (attName != null) {
	        // DefaultHandler may call this method more than once for a single
	        // attribute content -> hold text & create attribute in endElement
	        while (l > 0) {
	            boolean whitespace = false;
	            switch(ch[st]) {
	                case ' ':
	                case '\r':
	                case '\n':
	                case '\t':
	                    whitespace = true;
	                    break;
	                default:
	                    break;
	            }
	            if (!whitespace) {
	                break;
	            }
	            ++st;
	            --l;
	        }
	
	        if (l > 0) {
	            StringBuffer s = new StringBuffer();
	            s.append(ch, st, l);
	            attValue.append(s);
	        }
	    //}
    }
	public void endDocument() throws SAXException {
		Date date = new Date();
		LOG.info("End document: "+ date.toString());
		
	}
	private static String convertToFileURL(String filename) {
        String path = new File(filename).getAbsolutePath();
        if (File.separatorChar != '/') {
            path = path.replace(File.separatorChar, '/');
        }

        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return "file:" + path;
    }
	public void processPublication(Publication publication) throws SQLException{
		if(loadDB){
			
			int pubID =  pubLoader.updatePublication(publication);
			if (pubID > 0){
				processedPMIDs.put(publication.getPubMedId(), pubID);
				LOG.info("New record pub_id="+pubID+" for PMID="+publication.getPubMedId());
			}
		} else {
			//pub.printData();
		}
	}
	public Hashtable getProcessedPMIDs(){
		return processedPMIDs;
	}
	
	public static void main (String[] args) throws Exception {
		//Set up a simple configuration that log on the console
		BasicConfigurator.configure();
		Date date = new Date ();
		LOG.setLevel(Level.INFO);
		LOG.info("Start application" + date.toString() );
		String user = null; 
		String pass = null; 
		String db = null; 
		String  server = null; 
		Options options = new Options ();
		options.addOption("file", true, "input file");
		options.addOption("D", true, "chado database name,tripal flavor or standalone");
		options.addOption("S", true, "database server");
		options.addOption("U", true, "user, able to write data into database");
		options.addOption("P", true, "password for db user");
		options.addOption("l", false,"load data into database, otherwise STDOUT");
		options.addOption("ftype", true, "feature type to process gene/protein");
		options.addOption("schema",true,"specify db schema name; optional if it is public; chado if it is tripal flavor");
		options.addOption("h", false, "help");
		
		
		CommandLineParser cmparser = new PosixParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = cmparser.parse(options, args);
		String filename = null;
		boolean loadDB = false;
		if(cmd.hasOption("h")){
			formatter.printHelp("PubMedXMLParser", options);
			System.exit(1);
		} else if (cmd.hasOption("file")){
			filename = cmd.getOptionValue("file");
			File file = new File (filename);
			if(file.exists() && !file.isDirectory()){
				System.out.println("File to process: "+filename);
			} else {
				System.out.println("There is a problem with file: "+ filename );
				formatter.printHelp("PubMedXMLParser", options);
				System.exit(1);
			}
		} else {
			LOG.fatal("File to process is required");
			formatter.printHelp("PubMedXMLParser", options);
			System.exit(1);
		}
		if(cmd.hasOption("l")){
		//load data into database
			loadDB=true;
		}
		String featType = "gene"; //by default
		if(cmd.hasOption("ftype")){
			//load data into database
			featType=cmd.getOptionValue("ftype");;
		}
		if(!cmd.hasOption("D") || !cmd.hasOption("S") || !cmd.hasOption("U") || !cmd.hasOption("P")){
			LOG.fatal("Database name, server, user and password are required if you want data to be loaded into db.");
			formatter.printHelp("PubMedXMLParser", options);
			System.exit(1);
		}else{
			db = cmd.getOptionValue("D");
			server = cmd.getOptionValue("S");
			user = cmd.getOptionValue("U");
			pass = cmd.getOptionValue("P");
		}
		String schema = null;
		if(cmd.hasOption("schema")){
			String sch = cmd.getOptionValue("schema");
			if(!sch.equals("public")){
				schema = sch;
			}
		}
		Connection conn = DatabaseUtil.connect(server, db, user, pass, schema);
		File f = new File (filename);
		String dir = f.getParent();
		BufferedReader br = null;
		if (conn != null){
			//collect which PMIDs are already in the chado DB
			PublicationFactory pubFactory = new PublicationFactory(conn);
			Hashtable<String,Integer> existingPMIDs = pubFactory.collectPMIDs();
			Set <Integer> pubmedids = new HashSet <Integer> ();
			//Set <Entry> entrySet = new HashSet <Entry>();
			Map <String, Set <String>> entries = new HashMap <String, Set<String>>();
			PubMedImporter importer = new PubMedImporter(conn);
			try { 
		        br = new BufferedReader(new FileReader(filename)); 
		        String line;
		        while ((line = br.readLine()) != null) { 
		          if(line.trim().length() == 0){
		        	  continue;
		          }
		          // in general case, file should have featureID \t PMID
		          String[] toks = line.split("\t", -1); // don't truncate empty fields
		          Integer pmid = 0;
		          try {
		        	  pmid = Integer.parseInt(toks[1]);
		          } catch  (NumberFormatException nfe){
		        	  LOG.error("Can't recognize PMID from the line: "+line);
		          }
		          if(entries.containsKey(toks[0])){ 
		        	  entries.get(toks[0]).add(toks[1]);
		          } else {
		        	  Set <String> pmidSet = new HashSet <String>();
		        	  pmidSet.add(toks[1]);
		        	  entries.put(toks[0], pmidSet);
		        	  
		          }
		          if (pmid > 0){
		        	  if(existingPMIDs.containsKey(toks[1])){
		        		  //there is pub_dbxref record for pmid in chado db
		        		  // do not request it from PubMed
		        	  } else {
		        		  //needs to be retrieved from PubMed
		        		  pubmedids.add(pmid);
		        	  }
	
		          }
		        } 
		     } finally { 
		        if (br != null) br.close(); // don't throw an NPE because the file wasn't found.
		     } 
			LOG.debug ("Found "+pubmedids.size()+ " PMIDs to get from PubMed");
			PubLoader pubLoader = new PubLoader (conn);
			LOG.debug("Feature type from options: "+featType);
			int typeId = pubLoader.getFeatureType(featType);
			if (typeId == 0){
				LOG.fatal("Type_id for "+featType+ " not found");
				System.exit(1);
			}
			List<String> fList = importer.processPubMed(pubmedids, dir);
			//List<String> fList = new ArrayList();
			//fList.add("/data/tmp1");
			//fList.add("/data/tmp2");
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setNamespaceAware(true);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			PubMedXMLParser xmlParser = new PubMedXMLParser();
			if(loadDB){
				xmlParser.setLoadDB(true);
				
			}
			xmlParser.setPubLoader(pubLoader);
			xmlReader.setContentHandler(xmlParser);
			for (int il=0; il< fList.size(); il++){
				String xmlFile = fList.get(il);
				LOG.debug("Processing "+xmlFile);
				xmlReader.parse(convertToFileURL(xmlFile));
			}
			Hashtable<String,Integer> processed = xmlParser.getProcessedPMIDs();
			Set<String> keySet = processed.keySet();
			LOG.info("Existing:" + existingPMIDs.keySet().size());
			
			int count = 0;
			for (String processedPMID: keySet){
				if(existingPMIDs.containsKey(processedPMID)){
					LOG.debug("Found between existing: "+processedPMID);
				} else {
					LOG.debug("to add: "+processedPMID+"="+processed.get(processedPMID));
					existingPMIDs.put(processedPMID, processed.get(processedPMID));
					count++;
					
				}
				
			}
			LOG.debug("Added "+ count + " PubMed records");
			LOG.info ("Update cross reference with entries:");
			ChadoFeatureFactory featFactory = new ChadoFeatureFactory(conn);
			int updatedFeatPub=0;
			int existsFeatPub=0;
			for (String entryKey: entries.keySet()){
				int featureID = featFactory.getFeatureId(typeId, entryKey);
				if(featureID > 0){ //if there is a feature with that name
					Set entryPMIDs = entries.get(entryKey);
					Iterator iter = entryPMIDs.iterator();
					while (iter.hasNext()){
						String curPMID = iter.next().toString();
						if(existingPMIDs.containsKey(curPMID) && existingPMIDs.get(curPMID) > 0){
							int pubId = existingPMIDs.get(curPMID);
							int featPubId = featFactory.getFeaturePubRecord(featureID,pubId);
							if(featPubId ==0){
								featFactory.setFeaturePubRecord(featureID,pubId);
								updatedFeatPub++;
							} else {
								existsFeatPub++;
							}
							LOG.info ("Linked " + entryKey + " with PMID= " + curPMID);
						} else {
							LOG.warn("No record for "+ curPMID);
							LOG.debug(existingPMIDs.toString());
						}
					}
				} else {
					LOG.warn("No Feature record for "+entryKey);
				}
			}
			LOG.info("Created "+updatedFeatPub +" Feature_Pub records");
			LOG.info("There are "+ existsFeatPub+" previously created records in feature_pub table");
			LOG.info("Close connection for now");
			conn.close();
		}
		date = new Date();
		LOG.info("End application " + date.toString());
	}
	

}
