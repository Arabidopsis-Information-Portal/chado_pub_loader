package parser;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

import javax.xml.parsers.*;
import model.*;
import loader.*;
public class UniprotConverter extends DefaultHandler {
	private Stack<String> stack = new Stack<String>();
    private String attName = null;
    private StringBuffer attValue = null;
    private int entryCount = 0;
    private int noGeneLocus = 0;
    private int noProtId = 0;
    private int noProteinAcc = 0;
    private Entry entry;
	private Publication pub = null;
	private int rank ;
	//private Set <Author> authors = null;
	private boolean loadDB = false;
	private boolean loadPM = false;
	private PubLoader pubLoader = null;
	public UniprotConverter (){
		super();
	}
	public void setPubLoader(Connection conn) throws SQLException {
		this.pubLoader = new PubLoader(conn);
	}
	public void startDocument() throws SAXException {
		Date date = new Date ();
		System.out.println ("Start document "+ date.toString());
	}
	@Override
	public void startElement(String namespaceURI, String localName, String qName,  Attributes attrs)
		throws SAXException {
		/* if("".equals(namespaceURI))
			System.out.println("Start element: "+qName);
		else 
			System.out.println("Start element: {"+namespaceURI + "}" + qName);*/
		String previousQName = null;
        if (!stack.isEmpty()) {
            previousQName = stack.peek();
        }
        attName = null;
        if ("entry".equals(qName)) {
        	entry=new Entry();
	
        } else if("property".equals(qName) && "dbReference".equals(previousQName)){
        	/*String geneID = getAttrValue(attrs, "gene ID");
        	
        	if(geneID == null){
        		
        	}else {
        		System.out.println("gene ID="+ geneID);
        		entry.set("geneID", geneID);
        	}*/
        	
        } else if ("name".equals(qName) && "entry".equals(previousQName)) {
            attName = "primaryIdentifier";
            
        } else if ("accession".equals(qName) && "entry".equals(previousQName)) {
            attName = "accession";
         
        } else if ("property".equals(qName) && "dbReference".equals(previousQName)){
        	
        	attName = "property";
        } else if ("name".equals(qName) && "gene".equals(previousQName)){
        	attName = getAttrValue(attrs, "type");
        	
        } else if ("citation".equals(qName)){
        	pub = new Publication ();
        	rank = 0;
        	for (int i = 0; i< attrs.getLength(); i++){
        		String locName = attrs.getLocalName(i);
        		String avalue = attrs.getValue(i);
        		pub.setFieldValue(locName, avalue);
        	}
        }else if ("citation".equals(previousQName) && "title".equals(qName)){
        	attName = "title";
        }else if ("citation".equals(previousQName) && "locator".equals(qName)){
        	attName = "locator";
        }else if ("dbReference".equals(qName) && "citation".equals(previousQName)){
        	
        	if("PubMed".equals(getAttrValue(attrs, "type"))){
        		String pubid = getAttrValue(attrs, "id");
        		pub.setPubMedId(pubid);
        	} else if ("DOI".equals(getAttrValue(attrs, "type"))){
        		String doi = getAttrValue(attrs, "id");
        		pub.setDOI(doi);
        	}
        } else if ("authorList".equals(previousQName) && stack.search("citation") == 2){
        	Author author = new Author ();
        	author.setType (qName); //person or ?... 
        	String aname = getAttrValue(attrs,"name");
        	aname = aname.replace(".","");
        	author.setName(aname); //as it is in uniprot file
        	if(qName.equals("person")){
        		String[] nameParts = aname.split(" ",2);
        		author.setLastName(nameParts[0]);
        		if(nameParts.length > 1){
        			author.setFirstName(nameParts[1]);
        			author.setInitials(nameParts[1]);
        		}
        	}
        	author.setRank(rank);
    		rank++;
        	//authors.add(author);
        	pub.addAuthors(author);
        }
        super.startElement(namespaceURI, localName, qName, attrs);
        stack.push(qName);
       // System.out.println("Stack startElement: "+stack);
        attValue = new StringBuffer();

	}
	@Override
	public void endElement(String uri, String localName, String qName)
     throws SAXException  {
        super.endElement(uri, localName, qName);
        stack.pop();
       // System.out.println("Stack endElement after pop: "+stack);
		//System.out.println("qName after pop: "+ qName);
       // if (attName == null && attValue.toString() == null) {
       //     return;
       // } 
        String previousQName = null;
        if (!stack.isEmpty()) {
            previousQName = stack.peek();
        }
        //System.out.println("End element: "+ qName);
        
        if ("accession".equals(attName)){
        	//System.out.println("Accession = "+attValue.toString());
        	entry.setProteinAccession(attValue.toString());
        	//System.out.println(entry.get("accession"));
        } else if ("primaryIdentifier".equals(attName)){
        	//System.out.println("Primary protein id = " + attValue.toString());
        	entry.setProteinPrimaryID(attValue.toString());
        } else if ("name".equals(qName) && "gene".equals(previousQName)){
        	if("ordered locus".equals(attName)){
        		//System.out.println("gene="+ attValue.toString().toUpperCase() );
        		
        		entry.setGeneLocus(attValue.toString().toUpperCase());
        	}
        } else if("citation".equals(qName)){
        	//System.out.println ("/Citation");
        	if(pub !=null){
        		//System.out.println("Pub Type is "+pub.getType());
        		String pubType=pub.getType(); //exclude "submission"
        		if(!pubType.equals("submission")){
        			//process 
        			if(pub.getPubMedId() == null){
        				
        			} else {
        				entry.addPublication(pub);
        			}
        		}
        	}
        	//System.out.println("CITATION:"+pub);
        }else if("title".equals(attName) ){
        	String title = attValue.toString();
        	//System.out.println("Title ="+title);
        	if (pub != null){
        		pub.setTitle(title);
        	}
        } else if ("locator".equals(attName)){
        	String locator = attValue.toString();
        	if(pub != null){
        		pub.setLocator(locator);
        	}
        } else if ("entry".equals(qName)){
        	
        	//System.out.println("/Entry");
        	try {
        		processEntry (entry);
        	} catch (SQLException sqle){
        		System.out.println("UniprotConverter.processEntry Exception: "+sqle);
        	}
        }
        //System.out.println("qName in endElement: "+ qName);
        
  
        
       // System.out.println("endElement: previous QName="+previousQName);
       // System.out.println("Attname="+attName+" value="+attValue.toString());
        

	}
	public void characters  (char ch[], int start, int length) throws SAXException {
		
		int st = start;
	    int l = length;
	    if (attName != null) {
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
	    }
    }
	public void endDocument() throws SAXException {
		Date date = new Date();
		System.out.println("End document: "+ date.toString());
		System.out.println("No geneLocus="+noGeneLocus+"; noProtID="+noProtId+"; noProteinAcc="+noProteinAcc);
	}
	public void processEntry(Entry entry) throws SQLException {
		//entryCount++;
		//if(entryCount % 100 == 0){
		//	System.out.println("Processed "+ entryCount + " entries.");
		//}
		if(loadDB){
			pubLoader.processEntry(entry,loadPM);
		} else {
			Set <Publication> pubs = entry.getPublications();
			if(pubs.isEmpty()){
				return;
			}
			Iterator<Publication> iter =  pubs.iterator();
			
			String gene = entry.getGeneLocus();
			if(gene == null){
				//System.out.println("No gene locus for");
				noGeneLocus++;
				
			}
			String proteinAcc = entry.getProteinAccession();
			if(proteinAcc == null){
				//System.out.println("No protein accession");
				noProteinAcc++;
				
			}
			String protID = entry.getProteinPrimaryID();
			if(protID == null){
				//System.out.println("No protein ID");
				noProtId++;
				//System.exit(3);
			
			}
			while(iter.hasNext()){
				Publication publication = (Publication)iter.next();
				//ArrayList<Author> authors = publication.getAuthors();
				if("submission".equals(publication.getType())){
					//System.out.println("PublicationType=submission; Authors:");
					//for (Author a: authors){
					//	System.out.println(a.getType()+": "+a.getName());
					//}
					//skip it
				} else {	
					if (publication.getPubMedId() != null){
						if(loadPM){
							System.out.println(gene+"\t"+proteinAcc+"\t"+protID+"\t"+publication.toString());
						} 
					} else { //no PMID only
						System.out.println(gene+"\t"+proteinAcc+"\t"+protID+"\t"+publication.toString());
					}
				}
			}
		}
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
	private String getAttrValue(Attributes attrs, String name) {
        if (attrs.getValue(name) != null) {
            return attrs.getValue(name).trim();
        }
        return null;
    }
	public void setLoadDB(boolean loadDB){
		this.loadDB = loadDB;
	}
	public void setLoadPM(boolean loadPM){
		this.loadPM = loadPM;
	}
	static public void main (String[] args) throws Exception {
		Options options = new Options ();
		options.addOption("file", true, "input file");
		options.addOption("D", true, "chado database name,tripal flavor");
		options.addOption("S", true, "database server");
		options.addOption("U", true, "user, able to write data into database");
		options.addOption("P", true, "password for db user");
		options.addOption("l", false,"load data into database, otherwise create tab-delimited file");
		options.addOption("h", false, "help");
		options.addOption("pubmed",false, "load publication even it has PMID");
		CommandLineParser cmparser = new PosixParser();
	
		HelpFormatter formatter = new HelpFormatter();
		String filename = null;
		boolean loadDB = false;
		boolean loadPM = false;
		
		CommandLine cmd = cmparser.parse(options, args);
		
		if(cmd.hasOption("h")){
			formatter.printHelp("UniprotConverter", options);
			System.exit(0);
		} else if (cmd.hasOption("file")){
			filename = cmd.getOptionValue("file");
			File file = new File (filename);
			if(file.exists() && !file.isDirectory()){
				System.out.println("File to process: "+filename);
			} else {
				System.out.println("There is a problem with file: "+ filename );
				formatter.printHelp("UniprotConverter", options);
				System.exit(0);
			}
		} else {
			System.out.println("Uniprot file to process is required");
			formatter.printHelp("UniprotConverter", options);
			System.exit(0);
		}
		String db = null;
		String server = null;
		String user = null;
		String pwd = null;
		if(cmd.hasOption("pubmed")){
			loadPM=true;
		}
		if(cmd.hasOption("l")){
		//load data into database
			loadDB=true;
			if(!cmd.hasOption("D") || !cmd.hasOption("S") || !cmd.hasOption("U") || !cmd.hasOption("P")){
				System.out.println("Database name, server, user and password are required if you want data to be loaded into db.");
				formatter.printHelp("UniprotConverter", options);
				System.exit(0);
			}else{
				db = cmd.getOptionValue("D");
				server = cmd.getOptionValue("S");
				user = cmd.getOptionValue("U");
				pwd = cmd.getOptionValue("P");
			}
		}
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setNamespaceAware(true);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		UniprotConverter converter = new UniprotConverter();
		if(loadDB){
			//set PubLoader
			converter.setLoadDB(true);
			DatabaseUtil dbUtil = new DatabaseUtil();
			Connection connection = dbUtil.connect(server,db,user,pwd,"chado");
			converter.setPubLoader(connection);
		}
		System.exit(0);
		converter.setLoadPM(loadPM);
		xmlReader.setContentHandler(converter);
		xmlReader.parse(convertToFileURL(filename));
		//close connection !!!!
	}
	

}
