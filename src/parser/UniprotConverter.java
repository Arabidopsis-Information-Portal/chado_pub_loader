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
import org.apache.log4j.Logger;

public class UniprotConverter extends DefaultHandler {
	private Stack<String> stack = new Stack<String>();
    private String attName = null;
    private StringBuffer attValue = null;
    private int noGeneLocus = 0;
    private int noProtId = 0;
    private Entry entry;
	private Publication pub = null;
	private int rank ;
	//private Set <Author> authors = null;
	private boolean loadDB = false;
	private boolean loadPM = false;
	private PubLoader pubLoader = null;
	static Logger LOG = Logger.getLogger(UniprotConverter.class);
	
	public UniprotConverter (){
		super();
	}
	public void setPubLoader(Connection conn) throws SQLException {
		this.pubLoader = new PubLoader(conn);
	}
	public void startDocument() throws SAXException {
		Date date = new Date ();
		LOG.info ("Start document "+ date.toString());
	}
	@Override
	public void startElement(String namespaceURI, String localName, String qName,  Attributes attrs)
		throws SAXException {
		String previousQName = null;
        if (!stack.isEmpty()) {
            previousQName = stack.peek();
        }
        attName = null;
        if ("entry".equals(qName)) {
        	entry=new Entry();
	
        } else if("property".equals(qName) && "dbReference".equals(previousQName)){
        	String geneID = getAttrValue(attrs, "gene ID");
        	if(geneID != null){
        		LOG.debug("gene ID="+ geneID);
        		entry.setGeneID(geneID);
        	}
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
        	author.setType (qName); //person or consortium ?... 
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
        attValue = new StringBuffer();

	}
	@Override
	public void endElement(String uri, String localName, String qName)
     throws SAXException  {
        super.endElement(uri, localName, qName);
        stack.pop();
        String previousQName = null;
        if (!stack.isEmpty()) {
            previousQName = stack.peek();
        }
        if ("accession".equals(attName)){
        	if(entry.getProteinAccession() == null){
        		entry.setProteinAccession(attValue.toString());
        	}
        } else if ("primaryIdentifier".equals(attName)){
        	entry.setProteinPrimaryID(attValue.toString());
        } else if ("name".equals(qName) && "gene".equals(previousQName)){
        	if("ordered locus".equals(attName)){
        		entry.setGeneLocus(attValue.toString().toUpperCase());
        	}
        } else if("citation".equals(qName)){
        	if(pub !=null){
        		String pubType=pub.getType(); //exclude "submission"
        		if(!pubType.equals("submission")){
        			entry.addPublication(pub);
        		}
        	}
        }else if("title".equals(attName) ){
        	String title = attValue.toString();
        	if (pub != null){
        		pub.setTitle(title);
        	}
        } else if ("locator".equals(attName)){
        	String locator = attValue.toString();
        	if(pub != null){
        		pub.setLocator(locator);
        	}
        } else if ("entry".equals(qName)){
        	try {
        		processEntry (entry);
        	} catch (SQLException sqle){
        		LOG.error("UniprotConverter.processEntry Exception: "+sqle);
        	}
        }
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
		LOG.info("End document: "+ date.toString());
		LOG.warn("No geneLocus="+noGeneLocus+"; noProtID="+noProtId);
	}
	public void processEntry(Entry entry) throws SQLException {
		String proteinAcc = entry.getProteinAccession();
		if(proteinAcc == null){
			LOG.error("No protein accession");
		} else {
			if(loadDB){
				pubLoader.processEntry(entry,loadPM);
			} else {
				LOG.debug("No database load for "+entry.getProteinAccession()+", loadDB="+loadDB);
				Set <Publication> pubs = entry.getPublications();
				if(pubs.isEmpty()){
					return;
				}
				String gene = entry.getGeneLocus();
				if(gene == null){
					LOG.warn("No gene locus for "+entry.getProteinAccession());
					noGeneLocus++;
				}
				
				String protID = entry.getProteinPrimaryID();
				if(protID == null){
					LOG.warn("No primary protein ID for "+entry.getProteinAccession());
					noProtId++;
				}
				for (Publication publication: pubs){
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
		options.addOption("D", true, "chado database name");
		options.addOption("S", true, "database server");
		options.addOption("U", true, "user, able to write data into database");
		options.addOption("P", true, "password for db user");
		options.addOption("l", false,"load data into database, otherwise create tab-delimited file");
		options.addOption("h", false, "help");
		options.addOption("pubmed",false, "load publication even it has PMID");
		options.addOption("schema",true,"specify db schema name; optional if it is public");
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
				LOG.info("File to process: "+filename);
			} else {
				LOG.fatal("There is a problem with file: "+ filename );
				formatter.printHelp("UniprotConverter", options);
				System.exit(1);
			}
		} else {
			LOG.fatal("Uniprot file to process is required");
			formatter.printHelp("UniprotConverter", options);
			System.exit(1);
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
				LOG.fatal("Database name, server, user and password are required if you want data to be loaded into db.");
				formatter.printHelp("UniprotConverter", options);
				System.exit(1);
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
		Connection connection = null;
		if(loadDB){
			//set PubLoader
			converter.setLoadDB(true);
			String schema = null;
			if(cmd.hasOption("schema")){
				String sch = cmd.getOptionValue("schema");
				if(!sch.equals("public")){
					schema = sch;
				}
			}
			connection = DatabaseUtil.connect(server,db,user,pwd,schema);
			converter.setPubLoader(connection);
		}
		converter.setLoadPM(loadPM);
		xmlReader.setContentHandler(converter);
		xmlReader.parse(convertToFileURL(filename));
		//close connection !!!!
		if(connection != null){
			connection.close();
		}
	}
	

}
