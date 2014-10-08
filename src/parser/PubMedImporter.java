package parser;


import java.net.*;

import gov.nih.nlm.ncbi.www.soap.eutils.EFetchPubmedServiceStub;
import gov.nih.nlm.ncbi.www.soap.eutils.EFetchPubmedServiceStub.PubmedArticleType;
import gov.nih.nlm.ncbi.www.soap.eutils.EUtilsServiceStub;
import java.io.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.SAXParser;

import loader.DatabaseUtil;
import loader.PubLoader;
import loader.PublicationFactory;
import model.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser; 

import org.apache.log4j.Logger;

public class PubMedImporter {
	private String gbURL = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&tool=PubMedImporter";
	private static final int MAX_TRIES = 5;
	public static final int RET_MAX = 500;
	private String retmode = "xml";
	private Set<Integer> seenPubMeds = new HashSet<Integer>();
	
	static Logger LOG = Logger.getLogger(PubMedImporter.class);
	
	public PubMedImporter(Connection conn) {
		// TODO Auto-generated constructor stub
	}
	public Reader getReader (Set <Integer> pubmedids) throws Exception {
		String email = System.getProperty("user.name")+"@jcvi.org";
		String queryURL = gbURL+"&retmode="+retmode+"&email="+email;
		String urlString = gbURL+"&retmode="+retmode+"&id="+join(pubmedids, ",");
		LOG.debug("retrieving: " + urlString);
		URL url = new URL (urlString);
		URLConnection urlConn = url.openConnection();
		urlConn.setDoOutput(true);
		urlConn.setDoInput(true);
		InputStream connInput = urlConn.getInputStream();
		InputStreamReader sReader = new InputStreamReader(connInput);
		BufferedReader rd = new BufferedReader(sReader);
        return rd;
	}
	public String join (Collection<?> c, String delim){
		StringBuffer sb = new StringBuffer();
        boolean needComma = false;
        for (Object o : c) {
            if (needComma) {
                sb.append(delim);
            }
            needComma = true;
            sb.append(o.toString());
        }
       // System.out.println(sb.toString());
        return sb.toString();

	}
	public List<String> processPubMed (Set <Integer> pubmedids, String dirPath) throws Exception {
		List<String> fileList = new ArrayList<String>();
		Set<Integer> idsToFetch = new HashSet<Integer>();
		Iterator<Integer> it = pubmedids.iterator();
		int fileNumber = 1;
		while (it.hasNext()){
			int id = it.next();
			idsToFetch.add(id);
			if(idsToFetch.size() == RET_MAX || !it.hasNext() && idsToFetch.size() > 0){
				String outFile = dirPath+"/tmp"+fileNumber;
				System.out.println("Will write in file:" + outFile);
				fileList.add(outFile);
				PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter (outFile)));
				BufferedReader br = new BufferedReader(getReader(idsToFetch));
				Map<String, Map<String, Object>> fromServerMap = null;

				StringBuffer buf = new StringBuffer();
				String line;
				while ((line = br.readLine()) != null){
					buf.append(line + "\n");
				}
				out.print(buf);
				//System.out.println(buf.toString());
				if(out.checkError()){
					System.out.println("Error happened in writing to "+outFile);
				}
				out.close();
				br.close();
				fromServerMap = new HashMap<String, Map<String, Object>>();

				//SAXParser.parse(new InputSource( new StringReader (buf.toString())), new PubMedXMLParser(),false);
				idsToFetch.clear();
				fileNumber++;
			}
			
		}
		return fileList;
	}
	public static void main (String[] args) throws Exception {
		String user = null; 
		String pass = null; 
		String db = null; 
		String  server = null; 
		Set <Entry> entrySet = new HashSet <Entry>();
		Set <Integer> pubmedids = new HashSet <Integer> ();
		Options options = new Options ();
		options.addOption("file", true, "input file");
		options.addOption("D", true, "chado database name,tripal flavor");
		options.addOption("S", true, "database server");
		options.addOption("U", true, "user, able to write data into database");
		options.addOption("P", true, "password for db user");
		options.addOption("h", false, "help");
		
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
		BufferedReader br = null; 
		Connection conn = DatabaseUtil.connect(server, db, user, pass, null);
		Map <String, Set <String>> entries = new HashMap <String, Set<String>>(); 
		
		if (conn != null){
			//collect which PMIDs are already in the chado DB
			PublicationFactory pubFactory = new PublicationFactory(conn);
			Hashtable existingPMIDs = pubFactory.collectPMIDs();
			Enumeration items = existingPMIDs.keys();
			while (items.hasMoreElements()){
				String accession = (String) items.nextElement();
				System.out.println( accession+ "=" + existingPMIDs.get(accession));
				
			}
			System.out.println ("connection to "+db+ " is available");
			PubMedImporter importer = new PubMedImporter(conn);
			File f = new File (filename);
			String dir = f.getParent();
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
		        	  System.out.println("Can't recognize PMID from the line: "+line);
		          }
		          if(entries.containsKey(toks[0])){ 
		        	  entries.get(toks[0]).add(toks[1]);
		          } else {
		        	  Set <String> pmidSet = new HashSet <String>();
		        	  pmidSet.add(toks[1]);
		        	  entries.put(toks[0], pmidSet);
		        	  
		          }
		         // Entry entry = new Entry ();
		         // entry.setFeatureName(toks[0]); //feature name
		          //entry.addPubMedID (Integer.parseInt(toks[1]));
		          // entrySet.add(entry);
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
			System.out.println ("Found "+pubmedids.size()+ " PMIDs to get from PubMed");
			
			List<String> fList = importer.processPubMed(pubmedids, dir);
			
			
			System.out.println("Close connection for now");
			conn.close();
		}
		
	}
}
