package loader;

import java.sql.*;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import model.Publication;
import model.Author;
import chado.ChadoCVFactory;
import chado.ChadoDBxref;
public class PublicationFactory  {
	static Logger LOG = Logger.getLogger(PublicationFactory.class);
    private final Connection connection ;
    private PreparedStatement pubIDstmt;
    public PublicationFactory (Connection connection){
    	this.connection = connection;
    	String getPubIDquery = "SELECT pub_id FROM pub "
    			+ " WHERE type_id = ? AND "
    			+ " title = ? AND "
    			+ " series_name = ?";
    	try {
    		pubIDstmt = connection.prepareStatement(getPubIDquery);
    	} catch (SQLException e){
    		LOG.warn ("Can't prepare \"" + getPubIDquery+" \" query");
    	}
    			
    }
    public int getPubDBxRefForPMID(String pmid, int dbID) throws SQLException {
    	int dbXrefID = 0;
    	String query = "SELECT dbxref_id from dbxref WHERE db_id = "+dbID+ " AND accession=?";
    	LOG.info("Executing: "+query + " for PMID="+pmid);
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setString(1, pmid);
    	ResultSet res = stmt.executeQuery();
    	while (res.next()){
    		dbXrefID = res.getInt("pub_id");
    	}
    	return dbXrefID;
    }
    public int getPubIdForDBXrefID (int dbXrefID) 
    		throws SQLException {
    	int pubID = 0;
    	String query = "SELECT pub_id FROM pub_dbxref"
    			+ " WHERE dbxref_id = ?";
    	LOG.info("executing: " + query +" for dbxref_id="+dbXrefID);
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setInt(1,dbXrefID);
    	ResultSet res = stmt.executeQuery();
    	int numPubForPmid = 0;
    	
    	while (res.next()){
    		pubID = res.getInt("pub_id");
    		numPubForPmid ++;
    	}
    	if(numPubForPmid > 1){
    		LOG.warn("There are multiple records for PMID with dbxref_id=" + dbXrefID);
    	}
    	return pubID;
    }
    public int  addPubDBXref(ChadoDBxref dbXrefHelper, String dbName, String accession, int pubID) 
    		throws SQLException {
    	int pubDBXrefID = 0;
    	//check if accession exists in dbxref table
    	int dbxrefID = dbXrefHelper.getDBXrefForName(accession,dbName );
    	
    	if (dbxrefID == 0){ //there is no PMID in the dbxref table, let' write one
    		dbxrefID = dbXrefHelper.insertDBXrefForName(dbName, accession);
    	}
    	pubDBXrefID = getPubIdForDBXrefID(dbxrefID);
    	if(pubDBXrefID == 0){
    		pubDBXrefID = insertPubDBXref (dbxrefID,pubID);
    	}	
    	return pubDBXrefID;
    }
    public int getPubId (Publication pub, ChadoDBxref dbXrefHelper) 
    		throws SQLException {
    	int pubID = 0;
    	int dbXrefID = 0;
    	if(pub.getPubMedId() != null){ //if publication has PMID
    		//get dbXrefId for PMID
    		dbXrefID = dbXrefHelper.getDBXrefForName(pub.getPubMedId(),"PMID");
			if(dbXrefID > 0){
				//PMID is found, let's find corresponding pub_id
				// it should be one-to-one relationship
				pubID = getPubIdForDBXrefID(dbXrefID);
				LOG.debug("Found dbxref record for PMID= "+pub.getPubMedId());
			}
    	}
		if(pubID == 0){
			int typeCVTermId = pub.getCVTermId();
			pubIDstmt.setInt(1, typeCVTermId );
			pubIDstmt.setString(2, pub.getTitle());
			pubIDstmt.setString(3,pub.getName());
			ResultSet res = pubIDstmt.executeQuery();
			LOG.debug("Looking for title=\""+pub.getTitle()+"\" cvterm_id for type="+typeCVTermId +" in journal: \""+pub.getName()+"\"");
			while(res.next()){
				pubID = res.getInt("pub_id");
			}
			if(pubID > 0 && pub.getPubMedId() != null) { //publication is in db but PMID is missing
				//let's add PMID:
				insertPubDBXref(dbXrefID, pubID);
			}
		}
	
    	return pubID;
    
    }
    public int addPub(Publication pub) throws SQLException{
    	int newPubID = 0;
    	
    	String query = "INSERT into pub (type_id, title, pyear, series_name, volume,pages, uniquename, issue) "
    			+ " VALUES (?,?,?,?,?,?,?,?)";
    	PreparedStatement stmt = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    	stmt.setInt(1,pub.getCVTermId());
    	stmt.setString(2,pub.getTitle());
    	stmt.setString(3,pub.getYear());
    	stmt.setString(4,pub.getName());
    	stmt.setString(5,pub.getVolume());
    	stmt.setString(6,pub.getPages());
    	
    	String uniquename = createUniquename(pub);
    	pub.setUniqueName(uniquename);
    	stmt.setString(7, uniquename);
    	stmt.setString(8,pub.getIssue());
    	//stmt.setString(9, pub.getMonth());
    	LOG.info("Executing :\""+query+"\" for "+pub.getCVTermId()+"; "+pub.getTitle()+"; "+pub.getYear()+"; "+pub.getName()+"; "+uniquename);
    	int count = stmt.executeUpdate();
    	ResultSet resKey = stmt.getGeneratedKeys();
    	if(resKey.next()){
    		newPubID = resKey.getInt(1);
    		LOG.debug("New pub_id = "+newPubID);
    	}
    	LOG.debug("Updated "+ count+ " raws in pub");
    	return newPubID;
    }
    public int insertPubDBXref(int dbxrefID,  int pubID) throws SQLException {
    	int pubDBxrefID = 0;
    	String query = "INSERT into pub_dbxref (pub_id, dbxref_id, is_current) "
    			+ "VALUES (?,?,true)";
    	PreparedStatement stmt = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    	stmt.setInt(1, pubID);
    	stmt.setInt(2, dbxrefID);
    	stmt.executeUpdate();
    	ResultSet resKey = stmt.getGeneratedKeys();
    	if(resKey.next()){
    		pubDBxrefID = resKey.getInt(1);
    	}
    	return pubDBxrefID;
    }
    public String createUniquename(Publication pub){
    	String uname = "";
    	boolean year = false;
    	String authorsList = pub.getAuthorsList();
    	if(authorsList!=null && !authorsList.isEmpty()){
    		uname = authorsList;
    	}
    	if(pub.getTitle()!=null && !pub.getTitle().isEmpty()){
    		addSpace(uname);
    		uname += pub.getTitle();
    	}
    	if(pub.getName()!=null && !pub.getName().isEmpty()){
    		addSpace(uname);
    		uname += pub.getName();
    	}
    	if(pub.getYear()!=null && !pub.getYear().isEmpty()){
    		addSpace(uname);
    		uname += pub.getYear();
    		year=true;
    		
    	}
    	if(pub.getVolume()!=null && !pub.getVolume().isEmpty()){
    		if(year){
    			uname +=";";
    		}
    		uname += pub.getVolume();
    		if(pub.getPages() != null && !pub.getPages().isEmpty()){
    			uname += ":"+pub.getPages();
    		}
    	}
    	LOG.debug("Created uniquename: "+uname);
    	return uname;
    }
    public String addSpace (String s){
    	if(!s.isEmpty()){
    		s= s+" ";
    	}
    	return s;
    }
    public int addPubProp (int typeID, int pubID, String value) throws SQLException {
    	int pubPropID=0;
    	String query = "INSERT into pubprop (pub_id, type_id, value, rank) "
    			+ "VALUES (?, ?, ?, ?)";
    	PreparedStatement stmt = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    	stmt.setInt(1,pubID);
    	stmt.setInt(2,typeID);
    	stmt.setString(3,value);
    	stmt.setInt(4,0);
    	pubPropID = stmt.executeUpdate();
    	return pubPropID;
    }
    public void updatePubProp(int typeID, int pubID, String value) throws SQLException {
    	int pubPropID=0;
    	//pub_id + type_id + rank should be unique in pubprop (rank is always 0 here)
    	String query = "SELECT pubprop_id from pubprop WHERE pub_id =? and type_id=?";
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setInt(1, pubID);
    	stmt.setInt(2, typeID);
    	//stmt.setString(3, value);
    	ResultSet res = stmt.executeQuery();
    	int count = 0;
    	while (res.next()){
    		pubPropID = res.getInt("pubprop_id");
    		count++;
    	}
    	if(count > 1){
    		LOG.warn("updatePubProp: multiple records for pubID="+pubID+" typeID="+" value:"+typeID+value);
    	}
    	if(pubPropID == 0){
    		addPubProp(typeID,pubID,value);
    	}
    	
    }
    public void addAuthors (ArrayList<Author> authors, int pubID) throws SQLException {
    	//as it is done in tripal: delete all records for pubID from pubauthor
    	String query = "DELETE FROM pubauthor WHERE pub_id = ?";
		PreparedStatement stmt = connection.prepareStatement(query);
		stmt.setInt(1,pubID);
		int aNumber = stmt.executeUpdate();
		LOG.debug("Deleted "+aNumber+ " records from pubauthor table for pub_id="+pubID);
		String insertQuery = "INSERT into pubauthor (pub_id, surname,givennames,rank) "
				+ " VALUES (?,?,?,?)";
		stmt = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);
    	for (Author a: authors){
    		stmt.setInt (1,pubID);
    		if("person".equals(a.getType())){
    			stmt.setString(2, a.getLastName());
    			stmt.setString(3, a.getFirstName());
    		} else {
    			stmt.setString(2,a.getName());
    			stmt.setString(3, "");
    		}
    		stmt.setInt(4, a.getRank());
    		int count = stmt.executeUpdate();
    		ResultSet resKey = stmt.getGeneratedKeys();
    		if(resKey.next()){
        		LOG.debug(resKey.getInt(1)+": Updated pubauthor record for "+ a.getName());
        	}

    	}
    }
    public Hashtable collectPMIDs() throws SQLException {
    	Hashtable<String,Integer> pubMedPubs = new Hashtable<String,Integer>();
    	String query = "SELECT accession, pub_id FROM pub_dbxref  p, dbxref x, db d "+
    	"WHERE d.name='PMID' and x.db_id=d.db_id and p.dbxref_id=x.dbxref_id";
    	PreparedStatement stmt = connection.prepareStatement(query);
    	ResultSet res = stmt.executeQuery();
    	int count = 0;
    	while (res.next()){
    		pubMedPubs.put(res.getString("accession"),res.getInt("pub_id"));
    		count++;
    	}
    	LOG.info("collectPMIDs: found "+count+" PMIDs in db");
    	return pubMedPubs;
    }

}
