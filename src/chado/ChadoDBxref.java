package chado;

import java.sql.*;
import org.apache.log4j.Logger;
import java.util.*;

public class ChadoDBxref {
	private static final Logger LOG = Logger.getLogger(ChadoDBxref.class);
	private final Connection connection;
	private PreparedStatement dbXrefStmt=null;
	private Map<String, Integer> dbs = null;
	public ChadoDBxref(Connection conn){
		this.connection = conn;
		
	}
	
	public void setDB()throws SQLException {
		Map<String, Integer> allDBs = new HashMap();
		String query = 
				"SELECT db_id, name "
				+ "  FROM db";
		LOG.info("executing: " + query);
		PreparedStatement stmt = this.connection.prepareStatement(query);
		ResultSet res = stmt.executeQuery();
		while (res.next()){
			Integer dbId = new Integer (res.getInt("db_id"));
			String dbName = res.getString("name");
			allDBs.put(dbName, dbId);
		}
		this.dbs = allDBs;
	}
	public int getDBid( String name)
		 throws SQLException{
		if (dbs == null){
			setDB();
		}
		if(dbs.containsKey(name)){
			return dbs.get(name);
		} else {
			return 0;
		}
				
	}
	
	public int getDBXrefId (int dbId, String accession) 
			throws SQLException {
		int dbXrefId = 0;
		//prepare query to get dbxref_id for given accession and db_id
		String dbXrefQuery =
				"SELECT dbxref_id "
				+ "FROM dbxref "
				+ "WHERE db_id = ? and accession = ?";
		this.dbXrefStmt = connection.prepareStatement(dbXrefQuery);
		dbXrefStmt.setInt(1,dbId);
		dbXrefStmt.setString(2,accession);
		ResultSet res = dbXrefStmt.executeQuery();
		while(res.next()){
			dbXrefId = res.getInt("dbxref_id");
		}
		return dbXrefId;
	}
	public int getDBXrefForName (String accession, String dbName) throws SQLException {
    	int dbXrefID =0;
    	int dbID =0;
    	
    	String query = "SELECT db_id FROM db WHERE name = ?";
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setString(1,dbName);
    	ResultSet res = stmt.executeQuery();
    	
    	while (res.next()){
    		dbID = res.getInt("db_id");
    		
    	}
    	//check if dbxref for (dbID and accession already exists in dbxref
    	dbXrefID = getDBXrefId(dbID, accession);
    	if(dbXrefID == 0){
    		insertDBXref (dbID, accession);
    		dbXrefID = getDBXrefId(dbID, accession);
    	}
    		
    	
    	return dbXrefID;
    }
	public int insertDBXref (int dbID, String accession) throws SQLException {
    	int dbxrefID;
    	String query = "INSERT into dbxref (db_id, accession) VALUES (?,?)";
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setInt(1,dbID);
    	stmt.setString(2,accession);
    	LOG.info("executing \""+query+ "for db_id=" + dbID + " and accession=" + accession);
    	dbxrefID = stmt.executeUpdate(); // returns either raw count or 0, I need new dbxref_id
    	return dbxrefID;
    }
	public int insertDBXrefForName (String dbName, String accession) throws SQLException {
		int dbxrefID;
		int dbID = getDBid(dbName);
		dbxrefID = insertDBXref(dbID, accession);
		return dbxrefID;
	}

}
