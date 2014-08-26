package chado;

import java.sql.*;
import org.apache.log4j.Logger;
public class ChadoCVFactory {
	private static final Logger LOG = Logger.getLogger(ChadoCVFactory.class);
    private final Connection connection;
    private ChadoDBxref dbxref;
    /**
     * Create a new ChadoCVFactory.
     * @param connection the connection to use for querying cvterms.
     */
    public ChadoCVFactory(Connection connection) {
        this.connection = connection;
    }
    /**
     * Get a new ChadoCV containing only cv terms from the cv with the given name.
     * @param cvName name of controlled vocabulary, eg. sequence ontology
     * @return the new ChadoCV object
     * @throws SQLException if there is problem while querying
     */
    public ChadoCV getChadoCV(String cvName)
        throws SQLException {
        ChadoCV cv = new ChadoCV(cvName);

        ResultSet cvtermRes = getCVTermResultSet(connection, cvName);
        while (cvtermRes.next()) {
            Integer cvtermId = new Integer(cvtermRes.getInt("cvterm_id"));
            String cvtermName = cvtermRes.getString("cvterm_name");
            ChadoCVTerm cvTerm = new ChadoCVTerm(cvtermName);
            cv.addByChadoId(cvtermId, cvTerm);
        }

        ResultSet cvrelRes = getCVTermRelationshipResultSet(connection, cvName);
        while (cvrelRes.next()) {
            Integer subjectId = new Integer(cvrelRes.getInt("subject_id"));
            Integer objectId = new Integer(cvrelRes.getInt("object_id"));
            ChadoCVTerm subject = cv.getByChadoId(subjectId);
            ChadoCVTerm object = cv.getByChadoId(objectId);

            subject.getDirectParents().add(object);
            object.getDirectChildren().add(subject);
        }

        return cv;
    }
    /**
     * Return the rows from the cvterm_relationship table that relate cvterms from the cv with the
     * given name.
     * This is a protected method so that it can be overriden for testing
     * @param connection the db connection
     * @param cvName the value of the name field to use when finding the cv
     * @return the SQL result set
     * @throws SQLException if a database problem occurs
     */
    protected ResultSet getCVTermRelationshipResultSet(Connection connection, String cvName)
        throws SQLException {
        String query =
            "SELECT cvterm_rel.subject_id, cvterm_rel.object_id, rel_type.name"
            + "  FROM chado.cvterm_relationship cvterm_rel, chado.cvterm subject_cvterm,"
            + "       chado.cvterm object_cvterm, chado.cv cvterm_cv,"
            + "       chado.cvterm rel_type"
            + " WHERE subject_cvterm.cv_id = cvterm_cv.cv_id"
            + "   AND object_cvterm.cv_id = cvterm_cv.cv_id"
            + "   AND cvterm_cv.name = ?"
            + "   AND cvterm_rel.subject_id = subject_cvterm.cvterm_id"
            + "   AND cvterm_rel.object_id = object_cvterm.cvterm_id"
            + "   AND cvterm_rel.type_id = rel_type.cvterm_id"
            + "   AND (rel_type.name = 'isa' OR rel_type.name = 'is_a')";
        LOG.info("executing: " + query);
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setString(1, cvName);
        ResultSet res = stmt.executeQuery();
        return res;
    }

    /**
     * Return the rows from the cvterm table that are from the cv with the given name.
     * This is a protected method so that it can be overriden for testing
     * @param connection the db connection
     * @param cvName the value of the name field to use when finding the cv
     * @return the SQL result set
     * @throws SQLException if a database problem occurs
     */
    protected ResultSet getCVTermResultSet(Connection connection, String cvName)
        throws SQLException {
        String query =
            "SELECT cvterm.cvterm_id, cvterm.name as cvterm_name"
            + " FROM chado.cvterm, chado.cv WHERE cv.name = ?"
            + " AND cvterm.cv_id = cv.cv_id";
        LOG.info("executing: " + query);
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setString(1, cvName);
        ResultSet res = stmt.executeQuery();
        return res;
    }
    public Integer getIDForTerm(String term) throws SQLException {
    	Integer cvtermID = new Integer (0);
    	String query = "SELECT cvterm_id FROM chado.cvterm WHERE name=?";
    	LOG.info("executing: " + query);
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setString(1, term);
    	ResultSet res = stmt.executeQuery();
    	while (res.next()){ // term should be unique through the cvterm table
    		cvtermID = res.getInt("cvterm_id");
    		
    	}
    	return cvtermID;		
    }
    // add term for CV ("autocreated") and db_id {internal}
    public int addTerm (int dbID, String cv, String term) throws SQLException {
    	int newTermId = 0;
    	String query = "SELECT cv_id FROM chado.cv WHERE name = ?";
    	LOG.info("executing: " + query);
    	PreparedStatement stmt = connection.prepareStatement(query);
    	stmt.setString(1, cv);
    	ResultSet res = stmt.executeQuery();
    	int cvID = 0;
    	while (res.next()){
    		cvID = res.getInt("cv_id");
    	}
    	// what to do if CV does not exists?
    	// add code here
    	if (cvID == 0){
    		LOG.fatal ("There is no "+ cv + "CV in the database");
    		System.exit(1);
    	}
    	//we need dbxref_id for our term
    	// check if there is a dbxref_id  for db_id = dbID and accession=term in dbxref
    	
    	int xrefID = dbxref.getDBXrefForName (term, "internal");
    	
    	
    	String insert = "INSERT into chado.cvterm (cv_id, name, dbxref_id, is_obsolete, is_relationshiptype) "
    			+ "VALUES (?, ?, ?, 0, 0)";
    	stmt = connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS);
    	stmt.setInt(1, cvID);
    	stmt.setString(2, term);
    	stmt.setInt(3, xrefID);
    	
    	System.out.println ("Executing \"" + insert + "\" for cv_id = " + cvID + " name=" +term + " dbxref_id = " + xrefID);
    	int count = stmt.executeUpdate();
    	ResultSet resKey = stmt.getGeneratedKeys();
    	if(resKey.next()){
    		newTermId = resKey.getInt(1);
    	}
    	System.out.println("Updated "+ count+ "raws in cvterm");
    	System.out.println("New term_id = "+newTermId);
    	return newTermId;
    }
    //ChadoDBxref for this connection
    public void setDBXrefRef (ChadoDBxref dbxref){
    	this.dbxref = dbxref;
    }

}
