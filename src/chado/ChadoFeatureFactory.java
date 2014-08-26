package chado;
import java.sql.*;

import org.apache.log4j.Logger;

public class ChadoFeatureFactory {
	private static final Logger LOG = Logger.getLogger(ChadoCVFactory.class);
    private final Connection connection;
    
    public ChadoFeatureFactory (Connection connection){
    	this.connection = connection;
    }
    public int getFeatureId (int typeID, String name) throws SQLException {
    	int featureId = 0;
    	String query = "SELECT feature_id from chado.feature where is_obsolete = FALSE AND type_id = ? AND uniquename=?";
    	PreparedStatement stmt = connection.prepareStatement (query);
    	stmt.setInt(1, typeID);
    	stmt.setString(2,name);
    	ResultSet res = stmt.executeQuery();
    	int count =0;
    	while (res.next()){
    		featureId = res.getInt("feature_id");
    		count++;
    	}
    	if(count > 1){
    		LOG.warn("Multiples features for name="+name);
    		System.out.println("Multiples features for name="+name);
    		
    	}
    	return featureId;
    }
    public int getFeaturePubRecord(int featureID, int pubID)throws SQLException {
    	int featurePubID = 0;
    	String query = "SELECT feature_pub_id FROM chado.feature_pub WHERE feature_id=? AND pub_id =?";
    	PreparedStatement stmt = connection.prepareStatement (query);
    	stmt.setInt(1, featureID);
    	stmt.setInt(2,pubID);
    	ResultSet res = stmt.executeQuery();
    	int count =0;
    	while (res.next()){
    		featurePubID = res.getInt("feature_pub_id");
    		count++;
    	}
    	if(count >1){
    		LOG.warn("Multiple records in feature_pub for feature="+featureID+" and pub:"+pubID);
    	}
    	return featurePubID;
    }
    public int setFeaturePubRecord(int featureID, int pubID) throws SQLException {
    	int featurePubID = getFeaturePubRecord (featureID, pubID);
    	if(featurePubID==0){
    		String query = "INSERT into chado.feature_pub (feature_id, pub_id) VALUES (?,?)";
    		PreparedStatement stmt = connection.prepareStatement (query, Statement.RETURN_GENERATED_KEYS);
    		stmt.setInt(1, featureID);
    		stmt.setInt(2,pubID);
    		int count = stmt.executeUpdate();
    		if (count > 1){
    			System.out.println("ChadoFeatureFactory: featureID="+ featureID+", pubID="+pubID);
    			System.out.println("query:\""+query+"\"");
    			System.out.println("made " + count + " updates");
    		}
    		ResultSet resKey = stmt.getGeneratedKeys();
        	if(resKey.next()){
        		featurePubID = resKey.getInt(1);
        	}
    	}
    	return featurePubID;
    }


}
