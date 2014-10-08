package loader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;
public class DatabaseUtil {
	static Logger LOG = Logger.getLogger(DatabaseUtil.class);
	public static String formJdbcUrl(String server, String databaseName, String schema) {
		
		try {
			Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LOG.fatal("Postgres JDBC driver is not on the class path.");
            System.exit(1);
        }
        
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://").append(server).append('/');
        url.append(databaseName);
        if(schema != null){
        	url.append("?searchpath=").append(schema);
        } 
        return url.toString();
	}

	 public static Connection connect(String server, String databaseName, String userName, String password, String schema){
		 Connection conn = null;
		 String url = formJdbcUrl(server,databaseName,schema);
		 try {
			 conn =  DriverManager.getConnection(url, userName, password);
			 String query = "SHOW search_path";
			 PreparedStatement stmt = conn.prepareStatement(query);
			 ResultSet res = stmt.executeQuery();
			 while(res.next()){
				 LOG.info("DB Schema is "+ res.getString(1));
			 }
		 } catch (SQLException ex){
			 ex.printStackTrace();
			 
		 }
		 return conn;
	}


}
