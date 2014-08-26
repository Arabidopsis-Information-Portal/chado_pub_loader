package loader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseUtil {
	
	public static String formJdbcUrl(String server, String databaseName, String schema) {

		try {
			Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Postgres JDBC driver is not on the class path.");
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

	 public static boolean checkDatabaseExists(String server, String databaseName,
             String userName, String password, String schema) {

		 String url = formJdbcUrl(server, databaseName, schema);

		 try {
			 Connection conn = DriverManager.getConnection(url, userName, password);
			 try {
				 conn.createStatement().execute("select 1");

				 return true;
			 } finally {
				 conn.close();
			 }
		 } catch (SQLException e) {
			 e.printStackTrace();
			// String message = e.getMessage();

		 }

		 return false;
	 }
	 public static Connection connect(String server, String databaseName, String userName, String password, String schema){
		 boolean dbExists;
		 Connection conn = null;
		 dbExists = checkDatabaseExists (server, databaseName, userName, password,schema);
		 String url = formJdbcUrl(server,databaseName,schema);
		 if (dbExists) {
			 try {
				 conn =  DriverManager.getConnection(url, userName, password);
			 } catch (SQLException ex){
				 ex.printStackTrace();
				 
			 }
		 } 
		 return conn;
	}


}
