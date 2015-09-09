/**
 * A Simple DataSource sample without using JNDI.
 * Please compare to DataSourceJNDI.java
 *
 * Please use jdk1.2 or later version
 */

// You need to import the java.sql package to use JDBC
import java.sql.*;
import javax.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

public class DataSource
{
	/*
  public static void main (String args [])
    throws SQLException
  {
    // Create a OracleDataSource instance explicitly
    OracleDataSource ods = new OracleDataSource();

    // Set the user name, password, driver type and network protocol
    ods.setUser("prateek");
    ods.setPassword("prateek123");
    //ods.setServerName("hh-3v9dsfi0ipnm");
    //ods.setServiceName("scalarc_prateek");
    //ods.setDatabaseName("prateek");
    //ods.setDriverType("oci9");
    //ods.setNetworkProtocol("ipc");
    //ods.setNetworkProtocol("tcp");
    
    //url = "jdbc:oracle:oci:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=server6)(PORT=1573))(CONNECT_DATA=(SERVICE_NAME=rdbms3.server6.us.alchemy.com)))"
    
    // working
    String url="jdbc:oracle:oci:@"+"(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=199.199.199.70)(PORT=1521))"+"(CONNECT_DATA= (SERVICE_NAME=scalarc_prateek)))";
    ods.setURL(url);
    
    // Retrieve a connection
    Connection conn = ods.getConnection();
   
    getUserName(conn);
    // Close the connection
    conn.close();
    conn = null;
  }

  static void getUserName(Connection conn)
       throws SQLException
  {
    // Create a Statement
    Statement stmt = conn.createStatement ();

    // Select the USER column from the dual table
    ResultSet rset = stmt.executeQuery ("select USER from dual");

    // Iterate through the result and print the USER
    while (rset.next ())
      System.out.println ("User name is " + rset.getString (1));

    // Close the RseultSet
    rset.close();
    rset =  null;

    // Close the Statement
    stmt.close();
    stmt = null;
  }
  */
}



