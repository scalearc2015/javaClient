import java.io.*;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
//import java.util.Properties;
import java.math.BigDecimal;
//import java.math.BigInteger;
import java.util.logging.Logger;  
//import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import javax.management.Query;
//import javax.sql.*;
//import javax.sql.rowset.serial.SerialBlob;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;
//import oracle.sql.DATE;
import oracle.ucp.jdbc.PoolDataSource;
//import oracle.jdbc.pool.OracleDataSource;
//import oracle.jdbc.pool.OracleConnectionPoolDataSource;

import java.sql.Savepoint;

public class my implements Runnable
{
	private Connection conn_list;
	// private int max_queries;
	// static int query_count;
	static SynchronizedCounter query_count = new SynchronizedCounter();

	static int close_quer;
	//static int err_count;
	static SynchronizedCounter err_count = new SynchronizedCounter();
	//static int close_count;
	static SynchronizedCounter close_count = new SynchronizedCounter();
	//static int total_close_count;
	static SynchronizedCounter total_close_count = new SynchronizedCounter(); 
	static SynchronizedCounter total_start_count = new SynchronizedCounter();
	static SynchronizedCounter total_read_start_count = new SynchronizedCounter();
	static SynchronizedCounter total_write_start_count = new SynchronizedCounter();
	static SynchronizedCounter total_con_opening_time = new SynchronizedCounter();
	static SynchronizedCounter read_query_count = new SynchronizedCounter();
	static SynchronizedCounter write_query_count = new SynchronizedCounter();
	//static SynchronizedCounter preparecounter = new SynchronizedCounter();
	int cnt = 0;
	long err_no_data = 0;
	long err_exception = 0;
	long tstart_con;
	long tstop_con;
	int i = 0;			
	long query_no = 0;
	long time_lt_1ms = 0;
	long time_lt_2ms = 0;
	long time_lt_5ms = 0;
	long time_lt_10ms = 0;
	long time_lt_20ms = 0;
	long time_20ms_gte = 0;
	int prep_cnt; 
	int exec_cnt;
	int close_cnt;
	private long tot_conn = 0;
	//SQLServerDataSource ds;
	OracleDataSource ods;
	PoolDataSource ds;
	//PoolDataSource ds;
	long noOfQ;
	String query_val = null;
	String filename = null;
	Boolean flag ;
	private long repeat;
	String[] args;
	Logger logger;
	Mutex mutex;
	DataInputStream in = null;
	BufferedReader br = null;
	//    private	int k=0;
	private int mode=0;

	my( long tot_conn,long noOfQ, PoolDataSource ds2, String query_val, String filename,boolean flag ,long repeat,String[] args, Logger logger, Mutex mutex, BufferedReader br, DataInputStream in, int k) {
		this.tot_conn = tot_conn;
		this.noOfQ = noOfQ;
		this.ds = ds2;
		this.query_val = query_val;
		this.filename  = filename;
		this.flag = flag;
		this.repeat = repeat;
		this.args = args;
		this.logger = logger;
		this.mutex = mutex;
		this.br = br;
		this.in = in;
		this.mode = k;

	}

	my( long tot_conn,long noOfQ, OracleDataSource ds2, String query_val, String filename,boolean flag ,long repeat,String[] args, Logger logger, Mutex mutex, BufferedReader br, DataInputStream in, int k) {
		this.tot_conn = tot_conn;
		this.noOfQ = noOfQ;
		this.ods = ds2;
		this.query_val = query_val;
		this.filename  = filename;
		this.flag = flag;
		this.repeat = repeat;
		this.args = args;
		this.logger = logger;
		this.mutex = mutex;
		this.br = br;
		this.in = in;
		this.mode = k;

	}


	private static java.sql.Timestamp nowtimestamp()
	{
		java.sql.Timestamp  timestamp = new java.sql.Timestamp(new java.util.Date().getTime());
		System.out.println(timestamp.toString());
		return timestamp;
	}



	private String readFile(String fileName, Writer writerArg)throws FileNotFoundException, IOException
	{

	     BufferedReader br = new BufferedReader(new FileReader(fileName));
	     String nextLine = "";
	     StringBuffer sb = new StringBuffer();
	     while ((nextLine = br.readLine()) != null) {
	         //System.out.println("Writing: " + nextLine);
	         writerArg.write(nextLine);
	         sb.append(nextLine);
	     }
	     // Convert the content into to a string
	     String clobData = sb.toString();

	     // Return the data.
	     return clobData;
	 }


	/*
	 * This function kepps the aacount of time executed bt the query
	 */
	void updateTimer(long curr_exec_time)
	{

		if (curr_exec_time < 1)
			time_lt_1ms++;
		else if(curr_exec_time < 2)
			time_lt_2ms++;	
		else if(curr_exec_time < 5)
			time_lt_5ms++;
		else if(curr_exec_time < 10)
			time_lt_10ms++;	
		else if(curr_exec_time < 20)
			time_lt_20ms++;	
		else
			time_20ms_gte++;	

	}   
	/*
	 * Executes the  simple queries  and return the result set 
	 */
	ResultSet normalQuery(Connection con,String sql)
	{
		Statement statement=null;
		ResultSet rs=null;
		try
		{
			statement = con.createStatement();
		//	System.out.println("Hello");
			rs = statement.executeQuery(sql);
			while(rs.next())
			{
				//do nothing
			}
		//	System.out.println("Hellokfskdf;lks");
			statement.close();
		}

		catch(Exception e)
		{
			System.err.println("Error Normal Query Execution : " + e.getMessage());
			err_exception++;
			logger.warning(e.getMessage());
		}
		return rs;
	}
	/*
	 * Process autocommit  operation of a database connection depending on the value 
	 */

	void autoCommit(String str, Connection con)
	{
		if ( str.equalsIgnoreCase("false") ) 
		{

			try
			{
				con.setAutoCommit(false);
				//   System.out.println("Commit false");
			}
			catch (Exception e)
			{
				System.err.println("Error - Auto Commit false : " + e.getMessage() );
				err_exception++;
				logger.warning(e.getMessage());
			}
		}
		else if ( str.equalsIgnoreCase("true") ) 
		{

			try
			{
				con.setAutoCommit(true);
				//  System.out.println("Commit On");
			}
			catch (Exception e)
			{
				System.err.println("Error - Auto Commit true : " + e.getMessage() );
				err_exception++;
				logger.warning(e.getMessage());
			}
		}
	}
	/*
	 * Performs rollback opertaion on the database to return to some previous state
	 */

	void commitRollback(String str,Connection conn)
	{
		Connection con=conn;
		if ( str.equalsIgnoreCase("commit") )
		{
			try
			{
				con.commit();
			}
			catch (Exception e)
			{
				System.err.println("Error - Commit " + e.getMessage() );
				err_exception++;        
				logger.warning(e.getMessage());
			}

		}
		if ( str.equalsIgnoreCase("rollback") ) 
		{
			try
			{
				con.rollback();
			}
			catch (Exception e) 
			{
				System.err.println("Error - Rollback " + e.getMessage() );
				err_exception++;        
				logger.warning(e.getMessage());
			}

		}

	}

	Savepoint setSavePoint(Connection conn)
	{
		Savepoint save=null;
		Connection con=conn;
		try
		{
			save=con.setSavepoint();

		}
		catch(Exception e)
		{
			err_exception++;
			logger.warning(e.getMessage());
		}
		return save;
	}

	void rollback(Connection conn,Savepoint savepoint)
	{
		Connection con=conn;
		try
		{
			if (savepoint == null) 
			{
				// SQLException occurred in saving into Employee or Address tables
				con.rollback();
				System.out.println("JDBC Transaction rolled back successfully");
			}
			else 
			{
				con.rollback(savepoint);   
				con.commit();
			}	
		}
		catch(Exception e)
		{
			err_exception++;
			logger.warning(e.getMessage());
		}

	}

	/*
	 * Executes bExec and exec queries and return is true is variables are set 
	 */

	boolean execQuery(PreparedStatement pstmtn,String string, Pattern exec_individual_pat )
	{
		String exec_type=null;
		String[] exec_stmts = string.split("~");
		boolean fire_exec=false;
		PreparedStatement pstmt=pstmtn;
		/*
		 * Processes arugments of exec statements 
		 */
		for(int exec_counter=0; exec_counter <(exec_stmts.length); exec_counter++)
		{
			int exec_set_counter = exec_counter+1;
			Matcher exec_individual_m = exec_individual_pat.matcher(exec_stmts[exec_counter]);
			if (exec_individual_m.find()) 
			{
				exec_type = exec_individual_m.group(1);
				/*
				 * Set prepeared statement argument represented by exec_set_counter's valuse to interger
				 */
				if ( exec_type.equalsIgnoreCase("int") )
				{
					String str=exec_individual_m.group(2);
					if(str.startsWith("random"))
					{

						int minimum=0;
						int maximum=100;
						Pattern random_pat=Pattern.compile("random!(.+)!(.+)");
						Matcher random_m=random_pat.matcher(str);
						if(random_m.find())
						{	
							//	System.out.println("hello");
							minimum=Integer.parseInt(random_m.group(1));
							maximum=Integer.parseInt(random_m.group(2));
						}
						int randomNum = minimum + (int)(Math.random()*(maximum-minimum));
						try
						{
							pstmt.setInt(exec_set_counter,randomNum);
							//	System.out.println("random "+minimum+" "+maximum+" "+randomNum+str);
							fire_exec = true;
						}
						catch(Exception e)
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + " : " + e );
							err_exception++;
							logger.warning(e.getMessage());
						}

					}
					else
					{
						int exec_value = Integer.parseInt(str);
						try
						{
							pstmt.setInt(exec_set_counter,exec_value);
							fire_exec = true;
						}
						catch(Exception e)
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + "to integer  : " + e );
							err_exception++;
							logger.warning(e.getMessage());
						}
					}

				}
				/*
				 * Set prepeared statement argument represented by exec_set_counter's value to String
				 */
				else if ( exec_type.equalsIgnoreCase("string") )
				{
					String exec_value;
					exec_value = exec_individual_m.group(2);
					try 
					{
						pstmt.setString(exec_set_counter,exec_value);
					}
					catch(Exception e)
					{
						System.err.println("Error - Bind Variable : exec" + exec_cnt + "to string  : " + e );
						err_exception++;
						logger.warning(e.getMessage());

					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_set_counter's value to Float
				 */
				else if ( exec_type.equalsIgnoreCase("float") )
				{

					if(exec_individual_m.group(2).startsWith("random"))
					{

						float minimum=0;
						float maximum=100;
						Pattern random_pat=Pattern.compile("random!(.+)!(.+)");
						Matcher random_m=random_pat.matcher(exec_individual_m.group(2));
						if(random_m.find())
						{	
							minimum=Float.parseFloat(random_m.group(1));
							maximum=Float.parseFloat(random_m.group(2));
						}
						float randomNum = minimum + (float)(Math.random()*(maximum-minimum));;
						try
						{
							pstmt.setFloat(exec_set_counter,randomNum);
							fire_exec = true;
						}
						catch(Exception e)
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + " : " + e );
							err_exception++;
							logger.warning(e.getMessage());
						}

					}

					else
					{
						float exec_value;

						exec_value = Float.parseFloat(exec_individual_m.group(2));
						try
						{
							pstmt.setFloat(exec_set_counter,exec_value);
						}
						catch(Exception e)	
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + "to float  : " + e.getMessage() );
							err_exception++;
							logger.warning(e.getMessage());

						}
					}
					fire_exec = true;
				}

				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to Date
				 */
				else if ( exec_type.equalsIgnoreCase("date") )
				{
					DateFormat formatter;
					java.util.Date date;
					formatter = new SimpleDateFormat("dd-MMM-yy");
					try
					{
						date = (java.util.Date)formatter.parse(exec_individual_m.group(2));
						java.sql.Date sqldate = new java.sql.Date(date.getTime());
						pstmt.setDate(exec_set_counter, sqldate);
					}
					catch(Exception e)
					{
						System.err.println("Error - Bind Variable : exec" + exec_cnt + "to Date  : " + e.getMessage() );
						err_exception++;
						logger.warning(e.getMessage());

					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to Decimal 
				 */
				else if ( exec_type.equalsIgnoreCase("decimal") )
				{
					DecimalFormat format = (DecimalFormat) NumberFormat.getInstance(Locale.US);
					format.setParseBigDecimal(true);
					try
					{
						BigDecimal number = (BigDecimal) format.parse(exec_individual_m.group(2));
						pstmt.setBigDecimal(exec_set_counter, number);
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());

					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to timeStamp
				 */
				else if ( exec_type.equalsIgnoreCase("timestamp") ) 
				{
					String date_text = exec_individual_m.group(2);
					Timestamp ts = Timestamp.valueOf(date_text);               
					try
					{
						pstmt.setTimestamp(exec_set_counter, ts);
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());
					}
					fire_exec = true;
				} 
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to varchar
				 */
				else if ( exec_type.equalsIgnoreCase("varchar") ) 
				{
					String var_char = exec_individual_m.group(2);
					//	Timestamp ts = Timestamp.valueOf(date_text);               
					try
					{
					   // System.out.println("varchar");
						pstmt.setString(exec_set_counter,var_char);
					}
					catch(Exception e)
					{

						System.err.println("Error: " + e.getMessage());
						err_exception++;			
						logger.warning(e.getMessage());
					}
					fire_exec = true;
				} 
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to boolean
				 */
				else if ( exec_type.equalsIgnoreCase("boolean") ) 
				{
					String bool = exec_individual_m.group(2);
					try
					{
						pstmt.setBoolean(exec_set_counter,Boolean.parseBoolean(bool));
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;			
						logger.warning(e.getMessage());
					}
					fire_exec = true;
				} 
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to byte array
				 */
				else if ( exec_type.equalsIgnoreCase("byte") ) 
				{
					String byte1 = exec_individual_m.group(2);
					try
					{
						pstmt.setByte(exec_set_counter,Byte.parseByte(byte1));
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());
					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to short
				 */
				else if ( exec_type.equalsIgnoreCase("short") ) 
				{

					if(exec_individual_m.group(2).startsWith("random"))
					{

						short minimum=0;
						short maximum=100;
						Pattern random_pat=Pattern.compile("random!(.+)!(.+)");
						Matcher random_m=random_pat.matcher(exec_individual_m.group(2));
						if(random_m.find())
						{	
							minimum=Short.parseShort(random_m.group(1));
							maximum=Short.parseShort(random_m.group(2));
						}
						short randomNum =(short)( minimum + (int)(Math.random()*(maximum-minimum)));
						try
						{
							pstmt.setShort(exec_set_counter,randomNum);
							fire_exec = true;
						}
						catch(Exception e)
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + " : " + e );
							err_exception++;
							logger.warning(e.getMessage());
						}

					}
					else
					{
						try
						{
							pstmt.setShort(exec_set_counter,Short.parseShort(exec_individual_m.group(2)));
						}
						catch(Exception e)
						{
							System.err.println("Error: " + e.getMessage());
							err_exception++;
							logger.warning(e.getMessage());

						}
					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to long
				 */
				else if ( exec_type.equalsIgnoreCase("long") ) 
				{
					if(exec_individual_m.group(2).startsWith("random"))
					{

						long minimum=0;
						long maximum=100;
						Pattern random_pat=Pattern.compile("random!(.+)!(.+)");
						Matcher random_m=random_pat.matcher(exec_individual_m.group(2));
						if(random_m.find())
						{	
							minimum=Long.parseLong(random_m.group(1));
							maximum=Long.parseLong(random_m.group(2));
						}
						long randomNum =(long)( minimum + (int)(Math.random()*(maximum-minimum)));
						try
						{
							pstmt.setLong(exec_set_counter,randomNum);
							fire_exec = true;
						}
						catch(Exception e)
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + " : " + e );
							err_exception++;
							logger.warning(e.getMessage());
						}
					}
					else
					{
						try
						{
							pstmt.setLong(exec_set_counter,Long.parseLong(exec_individual_m.group(2)));
						}
						catch(Exception e)
						{
							logger.warning(e.getMessage());
							System.err.println("Error: " + e.getMessage());
							err_exception++;
							logger.warning(e.getMessage());
						}
					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to double
				 */
				else if ( exec_type.equalsIgnoreCase("double") ) 
				{

					if(exec_individual_m.group(2).startsWith("random"))
					{

						double minimum=0;
						double maximum=100;
						Pattern random_pat=Pattern.compile("random!(.+)!(.+)");
						Matcher random_m=random_pat.matcher(exec_individual_m.group(2));
						if(random_m.find())
						{	
							minimum=Double.parseDouble(random_m.group(1));
							maximum=Double.parseDouble(random_m.group(2));
						}
						double randomNum =(Double)( minimum + (int)(Math.random()*(maximum-minimum)));
						try
						{
							pstmt.setDouble(exec_set_counter,randomNum);
							fire_exec = true;
						}
						catch(Exception e)
						{
							System.err.println("Error - Bind Variable : exec" + exec_cnt + " : " + e );
							err_exception++;
							logger.warning(e.getMessage());
						}

					}


					else
					{
						try
						{
							pstmt.setDouble(exec_set_counter,Double.parseDouble(exec_individual_m.group(2)));
						}
						catch(Exception e)
						{
							logger.warning(e.getMessage());
							System.err.println("Error: " + e.getMessage());
							err_exception++;
							logger.warning(e.getMessage());

						}
					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to binary
				 */
				else if ( exec_type.equalsIgnoreCase("binary") ) 
				{	
					byte[] byteArray = exec_individual_m.group(2).getBytes();
					System.out.println(byteArray.toString());
					try
					{
						pstmt.setBytes(exec_set_counter,byteArray);
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());

					}
					fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to charstream
				 */
				else if ( exec_type.equalsIgnoreCase("charstream") ) 
				{	
					Reader reader = new StringReader(exec_individual_m.group(2));
					try
					{
						pstmt.setCharacterStream(exec_set_counter,reader,exec_individual_m.group(2).length());
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());
					}
					fire_exec = true;
				}
				else if(exec_type.equalsIgnoreCase("clobfile"))
				{
					   Connection conn = null;
					   try
					   {
						   conn=this.conn_list;
						   java.sql.Clob myClob2 = conn.createClob();
						   Writer clobwriter = myClob2.setCharacterStream(1);
						   String str = this.readFile(exec_individual_m.group(2), clobwriter);
						   myClob2.setString(1, str);      
				           pstmt.setClob(exec_set_counter, myClob2);
					   }
					   catch(Exception e)
					   {
						   err_exception++;
						   logger.warning(e.getMessage());
					   }
					   fire_exec = true;	
				}
				else if(exec_type.equalsIgnoreCase("blobfile"))
				{
					   Connection conn = null;
					   FileInputStream fis = null;
					   //System.out.println("Filename: "+exec_individual_m.group(2));
					   try
					   {
						   
						   File file=null;
						   try{
							   conn=this.conn_list;
						   file = new File(exec_individual_m.group(2));
						   }
						   catch(Exception e)
						   {
							   System.err.println("Error IO file: " + e.getMessage());
							   err_exception++;
							   logger.warning(e.getMessage());
						   }
						  // System.out.println("Hello");
						   fis = new FileInputStream(file);
						   byte b[] = new byte[(int)file.length()];
						   //fis.read(b);
				//		   System.out.println("Filename: "+exec_individual_m.group(2));
						   
						   java.sql.Blob myBlob2 = conn.createBlob();
					//	   ObjectOutputStream blobstream = new ObjectOutputStream(myBlob2.setBinaryStream(1));
						   //blobstream.writeObject(file);
						   	
						  // blobstream.write(b);
					//	   System.out.println("hellom");
						   //String str = this.readFile(exec_individual_m.group(2), blobstream);
						  // myBlob2.setBinaryStream(pos)(1, str);   
						  // myBlob2.setBytes(1,exec_individual_m.group().getBytes() );
						  // blobstream.close();
						//   System.out.println("hem");
						//   pstmt.setBlob(exec_set_counter, myBlob2);
				           pstmt.setBinaryStream(exec_set_counter, fis, fis.available());
					   }
					   catch(Exception e)
					   {
						   System.err.println("Error Blob file: " + e.getMessage());
							err_exception++;
							logger.warning(e.getMessage());
		
					   }
					   fire_exec = true;
				}
				/*
				 * Set prepeared statement argument represented by exec_setcounter's value to Character Large Object
				 */
				else if(exec_type.equalsIgnoreCase("clob"))
				{
					OracleConnection conn = null;
					try
					{
						conn = (OracleConnection) ds.getConnection();
						oracle.sql.CLOB cl = new oracle.sql.CLOB(conn, exec_individual_m.group(2).getBytes()); 
						pstmt.setClob(exec_set_counter, cl);
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());
					}
					fire_exec=true;
				}
				/*
				 * Set prepared statement argument represented by exec_setcounter's value to Binary Large Object
				 */
				else if(exec_type.equalsIgnoreCase("blob"))
				{

					fire_exec=true;
					OracleConnection conn = null;
					try
					{
						conn=(OracleConnection) this.conn_list;
						oracle.sql.BLOB b1=new oracle.sql.BLOB(conn, exec_individual_m.group(2).getBytes());
						pstmt.setBlob(exec_set_counter, b1);
					}
					catch(Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;			
						logger.warning(e.getMessage());
					}
					fire_exec = true;
				}
				else
				{
					System.err.println("Incorrect syntax: Undefined DataType");
					//	System.exit(0);						
				}

			}   // end if exec_individual_m.find()) 
		}   // end for exec_counter
		return fire_exec;
	}

	/*
	 * Close the prepared statement pstmt if it is not null 
	 */


	PreparedStatement close(PreparedStatement pstmt,int cnt)
	{
		if(pstmt == null )
		{
			System.err.println("Warning - Close" + close_cnt + ". Should not be null. Make sure you are not calling close twice on the same ID.");
			err_exception++;
		}
		else
		{
			try
			{
				pstmt.close();
				pstmt = null;
				//	System.out.println("Close Prepare statements prep counter : " + cnt );
			}
			catch(Exception e)
			{
				System.err.println("Error - Close Counter" + close_cnt );
				System.err.println("Msg - " + e.getMessage());
				err_exception++;
				logger.warning(e.getMessage());
			}
		}
		return pstmt;
	}
	/*
	 *	Checks if the prepared statement represented by pstmt is close..Returns true if closed and false if open 
	 */
	boolean isClose(PreparedStatement pstmt)
	{
		if(pstmt==null)
		{
			//System.out.println("Prepared statement is close");
			return true;
		}
		else 
		{
			//	System.out.println("Prepared statement is open");
			return false;
		}
	}

	/*
	 * Preocess the stored Procedure calls
	 */

	void procedure(Connection con, PreparedStatement pstmts ,String line)
	{
		String[] stmts = line.split("#");
		//System.out.println(stmts[0]+"In procedure");
		CallableStatement cs = null;
		try
		{
			cs = con.prepareCall(stmts[1]);
			for(int w= 2; w < (stmts.length); w++)
			{
				//	System.out.println("in procedure "+ stmts[w]);
				if (stmts[w].startsWith("params") == true)
				{
					String[] result = stmts[w].split("~");
					for(int j=1;j<(result.length);j++)
					{
						String[] parameter = result[j].split("@");
						if(parameter[0].equalsIgnoreCase("out"))
						{	
							//			System.out.println("in procedure  "+ parameter[0]+" "+parameter[1]+" "+j);
							cs.registerOutParameter(2, OracleTypes.NUMBER);
							if(parameter[1].equalsIgnoreCase("int")) 
								cs.registerOutParameter(j,java.sql.Types.INTEGER);				
							else if(parameter[1].equalsIgnoreCase("string") || (parameter[1].equalsIgnoreCase("varchar")))
								cs.registerOutParameter(j,java.sql.Types.VARCHAR);
							else if(parameter[1].equalsIgnoreCase("boolean"))
								cs.registerOutParameter(j,java.sql.Types.BOOLEAN);
							else if(parameter[1].equalsIgnoreCase("long") || parameter[1].equalsIgnoreCase("short") || parameter[1].equalsIgnoreCase("float") || parameter[1].equalsIgnoreCase("double") || parameter[1].equalsIgnoreCase("byte") || parameter[1].equalsIgnoreCase("decimal"))
								cs.registerOutParameter(j,java.sql.Types.NUMERIC);
							else if(parameter[1].equalsIgnoreCase("binary"))
								cs.registerOutParameter(j,java.sql.Types.BINARY);
							else if(parameter[1].equalsIgnoreCase("charstream") || parameter[1].equalsIgnoreCase("char"))
								cs.registerOutParameter(j,java.sql.Types.CHAR);
							else if(parameter[1].equalsIgnoreCase("date"))
								cs.registerOutParameter(j,java.sql.Types.DATE);
							else if(parameter[1].equalsIgnoreCase("time"))
								cs.registerOutParameter(j,java.sql.Types.TIME);
							else if(parameter[1].equalsIgnoreCase("timestamp"))
								cs.registerOutParameter(j,java.sql.Types.TIMESTAMP);
							else if(parameter[1].equalsIgnoreCase("blob"))
								cs.registerOutParameter(j,java.sql.Types.BLOB);
							else if(parameter[1].equalsIgnoreCase("clob"))
								cs.registerOutParameter(j,java.sql.Types.CLOB);
							else if(parameter[1].equalsIgnoreCase("cursor"))
							{	
								//System.out.println("coming here");
								cs.registerOutParameter(j,OracleTypes.CURSOR);
							}

						}//ENd IF if(parameter[0].equalsIgnoreCase("out"))
						else if(parameter[0].equalsIgnoreCase("int")) 
						{
							cs.setInt(j,Integer.parseInt(parameter[1]));
							System.out.println("Coming here as well in exec part and value of j =" + j + "and parsed value is " + Integer.parseInt(parameter[1]) );
						}
						else if(parameter[0].equalsIgnoreCase("string") || (parameter[0].equalsIgnoreCase("varchar")) )
						{ 	
							//		System.out.println("in string " +j+"  "+parameter[1]);
							cs.setString(1,parameter[1]);
							//	System.out.println("in string" +parameter[1]);
						}
						else if(parameter[0].equalsIgnoreCase("boolean"))
						{
							cs.setBoolean(j,Boolean.parseBoolean(parameter[1]));
						}
						else if(parameter[0].equalsIgnoreCase("long"))
						{
							cs.setLong(j,Long.parseLong(parameter[1]));
						}															
						else if(parameter[0].equalsIgnoreCase("double"))
						{
							cs.setDouble(j,Double.parseDouble(parameter[1]));
						}
						else if(parameter[0].equalsIgnoreCase("short"))
						{
							cs.setShort(j,Short.parseShort(parameter[1]));
						}													    	           
						else if(parameter[0].equalsIgnoreCase("byte"))
						{
							cs.setByte(j,Byte.parseByte(parameter[1]));
						}		
						else if(parameter[0].equalsIgnoreCase("binary"))
						{
							byte[] byteArray = parameter[1].getBytes();
							cs.setBytes(j,byteArray );	      	
						}	
						else if(parameter[0].equalsIgnoreCase("decimal"))
						{
							DecimalFormat format = (DecimalFormat) NumberFormat.getInstance(Locale.US);
							format.setParseBigDecimal(true);
							BigDecimal number = (BigDecimal) format.parse(parameter[1]);
							cs.setBigDecimal(j, number);	
						}
						else if(parameter[0].equalsIgnoreCase("charstream"))
						{	
							Reader reader = new StringReader(parameter[1]);
							pstmts.setCharacterStream(j,reader,parameter[1].length());
						}

						else if(parameter[0].equalsIgnoreCase("date"))
						{
							DateFormat formatter;
							java.util.Date date;																													formatter = new SimpleDateFormat("dd-MMM-yy");
							date = (java.util.Date)formatter.parse(parameter[1]);
							java.sql.Date sqldate = new java.sql.Date(date.getTime());
							pstmts.setDate(j, sqldate);
						}
						else if(parameter[0].equalsIgnoreCase("time"))
						{
							DateFormat sdf = new SimpleDateFormat("hh-mm-ss");
							java.util.Date date = (java.util.Date)sdf.parse(parameter[1]);
							java.sql.Time time = new java.sql.Time(date.getTime());	
							pstmts.setTime(j, time);
						}
						else if(parameter[0].equalsIgnoreCase("timestamp"))
						{
							String s = parameter[1];
							String[] vals = s.split(",");
							String ts = null;
							if(vals.length == 6 || vals.length == 7)
							{
								ts = vals[0];
								ts += '-';
								ts += vals[1];
								ts += '-';
								ts += vals[2];
								ts += ' ';
								ts += vals[3];
								ts += ':';
								ts += vals[4];
								ts += ':';
								ts += vals[5];
								if(vals.length == 7)
								{
									ts += '.';
									ts += vals[6];
								}
								java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf(ts);
								pstmts.setTimestamp(j, ts1);


							}
							else
							{
								System.out.println("Error in timestamp format. Exiting.");
								System.exit(0);
							}
						}			

					}//End of for result
				}//End if  params==true
			}//End of for
			query_count.increment();
			++i;
			//	ResultSet r_s = null;
			cs.execute();	
		}//end try
		catch(Exception e)
		{
			System.err.println("Error: In Procedure" + e.getMessage());
			err_exception++;
			logger.warning(e.getMessage());
		}

	}

	/*
	 * Proceses the batch query's prepared statement 
	 */
	PreparedStatement batchPrepare(Connection cons,PreparedStatement pstmts,String prep_query)
	{
		Connection con=cons;
		PreparedStatement pstmt=pstmts;
		try
		{
			pstmt = con.prepareStatement(prep_query);
			//	System.out.println("Batch stmt "+prep_query);

			query_no ++;
		}
		catch(Exception e)
		{
			System.err.println("Error - Prepare Statement. query_no = " + query_no + "Connection id : "  + ".Msg - " + e.getMessage());
			System.err.println("prep_cnt : batch" + "\tTimestamp : " + nowtimestamp()  +"\n" );
			err_exception++;
			logger.warning(e.getMessage());
		}
		return pstmt;
	}
	/*
	 * Process the executeUpdate and ExcuteQuery on Preapred Statmemt pstmtn
	 */

	void execute(PreparedStatement pstmtn, String line)
	{
		PreparedStatement pstmt=pstmtn;
		//System.out.println("in exec"+line);
		try
		{
			if ( line.toLowerCase().contains("execupdate") )
			{
				//System.out.println("In update");
				//	tStart = System.currentTimeMillis(); // start ticking clock for DB call
				pstmt.executeUpdate();			
				//tStop = System.currentTimeMillis(); // stop ticking clock for DB call
				//tTotal = tTotal + (tStop - tStart); // aggregate DB call clock 
			}
			else
			{
				//tStart = System.currentTimeMillis(); // start ticking clock for DB call
				ResultSet rs = pstmt.executeQuery();
				while(rs.next())
				{
					//do nothing
				}
				//tStop = System.currentTimeMillis(); // stop ticking clock for DB call
				//	curr_exec_time = tStop - tStart;
				//tTotal = tTotal + curr_exec_time; // aggregate DB call clock time

				//System.out.println("Execute Query");
				
				rs.close();
			}		// end else
		}
		catch(Exception e)
		{
			System.err.println("Error - Execute Query : " + e.getMessage() + "\tquery_no = " + query_no);
			err_exception++;
			logger.warning(e.getMessage());
		}
	}

	List<String> readFile(String file )
	{
		List<String> lines=new ArrayList<String>();
		try 
		{
			mutex.acquire();
			FileReader fileReader =null;
			BufferedReader bufferedReader = null;
			try
			{
				fileReader=  new FileReader(file);
				bufferedReader = new BufferedReader(fileReader);
				/*
				 * Reads the file filename from the buffer and stores it into list represented lines	 
				 */

				String line = null;
				while ((line = bufferedReader.readLine()) != null) 
				{
					lines.add(line);
				}
				bufferedReader.close();
			}
			catch (Exception e)
			{
				System.err.println("Error: File read. Msg : " + e.getMessage());
				err_exception++;
				logger.warning(e.getMessage());
			}
			finally
			{
				if(lines== null)
				{
					try
					{	
						/*
						 * Reset the bufferReader to point to first line
						 */
						bufferedReader.reset();
					}
					catch(Exception e)
					{
						System.err.println("Error : " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());
					}
				}
				mutex.release();
			}
		}
		catch(InterruptedException ie) 
		{
			System.err.println ("mutex failed");
			logger.warning(ie.getMessage());
			err_exception++;
			System.exit(0);
		}
		return lines;

	}


	public void run()
	{
		
		while  (total_start_count.value() < tot_conn)
		{
			total_start_count.increment();
		
		
		//System.out.println("Thread started"+Thread.activeCount());
		long tTotal, tStart, tStop, curr_exec_time;
		Boolean fire_exec = false;
		String sql;
		String prep_query;
		String exec_string;
		tTotal = 0;
		curr_exec_time = 0;
		Boolean query_flag = true;
		Boolean[] read_flag;
		long repeat1=repeat;
		tstart_con = System.currentTimeMillis();
		List<String> lines = new ArrayList<String>();
		//	Statement stmt=null;
		Savepoint[] savepoint=new Savepoint[10000];
		read_flag=new Boolean[10000];
		for(i=0;i<10000;i++)
		{
			read_flag[i]=false;
		}
		/*
		 * Creates tot_conn database Connections
		 */
		conn_list = null; 
		for ( i=0; i<1; i++)
		{
			try
			{
			//	System.out.println("Creating Connection...");
				if(flag)
					conn_list = ds.getConnection();
				else
					conn_list = ods.getConnection();
				//	System.out.println("Connection Created");
			}
			catch(Exception e)
			{
				System.out.println("Error creating connection "+e);
				err_exception++;
				logger.warning(e.getMessage());
				System.exit(1);
			}

		}
		int conn_id = 0; 
		i=0;
		int ps_arr_counter = 10000;
		PreparedStatement pstmt1 = null;
		ResultSet rs = null;
	
		boolean sleep_flag=false;
		PreparedStatement[] pstmts_arr = new PreparedStatement[ps_arr_counter]; 
		for(int pstmts_arr_cnt=0;pstmts_arr_cnt<ps_arr_counter;pstmts_arr_cnt++)
		{
			pstmts_arr[pstmts_arr_cnt] = null;
		}
			/*try
    {
    	if(flag)
    		con = ds.getConnection();
    	else
    		con=ods.getConnection();
	}
	catch (Exception e)
	{
		System.out.println ("eof");
		System.err.println("Error: " + e.getMessage());
		err_count.increment();
		logger.warning(e.getMessage());
	}*/



		/*	System.out.println("Creating a procedure");

	try
	{
		SQL = "create table temp_table1 (ID number, name varchar2(20 Byte), salary number)";
		stmt = con.createStatement();
		stmt.execute(SQL);

		SQL= "create or replace  procedure proc1 (names in varchar2, results out number) is begin select salary into results from temp_table1 where name = 'names' ; commit; end;";
		stmt = con.createStatement();
		stmt.execute(SQL);			
	}
	catch(Exception e)
	{
		System.err.println("ERROR: Creating a procedure");
	}


	System.out.println("Executing the procedure");

	String stored_proc = "call proc1(?, ?)";
	CallableStatement cs = null;
	try{
	cs = con.prepareCall(stored_proc);
	cs.setString(1, "Prateek");
	cs.registerOutParameter(2, OracleTypes.NUMBER);
	ResultSet r_s = null;
	r_s = cs.executeQuery();
	}
	catch(	Exception e)
	{

	}*/
		Pattern prepst_pat = Pattern.compile("^prep(\\d+)#(.+)");
		Pattern exec_pat = Pattern.compile("exec(?:update)?(\\d+)#(.+)");     // Normal exec or execupdate for select/update prep queries
		Pattern exec_individual_pat = Pattern.compile("(.+)@(.+)");
		Pattern prepst_close = Pattern.compile("close(\\d+)");
		Pattern normal_q_pat = Pattern.compile("normal#(.+)");
		Pattern auto_commit_p = Pattern.compile("autocommit#(.+)");
		Pattern commit_rollback_p = Pattern.compile("^(commit|rollback)");
		Pattern proc_pat = Pattern.compile("procedure#(.+)");
		Pattern batch_pat = Pattern.compile("batch#(.+)");
		Pattern b_exec_pat =Pattern.compile("bExec#(.+)");
		Pattern end_pat=Pattern.compile("end");
		Pattern sleep_pat=Pattern.compile("sleep#(.+)");
		Pattern savepoint_pat=Pattern.compile("savepoint#(.+)");
		Pattern rollback_pat=Pattern.compile("rollback#(.+)");

		//Read the batch file 

		lines=readFile(filename);

			tstop_con = System.currentTimeMillis();
			//    System.out.println("Time taken to open connection is " + (tstop_con - tstart_con));
			total_con_opening_time.set_value(total_con_opening_time.long_value() + (tstop_con - tstart_con));
			//	SQL = null;
			//String strLine = null;
			if(query_val.contains("batch"))
			{
				//	System.out.println("No. of queries "+noOfQ);
				while(true)
				{
					try
					{		
						for (String line : lines) 
						{
							if(noOfQ<=query_count.value() && mode==0)
							{
								try
								{
									for(int pstmts_arr_cnt=0;pstmts_arr_cnt<ps_arr_counter;pstmts_arr_cnt++)
									{
										if (pstmts_arr[pstmts_arr_cnt] != null)
										{
											//	System.out.println("Close Prepare statements prep counter : " + pstmts_arr_cnt ); 
											pstmts_arr[pstmts_arr_cnt].close();
											pstmts_arr[pstmts_arr_cnt] = null;

										}
									}
								}
								catch (SQLException se)
								{
									se.printStackTrace();
									logger.warning(se.getMessage());
									err_exception++;
								}		    
								try
								{
									for (i=0; i<1; i++)
									{
										conn_list.close();
										conn_list = null;
										total_close_count.increment();
									}

								}
								catch (SQLException se)
								{
									se.printStackTrace();
									logger.warning(se.getMessage());
									err_exception++;
								}
								System.exit(0);

								break;
							}
							try
							{
								Matcher normal_q_m = normal_q_pat.matcher(line);
								if (normal_q_m.find()) 
								{
									sql = normal_q_m.group(1);
									tStart = System.currentTimeMillis();
									rs=normalQuery(conn_list,sql);
									tStop = System.currentTimeMillis();
									query_no ++;
									curr_exec_time = tStop - tStart;
									tTotal = tTotal + curr_exec_time; 
									updateTimer(curr_exec_time);
									read_query_count.increment();

									query_flag=true;
									
								}   // end if normal query match exec
								Matcher auto_commit_m = auto_commit_p.matcher(line);
								if (auto_commit_m.find()) 
								{
									String str=auto_commit_m.group(1);
									autoCommit(str,conn_list);
									query_flag=false;
								}  
								Matcher commit_rollback_m = commit_rollback_p.matcher(line);
								if (commit_rollback_m.find()) 
								{
									String str=commit_rollback_m.group(1);
									commitRollback(str,conn_list);
									query_flag=false;
								} 

								Matcher savepoint_m=savepoint_pat.matcher(line);
								if(savepoint_m.find())
								{
									savepoint[Integer.parseInt(savepoint_m.group(1))]=setSavePoint(conn_list);
									query_flag=false;
								}
								Matcher rollback_m=rollback_pat.matcher(line);
								if(rollback_m.find())
								{
									rollback(conn_list,savepoint[Integer.parseInt(rollback_m.group(1))]);
									query_flag=false;
								}
								Matcher sleep_m= sleep_pat.matcher(line);
								if(sleep_m.find())
								{
									//						System.out.println("Sleeping.... "+sleep_m.group(1));
									Thread.sleep(Long.parseLong(sleep_m.group(1)));
								}
								Matcher prepst_m = prepst_pat.matcher(line);
								if (prepst_m.find()) 
								{
									prep_cnt = Integer.parseInt(prepst_m.group(1));
									prep_query = prepst_m.group(2);
									try
									{
										pstmts_arr[prep_cnt] = conn_list.prepareStatement(prep_query);
										query_no ++;
									}
									catch(Exception e)
									{
										System.err.println("Error - Prepare Statement. query_no = " + query_no + "Connection id : " + conn_id + ".Msg - " + e.getMessage());
										System.err.println("prep_cnt : " + prep_cnt + "\tTimestamp : " + nowtimestamp()  +"\n" );
										err_exception++;
										logger.warning(e.getMessage());
									}
									read_flag[prep_cnt]=true;
									query_flag=false;
								}	// end ifprepst_m.find();

								/////   Exec pattern
								Matcher exec_m = exec_pat.matcher(line);
								if (exec_m.find()) 
								{
									exec_cnt = Integer.parseInt(exec_m.group(1));
									exec_string = exec_m.group(2);
									//	System.out.println(" sdfs "+exec_string);
									if(!isClose(pstmts_arr[exec_cnt]))
										fire_exec=execQuery(pstmts_arr[exec_cnt],exec_string,exec_individual_pat);
									else if(isClose(pstmts_arr[exec_cnt]))
										System.err.println("Prepared statement "+exec_cnt+" not initalized");
									if(fire_exec == true  )
									{	
										if (!isClose(pstmts_arr[exec_cnt]))
										{
											tStart = System.currentTimeMillis();
											execute(pstmts_arr[exec_cnt],line);
											tStop = System.currentTimeMillis();
											query_no ++;
											curr_exec_time = tStop - tStart;
											tTotal = tTotal + curr_exec_time; 
											updateTimer(curr_exec_time);
										}
										fire_exec = false;
										if(read_flag[exec_cnt])
											read_query_count.increment();
										else
											write_query_count.increment();
									}   // end if fire_exec == true )  
									query_flag=true;
								}   // end if match exec    
								Matcher close_m = prepst_close.matcher(line);
								if (close_m.find())
								{
									close_cnt = Integer.parseInt(close_m.group(1)); //System.out.println("Close - " + close_cnt);
									pstmts_arr[close_cnt]=close(pstmts_arr[close_cnt],close_cnt);
									query_flag=false;
								}   // end if regex close
								Matcher proc_m = proc_pat.matcher(line);
								if (proc_m.find())
								{
									procedure(conn_list,pstmt1,line);			
								}//ENd of proc_m
								Matcher batch_m= batch_pat.matcher(line);
								if(batch_m.find())
								{
									prep_query = batch_m.group(1);
									pstmts_arr[ps_arr_counter-1]=batchPrepare(conn_list,pstmts_arr[ps_arr_counter-1],prep_query);
									query_flag=false;
								}

								Matcher bexec_m= b_exec_pat.matcher(line);
								boolean b=bexec_m.find();
								if(b)
								{
									//	System.out.println("batch execute "+bexec_m.group(1));
									if (b) 
									{
										exec_string = bexec_m.group(1);
										try
										{
											fire_exec=execQuery(pstmts_arr[ps_arr_counter-1],exec_string,exec_individual_pat);
											pstmts_arr[ps_arr_counter-1].addBatch();
										}
										catch(Exception e)
										{
											System.err.println("Error: "+e);
											err_exception++;
											logger.warning(e.getMessage());
										}
										
									}   // end if match exec   										    
									write_query_count.increment();
								}
								Matcher end_m= end_pat.matcher(line);
								if(end_m.find())
								{
									try
									{
										tStart = System.currentTimeMillis();
										pstmts_arr[ps_arr_counter-1].executeBatch();
										tStop = System.currentTimeMillis();
										query_no ++;
										curr_exec_time = tStop - tStart;
										tTotal = tTotal + curr_exec_time; 
										updateTimer(curr_exec_time);

									}
									catch(Exception e)
									{
										System.err.println("Error: Execute Batch " + e.getMessage());
										err_exception++;
										logger.warning(e.getMessage());
									}
									query_flag =true;
								}				
							}//End of try
							catch (Exception e)
							{

								System.err.println("Error: " + e.getMessage());
								err_exception++;
								logger.warning(e.getMessage());
							}
							if(query_flag & !sleep_flag)
							{
								query_count.increment();
								
														
								//System.out.println(" Query: "+line);									
							}
							sleep_flag=false;
						}//ENd of For loop lines

					}
					catch (Exception e)
					{
						System.err.println("Error: " + e.getMessage());
						err_exception++;
						logger.warning(e.getMessage());
					}
					if(mode==1)
					{			
						repeat1--;
						if(repeat1<=0)
							break;
					}									
				}//ENd of while

			}//ENd of If

			try
			{
				for(int pstmts_arr_cnt=0;pstmts_arr_cnt<ps_arr_counter;pstmts_arr_cnt++)
				{
					if (pstmts_arr[pstmts_arr_cnt] != null)
					{
						//	System.out.println("Close Prepare statements prep counter : " + pstmts_arr_cnt ); 
						pstmts_arr[pstmts_arr_cnt].close();
						pstmts_arr[pstmts_arr_cnt] = null;
					}
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
				err_exception++;
				logger.warning(se.getMessage());
			}		    
			try
			{ 
				for (i=0; i<1; i++)
				{
					conn_list.close();
					conn_list = null;
					total_close_count.increment();
					//			System.out.println("Connection closed");
				}

			}
			catch (SQLException se)
			{
				se.printStackTrace();
				err_exception++;
				logger.warning(se.getMessage());
			}				

			

		}// End of while
		close_count.increment();
				//	 System.out.println("NO. of query executed:  "+query_count.value());
	}//End of Run

}//end of class

