
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;  
import java.util.logging.Level;  
import java.util.logging.Logger;  
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.simplefan.FanEventListener;
import oracle.simplefan.FanManager;
import oracle.simplefan.FanSubscription;
import oracle.simplefan.LoadAdvisoryEvent;
import oracle.simplefan.NodeDownEvent;
import oracle.simplefan.ServiceDownEvent;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.jdbc.PoolDataSourceImpl;


import java.net.URL;
import java.net.URLClassLoader;


public class connectDS
{
	/*
	 * Function to read config file
	 */
	static List<String> readFile(String filename)
	{

		List<String> lines = new ArrayList<String>();
		try
	    {
			FileReader fileReader = new FileReader(filename);
	    	BufferedReader bufferedReader = new BufferedReader(fileReader);
	    	String line = null;
	    	while ((line = bufferedReader.readLine()) != null) 
	    	{
	    		lines.add(line);
	    	}
	    	bufferedReader.close();
	    }
	    catch (IOException io)
	    {
	        System.err.println("Error: File read. Msg : " + io.getMessage());
	    }
	    return lines;
		
	}

	/*
	 * Function to set System Properties
	 */
	static void setProperties()
	{
	    Properties p = new Properties(System.getProperties());
		p.put("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
		p.put("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "OFF"); // or any other
		System.setProperties(p);
		
	}

	/*
	 * Function to set logger file
	 */
	static FileHandler loggerfile(Logger logger)
	{
		FileHandler fh=null;
		try
		{  
            // This block configure the logger with handler and formatter  
	 		fh = new FileHandler("JavaOracleClientLogFile"+System.currentTimeMillis()+".log");  
            logger.addHandler(fh);    
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);       
            // the following statement is used to log any messages  
            logger.info("LOGS");  
              
        } 
		catch (SecurityException e) 
		{  
            e.printStackTrace();  
        }
		catch (IOException e) 
		{  
            e.printStackTrace();  
        }  
		return fh;
	}
	
	
	static OracleDataSource create(String url, String uName, String password)
	{
		OracleDataSource ds=null;
		try
		{
			ds = new OracleDataSource();
			ds.setURL(url);
		    ds.setUser(uName);                                  
			ds.setPassword(password);			
		}
		catch(Exception e)
		{
			
		}
		
		return ds;
	}
	
	
	static PoolDataSource createPool(String url, String uName, String password, int minPoolSize,int maxPoolSize,int initialPoolSize)
	{
		PoolDataSource ds = PoolDataSourceFactory.getPoolDataSource();
		try
		{
			UniversalConnectionPoolManager mgr = UniversalConnectionPoolManagerImpl. getUniversalConnectionPoolManager();
			ds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
			ds.setUser(uName);                                  
			ds.setPassword(password);
			ds.setURL(url);
			mgr.createConnectionPool((oracle.ucp.UniversalConnectionPoolAdapter)ds);
			ds.setMinPoolSize(minPoolSize);
			ds.setMaxPoolSize(maxPoolSize);
			ds.setInitialPoolSize(initialPoolSize);
		}
		catch(Exception e)
		{
			System.out.println("SQL wala error"+e.getMessage()+" Pool size "+ds.getInitialPoolSize());
		}
		return ds;
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, PropertyVetoException, SQLException 
	{
		ExecutorService executor = Executors.newFixedThreadPool(10000);
		String query_val = null;
		String flag = null;
		int i = 0;
		final SynchronizedCounter total_c = 	new SynchronizedCounter();
		final SynchronizedCounter pool_s = 		new SynchronizedCounter();
		final SynchronizedCounter conc = 		new SynchronizedCounter();
		final SynchronizedCounter tot_op_time = new SynchronizedCounter();
		final SynchronizedCounter start_time = 	new SynchronizedCounter();	
	//	System.out.println("Filename "+args[0]);
		
		String sName=null;
		String uName="system";
		String password="Info_123";
		String dbName="orcl";
		String fpath=null;
		String cType="oci_thin";
		String query_value="batch";
		int tConn=1;
		long concr=1;
		long qpconn=10;
		int sPort=1521;
		long repeat=1;
		int mode=0;
	    boolean pool=false;
	    int minPoolSize=2;
	    int maxPoolSize=4;
	    int initialPoolSize=2;
	    boolean service=true;
	    boolean drcp=false;
	    boolean statusPerSec=false;
	    String classPath=null;
	    int sdu=-1;
	    
	    
	    
	    FileHandler fh;
		Logger logger = Logger.getLogger("MyLog");
		fh=loggerfile(logger);
		
	    

		List<String> lines = new ArrayList<String>();
		if (args.length == 0)
		{
			System.err.println("Config file path missing ");
			System.err.println("USAGE: java -jar <jar file> <Config file path>");
			System.exit(1);
		}
		
		
		/*
		 * Read config file 
		 */
		lines=readFile(args[0]);
		/*
		 * Overwite the config file arguments with the command line arguments
		 */
		for(int j=1;j<args.length;j++)
		{
			lines.add(args[j]);
		}
		
		Pattern sName_pat=Pattern.compile("^ServerName:(.+)");
		Pattern sPort_pat=Pattern.compile("^ServerPort:(\\d+)");
		Pattern uName_pat=Pattern.compile("^UserName:(.+)");
		Pattern password_pat=Pattern.compile("^Password:(.+)");
		Pattern db_pat=Pattern.compile("^DatabaseName:(.+)");
		Pattern tConn_pat=Pattern.compile("^TotalConnections:(\\d+)");
		Pattern conc_pat=Pattern.compile("^Concurrency:(\\d+)");
		Pattern q_con_pat=Pattern.compile("^MaxQuery:(\\d+)");
		Pattern query_value_pat=Pattern.compile("^Query:(.+)");
		Pattern fpath_pat=Pattern.compile("^AbsoluteFilePath:(.+)");
		Pattern cType_pat=Pattern.compile("^ClientType:(.+)");
//		Pattern prepCount_pat =Pattern.compile("PrepCount:(\\d+)");
		Pattern mode_pat=Pattern.compile("^Mode:(\\d)");
		Pattern repeat_pat=Pattern.compile("^Repeat:(\\d+)");
		Pattern pool_pat=Pattern.compile("^Pool:(.+)");
		Pattern minPoolSize_pat=Pattern.compile("^MinPoolSize:(\\d+)");
		Pattern maxPoolSize_pat=Pattern.compile("^MaxPoolSize:(\\d+)");
		Pattern initialPoolSize_pat=Pattern.compile("^InitialPoolSize:(\\d+)");
		Pattern service_pat=Pattern.compile("^Service/SID:(.+)");
		Pattern drcp_pat=Pattern.compile("^Drcp:(.+)");
		Pattern classPath_pat=Pattern.compile("^Classpath:(.+)");
		Pattern sdu_pat=Pattern.compile("^SDU:(.+)");
		Pattern statusPerSec_pat=Pattern.compile("^StatusPerSec:(.+)");
		
		
		String info="";
		
		for (String line : lines) 
		{
			/*
			 * Assigning values to the variable from config file
			 */
			Matcher sName_m = sName_pat.matcher(line);
			if (sName_m.find()) 
			{
				sName=sName_m.group(1);
				//System.out.println("Servername:"+sName );
				info=info+"\n\tServerName:"+sName;
			}
			Matcher sPort_m = sPort_pat.matcher(line);
			if (sPort_m.find()) 
			{
				sPort=Integer.parseInt(sPort_m.group(1));
				//System.out.println("ServerPort:"+sPort );
				info=info+"\n\tServerPort:"+sPort;
			}
			Matcher uName_m = uName_pat.matcher(line);
			if (uName_m.find()) 
			{
				uName=uName_m.group(1);
				//System.out.println("UserName: "+uName );
				info=info+"\n\tUserName:"+uName;
			}
			Matcher password_m = password_pat.matcher(line);
			if (password_m.find()) 
			{
				password=password_m.group(1);
				//System.out.println("Password: "+password );
				info=info+"\n\tPassword:"+password;
			}
			Matcher db_m = db_pat.matcher(line);
			if (db_m.find()) 
			{
				dbName=db_m.group(1);
			//	System.out.println("Database name: "+dbName );
				info=info+"\n\tDatabaseName:"+dbName;
			}
			Matcher tConn_m = tConn_pat.matcher(line);
			if (tConn_m.find()) 
			{
				tConn=Integer.parseInt(tConn_m.group(1));
			//	System.out.println("Total  connection : "+tConn );
				info=info+"\n\tTotalConnection:"+tConn;
			}
			Matcher conc_m = conc_pat.matcher(line);
			if (conc_m.find()) 
			{
				concr=Long.parseLong(conc_m.group(1));
			//	System.out.println("Concurrency: "+concr );
				info=info+"\n\tConcurrency:"+concr;
			}
			Matcher q_con_m = q_con_pat.matcher(line);
			if (q_con_m.find()) 
			{
				qpconn=Long.parseLong(q_con_m.group(1));
		//		System.out.println("Max query: "+qpconn );
				info=info+"\n\tMaxQuery:"+qpconn;
			}
			Matcher fpath_m = fpath_pat.matcher(line);
			if (fpath_m.find()) 
			{
				fpath=fpath_m.group(1);
	//			System.out.println("Absolute File path: "+fpath );
				info=info+"\n\tAbsoluteFilePath:"+fpath;
			}
			Matcher cType_m = cType_pat.matcher(line);
			if (cType_m.find()) 
			{
				cType=cType_m.group(1);
//				System.out.println("Client Type: "+cType );
				info=info+"\n\tClientType:"+cType;
			}
			Matcher query_value_m = query_value_pat.matcher(line);
			if (query_value_m.find()) 
			{
				query_value=query_value_m.group(1);
				//System.out.println("Query VAL: "+query_value );
			}
			Matcher mode_m = mode_pat.matcher(line);
			if (mode_m.find()) 
			{
				mode=Integer.parseInt(mode_m.group(1));
			//	System.out.println("mode: "+mode );
				info=info+"\n\tMode:"+mode;
			}
			Matcher repeat_m = repeat_pat.matcher(line);
			if (repeat_m.find()) 
			{
				repeat=Long.parseLong(repeat_m.group(1));
			//	System.out.println("Repeat: "+repeat );
				info=info+"\n\tRepeat:"+repeat;
			}
			Matcher pool_m = pool_pat.matcher(line);
			if (pool_m.find()) 
			{
				pool=Boolean.parseBoolean(pool_m.group(1));
			//	System.out.println("Pooling : "+pool);
				info=info+"\n\tPool:"+pool;
			}
			Matcher minPoolSize_m = minPoolSize_pat.matcher(line);
			if (minPoolSize_m.find()) 
			{
				minPoolSize=Integer.parseInt(minPoolSize_m.group(1));
			//	System.out.println("Min Pool size: "+minPoolSize );
				info=info+"\n\tMinPoolSize:"+minPoolSize;
			}
			Matcher maxPoolSize_m = maxPoolSize_pat.matcher(line);
			if (maxPoolSize_m.find()) 
			{
				maxPoolSize=Integer.parseInt(maxPoolSize_m.group(1));
			//	System.out.println("Max pool size: "+maxPoolSize);
				info=info+"\n\tMaxPoolSize:"+maxPoolSize;
			}
			Matcher initialPoolSize_m = initialPoolSize_pat.matcher(line);
			if (initialPoolSize_m.find()) 
			{
				initialPoolSize=Integer.parseInt(initialPoolSize_m.group(1));
			//	System.out.println("Inital Pool size: "+initialPoolSize);
				info=info+"\n\tInitalPoolSize:"+initialPoolSize;
			}
			Matcher drcp_m = drcp_pat.matcher(line);
			if (drcp_m.find()) 
			{
				drcp=Boolean.parseBoolean(drcp_m.group(1));
		//		System.out.println("drcp : "+drcp);
				info=info+"\n\tDrcp:"+drcp;
			}
			Matcher service_m = service_pat.matcher(line);
			if (service_m.find()) 
			{
				if(service_m.group(1).equalsIgnoreCase("service"))
				service=true;
				else
					service=false;
			//	System.out.println("service : "+service);
				info=info+"\n\tservice:"+service;
			}
			Matcher classPath_m = classPath_pat.matcher(line);
			if (classPath_m.find()) 
			{
				classPath=classPath_m.group(1);
			//	System.out.println("Class Path : "+classPath);
				info=info+"\n\tClassPath : "+classPath;
				
			}
			Matcher sdu_m = sdu_pat.matcher(line);
			if (sdu_m.find()) 
			{
				sdu=Integer.parseInt(sdu_m.group(1));
		//		System.out.println("SDU : "+sdu);
				info=info+"\n\tSDU:"+sdu;
			}
			Matcher statusPerSec_m = statusPerSec_pat.matcher(line);
			if (statusPerSec_m.find()) 
			{
				statusPerSec=Boolean.parseBoolean(statusPerSec_m.group(1));
			//	System.out.println("Status Per Second : "+statusPerSec);
				info=info+"\n\tStatusPerSec:"+statusPerSec;
			}
		}

		logger.info(info);
		
		if(sName==null)
		{
			System.err.println("ServerName cannot be  null");
			System.exit(1);
		}
		if(repeat<1&&((mode==1)||(mode==3)))
		{
			System.err.println("Repeat value should be greater than 0");
			System.exit(1);
		}
	/*	if(classPath==null)
		{
			System.err.println("Class Path cannot be null");
			System.exit(1);
		}
		String classpath1 = System.getProperty("java.class.path");
		String sep = System.getProperty("path.separator");
		classpath1 = classpath1 + sep + classPath;
		System.setProperty("java.class.path", classpath1);
		System.out.println("class path " + classpath1);
		System.setProperty("java.class.path", classPath);*/
		total_c.set_value(tConn);	
		/*FileHandler fh;
		Logger logger = Logger.getLogger("MyLog");
		fh=loggerfile(logger);*/
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() 
		    {
		    	System.out.println("-----End------");
				int total_close_conn_cout = my.total_close_count.value();
				int colcount = my.close_count.value();
				int query_coun = my.query_count.value();
				int ecount = my.err_count.value();
				query_coun=my.query_count.value();
				System.out.println("Total Connections:  " + total_c.value());
				System.out.println("Max Pool Size:  " + pool_s.value());
				System.out.println("Concurrency:  " + conc.value());
                long tot_tim_con = 0;
				long tot_tim_con_op = tot_op_time.long_value();
				long ed_time1 = System.nanoTime();
				long str_time1 =  start_time.long_value();
				int avg_query_prop_sec = 0;
				long total_con_time = my.total_con_opening_time.long_value();
				int avg_con_time = (int)(total_con_time/total_c.value());
				tot_tim_con =  (ed_time1 - str_time1);
			//	System.out.println("Time Taken (in millisecond):  " + TimeUnit.MILLISECONDS.convert(tot_tim_con + tot_tim_con_op, TimeUnit.NANOSECONDS));
			//	System.out.println("Total time taken to open connection(in milisecond):" + total_con_time + ". Average time :" + avg_con_time);
				System.out.println("Time Taken (in second):  " + TimeUnit.SECONDS.convert(tot_tim_con + tot_tim_con_op, TimeUnit.NANOSECONDS));
				System.out.println("Connections Processed:  " + (total_close_conn_cout));
				System.out.println("Total Threads Completed: " + (colcount));
				System.out.println("Total Query Processed: " + (query_coun));
				try
				{
					avg_query_prop_sec = (int) ((query_coun)/TimeUnit.SECONDS.convert(tot_tim_con + tot_tim_con_op, TimeUnit.NANOSECONDS));
					System.out.println("Average Query/Second: " + (avg_query_prop_sec));
				}
				catch(Exception e)
				{
					avg_query_prop_sec=-1;
				}
				
				System.out.println("Error Count: " + (ecount));
				System.out.println("---Finished---");
		    }
		}));
		
		//System.setProperty("java.net.preferIPv6Addresses", "false");
		
		//setProperties(); //ConnectionPooling
				
		
		OracleDataSource ods = new OracleDataSource();
		PoolDataSource ds = PoolDataSourceFactory.getPoolDataSource();
		// Creaking Connection string
		String url = "jdbc:oracle:";
		if(cType.contains("thin"))
			url = url+"thin";
		else
			url = url+"oci8";

			url=url+":@(DESCRIPTION=";
			if(sdu!=-1)
				url=url+"(SDU="+sdu+")";
					
			url=url+"(ADDRESS=(PROTOCOL=tcp)(PORT="+ sPort+ ")(HOST="+ sName+ "))(CONNECT_DATA=";
		if(service)
			url=url+"(SERVICE_NAME="+ dbName+")";
		else
			url=url+"(SID="+dbName+")";
		if(drcp)
			url=url+"(SERVER=POOLED)";
		url=url+"))";
		
		System.out.println("URL:"+url);
		if(pool)
			ds=createPool( url,  uName,  password,  minPoolSize, maxPoolSize, initialPoolSize);
		else
			ods=create( url,  uName,  password);

		Properties jdbcProperties = new Properties();
		jdbcProperties.put("v$session.program", "IDB_PRIVATE");
		if(pool)
			ds.setConnectionProperties(jdbcProperties);
		else
			ods.setConnectionProperties(jdbcProperties);
		String filename = fpath;
		FileInputStream fstream = null;
		DataInputStream in = null;
		BufferedReader br = null;
		Mutex mutex = new Mutex();
		long tot_tim_con = 0;
		long tot_tim_con_op = 0;
		int countt=0;
		int k=0;
		long str_time = System.nanoTime();
		long new_total_conn  = 0;
		
		
		//System.out.println("**************************VERSION 11**********************************\n\n");
		if(cType.contains("thick"))
		{
			//System.out.println("****************Instant Client version 12.1.0.1.0**********************\n\n\n\n");
		}
		Runnable[] worker;
		worker=new Runnable[tConn];
		for ( i = 0;  i < tConn && i< concr ; i++)
		{
			new_total_conn = tConn;
			if(pool)
				worker[i] = new my(new_total_conn, qpconn, ds, query_value, filename, pool, repeat , args, logger, mutex, br, in, mode%2/*k*/);
			else 
				worker[i] = new my(new_total_conn, qpconn, ods, query_value, filename, pool, repeat , args, logger, mutex, br, in, mode%2/*k*/);
					/*
					 * To start thread execution as soon as thread are created
					 */
			if((mode==0) || (mode==1)) 
			{
				executor.execute(worker[i]);
				conc.increment();
			}
			countt=countt+1;
			if(i==concr)
			{
				
			}
			//Thread.sleep(500);
		}
		
		/*
		 *  To start thread execution after all the threads are created      
		 */
		
		if((mode==2)||(mode==3)) 
		{
			for ( i = 0; i < tConn && i<concr  ; i++)
			{
				executor.execute(worker[i]);
				conc.increment();
			}   
		}
			
			
		
			//System.out.println(" Total thread count " + countt);
		long ed_time = System.nanoTime();
		tot_tim_con_op = ed_time - str_time;
		tot_op_time.set_value(tot_tim_con_op);
		System.out.println(" Time to open Threads = " + TimeUnit.MILLISECONDS.convert(tot_tim_con_op, TimeUnit.NANOSECONDS) + " milliseconds ");
		executor.shutdown();
		//System.out.println("CP\tTP/sec\tTTC\tQPS\tTQP\tError");
		int colcount=my.close_count.value();
		int total_close_conn_cout = my.total_close_count.value();
		int query_coun=my.query_count.value();
		int ecount=my.err_count.value();  
		int close_qury=0;
		int last_cnt = 0;
		int last_read_count=0;
		int last_write_count=0;
		int read_count=0;
		int write_count=0;
		str_time = System.nanoTime();
		long str_time1 = System.nanoTime();
		start_time.set_value(str_time1);
		System.out.println("ConnectionsProcessed  \t ThreadsProcessed/sec \t TotalThreadsCompleted \t QPS \t TotalQP \t ErrorCount \t ReadQPS \t WriteQPS");
		while (!executor.isTerminated() && statusPerSec) 
		{
			if ((ed_time = System.nanoTime() - str_time) > 1000000000)
			{
				int avlConnCount = 0;//ds.getAvailableConnectionsCount();
				int brwConnCount = 0;//ds.getBorrowedConnectionsCount();
			//	System.out.println("ConnectionsProcessed  \t ThreadsProcessed/sec \t TotalThreadsCompleted \t QPS \t TotalQP \t ErrorCount");
				total_close_conn_cout = my.total_close_count.value();
				colcount=my.close_count.value();
				read_count=my.read_query_count.value();
				write_count=my.write_query_count.value();
				System.out.println(" Threads completed in last second = " + (colcount - last_cnt));
				query_coun=my.query_count.value();
				ecount=my.err_count.value();
			
				System.out.println( + (total_close_conn_cout)  + "\t"  + (colcount - last_cnt) + "\t" + colcount + "\t" + (query_coun - close_qury) + "\t" + query_coun + "\t" + ( ecount ) +"\t" + (read_count-last_read_count) + "\t"+(write_count-last_write_count));
		
				str_time= System.nanoTime();
				last_cnt = colcount;
				last_read_count=read_count;
				last_write_count=write_count;
				close_qury=query_coun;		
			}
		}
		int avlConnCount = 0;//ds.getAvailableConnectionsCount();
		int brwConnCount = 0;//ds.getBorrowedConnectionsCount();
		total_close_conn_cout = my.total_close_count.value();
		colcount=my.close_count.value();
		query_coun=my.query_count.value();
		ecount=my.err_count.value();
		query_coun=my.query_count.value();
		long ed_time1 = System.nanoTime();
		tot_tim_con = ed_time1 - str_time1;
		//Thread.sleep(1000);
		total_close_conn_cout = my.total_close_count.value();
		colcount=my.close_count.value();
		query_coun=my.query_count.value();
		ecount=my.err_count.value();
		query_coun=my.query_count.value();
	}
	
}
