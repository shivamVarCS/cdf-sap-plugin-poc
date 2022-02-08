import java.util.concurrent.CountDownLatch;
import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.time.LocalTime;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;


import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoContext;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
//import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoStructure;
//import com.sap.conn.jco.JCoFunctionTemplate;
//import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;

import com.sap.conn.jco.ext.DataProviderException;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

//import org.apache.log4j.Logger;

import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.ext.ServerDataProvider;
import com.sap.conn.jco.server.DefaultServerHandlerFactory;
import com.sap.conn.jco.server.JCoServer;
import com.sap.conn.jco.server.JCoServerContextInfo;
import com.sap.conn.jco.server.JCoServerErrorListener;
import com.sap.conn.jco.server.JCoServerExceptionListener;
import com.sap.conn.jco.server.JCoServerFactory;
import com.sap.conn.jco.server.JCoServerFunctionHandler;
import com.sap.conn.jco.server.JCoServerState;
import com.sap.conn.jco.server.JCoServerStateChangedListener;

import com.sap.conn.jco.ext.Environment;
import com.sap.conn.jco.ext.ServerDataEventListener;
import com.sap.conn.jco.ext.ServerDataProvider;

import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.server.JCoServerContext;
import com.sap.conn.jco.server.JCoServerFunctionHandler;
 


 
public class BWReader implements JCoServerErrorListener, JCoServerExceptionListener, JCoServerStateChangedListener {
    
	/**
	 * The properties necessary to define the server and destination.
	 */
    private static Properties properties;
    public static JCoDestination dest;
    
    static class parms {
        public static String ohdest;
        public static String dtp;
        public static String rid;
        public static String pc;
        public static String dbt;
        public static String file;
        public static Integer np;
        public static Integer recs;
    }
    
    static class AbapCallHandler implements JCoServerFunctionHandler {
    	
    	/**
    	 * This handler only supports one function with name {@code Z_SAMPLE_ABAP_CONNECTOR_CALL}.
    	 */
    	public static final String FUNCTION_NAME = "RSB_API_OHS_ETL_NOTIFY";
    	
    	private void printRequestInformation(JCoServerContext serverCtx, JCoFunction function) {
    		System.out.println("----------------------------------------------------------------");
            System.out.println("call              : " + function.getName());
            System.out.println("ConnectionId      : " + serverCtx.getConnectionID());
            System.out.println("SessionId         : " + serverCtx.getSessionID());
            System.out.println("TID               : " + serverCtx.getTID());
            System.out.println("repository name   : " + serverCtx.getRepository().getName());
            System.out.println("is in transaction : " + serverCtx.isInTransaction());
            System.out.println("is stateful       : " + serverCtx.isStatefulSession());
            System.out.println("----------------------------------------------------------------");
            System.out.println("gwhost: " + serverCtx.getServer().getGatewayHost());
            System.out.println("gwserv: " + serverCtx.getServer().getGatewayService());
            System.out.println("progid: " + serverCtx.getServer().getProgramID());
            System.out.println("----------------------------------------------------------------");
            //System.out.println("attributes  : ");
            //System.out.println(serverCtx.getConnectionAttributes().toString());
            //System.out.println("----------------------------------------------------------------");
    	}

    	public void handleRequest(JCoServerContext serverCtx, JCoFunction function) {
    		// Check if the called function is the supported one.
    		if(!function.getName().equals(FUNCTION_NAME)) {
    			System.out.println("Function '"+function.getName()+"' is no supported to be handled!");
    			return;
    		}
            printRequestInformation(serverCtx, function);
            
            /*Local Interface:
            	*"  IMPORTING
            	*"     VALUE(OHDEST) TYPE  RSOHDEST
            	*"     VALUE(OHDEST_TXT) TYPE  RSTXTLG
            	*"     VALUE(DTP) TYPE  RSBKDTPNM
            	*"     VALUE(REQUESTID) TYPE  RSBK_REQUID
            	*"     VALUE(PROCESS_CHAIN) TYPE  RSPC_CHAIN
            	*"     VALUE(PC_LOGID) TYPE  RSPC_LOGID
            	*"     VALUE(DBTABNAME) TYPE  RSBTABNAME
            	*"     VALUE(FILENAME) TYPE  RSBFILENAME
            	*"     VALUE(NUMB_OF_PACKETS) TYPE  I
            	*"     VALUE(RECORDS) TYPE  I
            	*"     VALUE(TIMESTAMP) TYPE  TIMESTAMP
            	*"  EXPORTING
            	*"     VALUE(RETURN) TYPE  BAPIRET2
            	*"  EXCEPTIONS
            	*"      COMMUNICATION_FAILURE
            	*"      SYSTEM_FAILURE*/
          
             parms.ohdest = function.getImportParameterList().getString("OHDEST");
             parms.dtp = function.getImportParameterList().getString("DTP");
             parms.rid = function.getImportParameterList().getString("REQUESTID");
             parms.pc = function.getImportParameterList().getString("PROCESS_CHAIN");
             parms.dbt = function.getImportParameterList().getString("DBTABNAME");
             parms.file = function.getImportParameterList().getString("FILENAME");
             parms.np = function.getImportParameterList().getInt("NUMB_OF_PACKETS");
             parms.recs = function.getImportParameterList().getInt("RECORDS");
            
            System.out.println("OHDEST "+parms.ohdest);
            System.out.println("REQUESTID "+parms.rid);
            System.out.println("DBTABNAME "+parms.dbt);
            System.out.println("FILENAME "+parms.file);
            System.out.println("NUMB_OF_PACKETS "+parms.np);
            System.out.println("RECORDS "+parms.recs);
            System.out.println("----------------------------------------------------------------");
            
        
        }
    }
    
    static class MyServerDataProvider implements ServerDataProvider {

    	/**
    	 * Initializes this instance with the given {@code properties}.
    	 * Performs a self-registration in case no instance of a
    	 * {@link MyServerDataProvider} is registered so far
    	 * (see {@link #register(MyServerDataProvider)}).
    	 * 
    	 * @param properties
    	 *            the {@link #properties}
    	 * 
    	 */
    	public MyServerDataProvider() {
    		super();
    		// Try to register this instance (in case there is not already another
    		// instance registered).
    		register(this);
    	}
    	
    	/**
    	 * Flag that indicates if the method was already called.
    	 */
    	private static boolean registered = false;

    	/**
    	 * Registers the given {@code provider} as server data provider at the
    	 * {@link Environment}.
    	 * 
    	 * @param provider
    	 *            the server data provider to register
    	 */
    	private static void register(MyServerDataProvider provider) {
    		// Check if a registration has already been performed.
    		if (registered == false) {
    			System.out.println("There is no " + MyServerDataProvider.class.getSimpleName()
    					+ " registered so far. Registering a new instance.");
    			// Register the destination data provider.
    			Environment.registerServerDataProvider(provider);
    			registered = true;
    		}
    	}
    	
    	@Override
    	public Properties getServerProperties(String serverName) {
    		System.out.println("Providing server properties for server '"+serverName+"' using the specified properties");
    		return getDestinationProperties_default();
    	}

    	@Override
    	public void setServerDataEventListener(ServerDataEventListener listener) {
    	}

    	@Override
    	public boolean supportsEvents() {
    		return false;
    	}
    }    
    
    static class MyDestinationDataProvider implements DestinationDataProvider
    {
        private DestinationDataEventListener eL;
        //private HashMap<String, Properties> secureDBStorage = new HashMap<String, Properties>();
        
        public Properties getDestinationProperties(String destinationName)
        {
            try
            {
            	System.out.println("getting destination");
            	return getDestinationProperties_default();
                //if (destinationName=="default") return getDestinationProperties_default();
                
                //return null;
            }
            catch(RuntimeException re)
            {
                throw new DataProviderException(DataProviderException.Reason.INTERNAL_ERROR, re);
            }
        }

        //An implementation supporting events has to retain the eventListener instance provided
        //by the JCo runtime. This listener instance shall be used to notify the JCo runtime
        //about all changes in destination configurations.
        public void setDestinationDataEventListener(DestinationDataEventListener eventListener)
        {
            this.eL = eventListener;
        }

        public boolean supportsEvents()
        {
            return true;
        }

        //implementation that saves the properties in a very secure way
        void changeProperties(String destName, Properties properties)
        {
        	
                if(properties==null)
                        eL.deleted(destName);
                else 
                    eL.updated(destName); // create or updated
        }
    } // end of MyDestinationDataProvider
    
    //business logic
    void executeCalls(String destName)
    {
        JCoDestination dest;
        try
        {
            dest = JCoDestinationManager.getDestination(destName);
            dest.ping();
            System.out.println("Destination " + destName + " works");
        }
        catch(JCoException e)
        {
            e.printStackTrace();
            System.out.println("Execution on destination " + destName+ " failed");
        }
    }
    
    static Properties getDestinationProperties_default()
    {
    Properties connectProperties = new Properties();
    /*
    connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, "35.233.86.135");
    connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, "00");
    connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, "100");
    connectProperties.setProperty(DestinationDataProvider.JCO_USER, "bpinst");
    connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, "Welcome1");
    connectProperties.setProperty(DestinationDataProvider.JCO_LANG, "en");
    */
    //load from config.properties
    try {

    	InputStream input = new FileInputStream("./config.properties");

        // load a properties file
    	connectProperties.load(input);
    	
    	//host = connectProperties.getProperty(DestinationDataProvider.JCO_ASHOST);

    } catch (IOException ex) {
        ex.printStackTrace();
    }   
    return connectProperties;
    }
  
    
    public BWReader(String propertiesPath) throws IOException {
    	/*InputStream propertiesInputStream = new FileInputStream(propertiesPath);
    	properties = new Properties();
    	properties.load(propertiesInputStream);*/
    	
    	properties = getDestinationProperties_default();
		new MyDestinationDataProvider();
		new MyServerDataProvider();
	}
    
    /**
     * Runnable to listen to the standard input stream to end the server.
     */
    private Runnable stdInListener = new Runnable() {
		
		@Override
		public void run() {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String line = null;
			try {
				while((line = br.readLine()) != null) {
					// Check if the server should be ended.
					if(line.equalsIgnoreCase("end")) {
						// Stop the server.
						server.stop();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	};
	
	private static JCoServer server;
   
    public static void serve() {
    	
        try {
            server = JCoServerFactory.getServer(properties.getProperty(ServerDataProvider.JCO_PROGID));
        } catch(JCoException e) {
            throw new RuntimeException("Unable to create the server " + properties.getProperty(ServerDataProvider.JCO_PROGID) + ", because of " + e.getMessage(), e);
        }
       
        JCoServerFunctionHandler abapCallHandler = new AbapCallHandler();
        DefaultServerHandlerFactory.FunctionHandlerFactory factory = new DefaultServerHandlerFactory.FunctionHandlerFactory();
        factory.registerHandler(AbapCallHandler.FUNCTION_NAME, abapCallHandler);
        server.setCallHandlerFactory(factory);
       
        // Add listener for errors.
        //server.addServerErrorListener(this);
        // Add listener for exceptions.
        //server.addServerExceptionListener(this);
        // Add server state change listener.
        //server.addServerStateChangedListener(this);
        
        // Add a stdIn listener.
        //new Thread(stdInListener).start();
        
        // Start the server
        
        server.start();
        System.out.println("The program can be stopped typing 'END'"); 
        
    }

	@Override
	public void serverExceptionOccurred(JCoServer jcoServer, String connectionId, JCoServerContextInfo arg2, Exception exception) {
		 System.out.println("Exception occured on " + jcoServer.getProgramID() + " connection " + connectionId);
	}
	
	@Override
	public void serverErrorOccurred(JCoServer jcoServer, String connectionId, JCoServerContextInfo arg2, Error error) {
		System.out.println("Error occured on " + jcoServer.getProgramID() + " connection " + connectionId);	
	}
   
	@Override
    public void serverStateChangeOccurred(JCoServer server, JCoServerState oldState, JCoServerState newState) {
        // Defined states are: STARTED, DEAD, ALIVE, STOPPED;
        // see JCoServerState class for details. 
        // Details for connections managed by a server instance
        // are available via JCoServerMonitor
        System.out.println("Server state changed from " + oldState.toString() + " to " + newState.toString() +
                " on server with program id " + server.getProgramID());
        if(newState.equals(JCoServerState.ALIVE)) {
        	System.out.println("Server with program ID '"+server.getProgramID()+"' is running");
        }
        if(newState.equals(JCoServerState.STOPPED)) {
        	System.out.println("Exit program");
        	System.exit(0);
        }
    }

    public static void main(String[] args) throws Exception {
    	/*if(args.length == 0) {
    		System.out.println("You must specify a properties file!");
    		return;
    	}*/
    	properties = getDestinationProperties_default();
		new MyDestinationDataProvider();
		new MyServerDataProvider();
		
    	serve();
        String rfm = "RSPC_API_CHAIN_START"; 
        String PC = args[0]; //"PC_PUR";
        String destName = "ZVD";
        String logid;
        //JCoDestination dest;
        
        try
        {
            dest = JCoDestinationManager.getDestination(destName);
            dest.ping();
            System.out.println("Destination " + destName + " works");
            
            //System.out.println("\nRunning Process Chain");
            
            rfm = "RSPC_API_CHAIN_START"; 
            JCoFunction function = dest.getRepository().getFunction(rfm);
            if(function == null)
                throw new RuntimeException(rfm + " not found in SAP.");

            function.getImportParameterList().setValue("I_CHAIN", PC);
            //function.getImportParameterList().setValue("I_SYNCHRONOUS", "X");

            System.out.println("\nCalling " + rfm + " " + PC);
            function.execute(dest);
            logid = (String) function.getExportParameterList().getValue("E_LOGID");
            System.out.println("E_LOGID "+logid); 
            
            rfm = "RSPC_API_CHAIN_GET_LOG";
            function = dest.getRepository().getFunction(rfm);
            if(function == null)
                throw new RuntimeException(rfm + " not found in SAP.");

            function.getImportParameterList().setValue("I_CHAIN", PC);
            function.getImportParameterList().setValue("I_LOGID", logid);

        	System.out.println("\nCalling " + rfm + " " + PC + " " + logid);
            function.execute(dest);
            
            JCoTable t = function.getTableParameterList().getTable("E_T_LOG");
            System.out.println("E_T_LOG (MSGV1+MSGV2+MSGV3):");
            for (int i = 0; i < t.getNumRows(); i++)
            {
            	t.setRow(i);
            	System.out.println(t.getValue("MSGV1").toString()+t.getValue("MSGV2").toString()+t.getValue("MSGV3").toString());
            }      
            
            
            System.out.println("Waiting for Request ID");
            
            while (parms.rid==null) Thread.sleep(1000);
            System.out.println("Req id: "+parms.rid);
            
            	
        	// GET the DATA 
            rfm = "RSB_API_OHS_ETL_READ_DATA";
            function = dest.getRepository().getFunction(rfm);
            if(function == null)
                throw new RuntimeException(rfm + " not found in SAP.");

            function.getImportParameterList().setValue("OHDEST", parms.ohdest);
            function.getImportParameterList().setValue("REQUESTID", parms.rid);
            function.getImportParameterList().setValue("PACKETID", 1);
            function.getImportParameterList().setValue("SKIP_TECKEY", "X");
            
            int c=0;
            do 
            {
        	System.out.println("\nCalling " + rfm + " " + parms.ohdest + " " + parms.rid);
            function.execute(dest);        
            t = function.getTableParameterList().getTable("RESULTDATA");
            Thread.sleep(3000);
            }
            while (t.getNumRows()==0 && c++<60 );
            
            System.out.println("DATA:");
            for (int i = 0; i < t.getNumRows() && i<3; i++)
            {
            	t.setRow(i);
            	System.out.println(t.getValue("DATA"));
            }   
            
            System.out.println();
            //SET REQ STATUS GREEN
            rfm = "RSB_API_OHS_ETL_SETSTATUS";
            function = dest.getRepository().getFunction(rfm);
            if(function == null)
                throw new RuntimeException(rfm + " not found in SAP.");

            function.getImportParameterList().setValue("STATUS", "G");
            function.getImportParameterList().setValue("REQUESTID", parms.rid);
            function.getImportParameterList().setValue("MESSAGE", "all good!");

        	System.out.println("\nCalling " + rfm + " " + parms.rid + " " + "G");
            function.execute(dest);	            
	      
            System.out.println("Stopping RFC Server");
            server.stop();
    
     
        }
  
        catch(AbapException e)
        {
            System.out.println(e.toString());
            return;
        }  
        catch(JCoException e)
        {
            e.printStackTrace();
            return;
        }      
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
        
    }
}

//to store data across threads - eg. common.getInstance().rowSize
class common {
	public int rowSize=0;
	private static common instance = null;
    public static common getInstance() 
    { 
        if (instance == null) 
            instance = new common();  
        return instance; 
    } 
}

class WorkerThread implements Runnable {
	   private int packageSize; private int retryCount=3;
	   private Thread t;
	   private String id;
	   private int rowSKIP, rowCOUNT;
	   private JCoDestination dest;
	   private String rfm; 
	   private String queryTable; private String timeString;
	   private JCoFunction function; private String filename;
	   String ts=""; FileWriter myWriter;
	   
	   WorkerThread( String TimeString, JCoDestination DEST, String QueryTable, String RFM, int ID, int ROWCOUNT, int ROWSKIP, int PACKAGE) { //reads from table with ROWCOUNT and ROWSKIPS
		  rowSKIP=ROWSKIP; rowCOUNT=ROWCOUNT; dest=DEST; rfm=RFM; 
		  queryTable=QueryTable; timeString=TimeString; packageSize = PACKAGE;
		  id = new Integer(ID).toString();
	      System.out.printf("Thread %s rowCOUNT=%d rowSKIP=%d \n", id, rowCOUNT, rowSKIP );
	      //System.out.printf("queryTable=%s rfm=%s timeString=%s \n", queryTable, rfm, timeString );
	      filename= "outTableReader_T" + id + "_" + queryTable+ "_" + timeString + ".txt";
	   }
	   
	   public void run() {
	      //System.out.println("Running " +  id );
		  // if (true) return;
	      try {
	    	  myWriter = new FileWriter(filename, true); 
	          //prepare function
	          function = dest.getRepository().getFunction(rfm);
	          if(function == null)
	              throw new RuntimeException(rfm + " not found in SAP.");
	          //divide rowCount into package size, read all packages 
	          int count = rowCOUNT/packageSize ;
	          for(int p=0;p<=count;p++)
	        	  for(int i=0;i<retryCount;i++) //retry if -1 (error) returned
	        		  if(readPackage(p)!=-1)i=retryCount;
	          myWriter.close();
	          common.getInstance().rowSize = ts.length(); //get row size

	      } /*catch (InterruptedException e) {
	         System.out.println("Thread " +  id + " interrupted.");
	      }*/
	      catch (JCoException e) {
		         System.out.println("Thread " +  id + " JCO Exception:");
		         System.out.println(e.toString());
		         System.out.println(e.getMessageText());
		      }
	      catch (IOException e) {
              System.out.println("An error occurred.");
              e.printStackTrace();
            }; 
	      System.out.println("Thread " +  id + " exiting.");
	   }
	   
	   public void start () {
	      System.out.println("Starting " +  id );
	      if (t == null) {
	         t = new Thread (this, id );
	         t.start ();
	      }
	   }
	   
	   //reads the corresponding package (100k records) starting at rowSkips
	   //caller has to make sure the package exists; returns -1 for errors (caller can retry)
	   int readPackage(int p) { 
		   //System.out.printf("Thread id=%s PACKAGE=%d SKIP=%d \n", id, p, rowSKIP+p*packageSize );
		   String rowCount, rowSkip; int rows, skip, rowno;
		   		
		  try {		  
			  skip = rowSKIP+p*packageSize;
			  rows = Math.min(packageSize, rowCOUNT + rowSKIP - skip);
			  if (rows<=0) return 0;
			  rowCount = new Integer(rows).toString();
			  rowSkip = new Integer(skip).toString();
			  JCoContext.begin(dest);
	          function.getImportParameterList().setValue("QUERY_TABLE", queryTable);
	          function.getImportParameterList().setValue("ROWCOUNT", rowCount );
	          function.getImportParameterList().setValue("ROWSKIPS", rowSkip);
	          JCoTable t = function.getTableParameterList().getTable("TBLOUT8192");
	          t.deleteAllRows();

	          try
	          {
	          	System.out.printf("Thread %s - Calling %s %s %s %s \n",id, rfm,queryTable,rowCount,rowSkip);
	            function.execute(dest);
	            //System.out.println("done");
	          }
	          catch(AbapException e)
	          {
	              System.out.println(e.toString());
	              return -1;
	          }
	                    
	          try {	                       
	              for (int i = 0; i < t.getNumRows(); i++)
	              {
	              	t.setRow(i);
	              	rowno = i + skip;
	              	myWriter.write("ROW "+rowno+">");
	              	ts = t.getValue("WA").toString();
	              	myWriter.write(ts);
	            	myWriter.write("\n");
	              }         
	              myWriter.flush();
	              JCoContext.end(dest);	              
	            } catch (IOException e) {
	              System.out.println("An error occurred.");
	              e.printStackTrace();
	              return -1;
	            } 		   
		  }
	      catch (JCoException e) {
		         System.out.println("Thread " +  id + " JCO Exception:");
		         System.out.println(e.toString());
		         System.out.println(e.getMessageText());
		         return -1;
		      }
		   return 0;
	   }
	}
