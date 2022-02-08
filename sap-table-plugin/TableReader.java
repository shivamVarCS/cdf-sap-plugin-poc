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
//import com.sap.conn.jco.JCoFunctionTemplate;
//import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;

import com.sap.conn.jco.ext.DataProviderException;
import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;

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

public class TableReader
{
    static String ABAP_AS = "ABAP_AS_WITHOUT_POOL";
    static String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    static String ABAP_MS = "ABAP_MS_WITHOUT_POOL";
    
    static String queryTable="ACDOCA";
    static String rfm="/SAPDS/RFC_READ_TABLE2";
    static String destName = "default";
    
    static String timeString;
    
    static int packageSize=10000;
    static String host; 
   
    static class MyDestinationDataProvider implements DestinationDataProvider
    {
        private DestinationDataEventListener eL;
        //private HashMap<String, Properties> secureDBStorage = new HashMap<String, Properties>();
        
        public Properties getDestinationProperties(String destinationName)
        {
            try
            {
                if (destinationName=="default") return getDestinationProperties_default();
                
                return null;
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
    	String s = connectProperties.getProperty("queryTable"); 
    	if (s!=null) queryTable=s;
    	
    	s = connectProperties.getProperty("packageSize"); 
    	if (s!=null) packageSize=Integer.parseInt(s);
    	
    	
    	s = connectProperties.getProperty("RFM");
    	if (s!=null) rfm=s;
    	
    	host = connectProperties.getProperty(DestinationDataProvider.JCO_ASHOST);

    } catch (IOException ex) {
        ex.printStackTrace();
    }    
    return connectProperties;
    }
    
    //writes config.properties with jco init params
    static void writePropertiesFile()
    {
        Properties connectProperties = new Properties();
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG, "en");
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, "100");
        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, "00");
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, "Welcome1");
        connectProperties.setProperty(DestinationDataProvider.JCO_USER, "bpinst");
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, "35.233.86.135");
        
        try {
        OutputStream output = new FileOutputStream("./config.properties");
        connectProperties.store(output, null);
        } catch (IOException io) {
            io.printStackTrace();
        };
    }   
 
    static void displayElapsedTime(long startTime, int rowCount) {
    	double byteSize = rowCount*common.getInstance().rowSize;
    	String size;
    	if (byteSize<1e+6) size=String.format ("%.2f", byteSize/1e+3) + " KB";
    	else
    	if (byteSize<1e+9) size=String.format ("%.2f", byteSize/1e+6) + " MB";
    	else
        size=String.format ("%.2f", byteSize/1e+9) + " GB";
    		
    	System.out.println("************************");
    	System.out.printf("Row size: %d \nOutput size: " + size + "\n", common.getInstance().rowSize);
    	
	    //truncate to seconds
	    long elapsedTime = 1_000_000_000 * (long) ((System.nanoTime()-startTime)/1_000_000_000);
	    long elapsedTimeSec = elapsedTime / 1_000_000_000;
	    LocalTime time = LocalTime.MIN; 
	    System.out.println("Elapsed time (hh:mm:ss) " + time.plusNanos(elapsedTime).toString());
	    System.out.printf("Throughput: %d Rows/sec \n", (int) rowCount/elapsedTimeSec);
    }

    //call with TableReader rowCount rowSkip (default 1000 0)
    //multithread TableReader M rowCount
    public static void main(String[] args) throws JCoException, InterruptedException
    {
    	JCoDestination dest;
    	String rowSkip="0", rowCount="1000", par1;
    	
    	long startTime = System.nanoTime();
    	
    	if (args.length==0) {
    		System.out.println("Usage:");
    		System.out.println("Multi-Thread (with packages): TableReader M <rowCount> (default 1000)");
    		System.out.println("Single-Thread (with packages): TableReader S <rowCount> <rowSkip> (default 0)");
    		System.out.println("Single-Thread Single-Package: TableReader <rowCount> ");   		
    		System.out.println("\nNote: define connection in config.properties");
    		return;
    	}
    	if (args.length>=1) rowCount=args[0];
    	if (args.length>=2) rowSkip=args[1];
    	par1=rowCount;
    	
        LocalTime localTime = LocalTime.now().truncatedTo(ChronoUnit.SECONDS);
        timeString = localTime.toString();
        
        MyDestinationDataProvider myProvider = new MyDestinationDataProvider();
        
        //register the provider with the JCo environment;
        //catch IllegalStateException if an instance is already registered
        try
        {
            com.sap.conn.jco.ext.Environment.registerDestinationDataProvider(myProvider);
        }
        catch(IllegalStateException providerAlreadyRegisteredException)
        {
            //somebody else registered its implementation, 
            //stop the execution
            throw new Error(providerAlreadyRegisteredException);
        }
        
        //ping destination
        try
        {
            dest = JCoDestinationManager.getDestination(destName);
            
            if (par1.charAt(0)=='M')
            	System.out.printf("\n***** MULTI-THREAD MULTI-PACKAGE TABLE=%s ROWCOUNT=%s PACKAGE=%d RFM=%s HOST=%s *****\n\n",queryTable, rowSkip, packageSize, rfm, host);
            else
            	if (par1.charAt(0)=='S') {
                	rowCount=rowSkip;
                	if (args.length>=3) rowSkip=args[2]; else rowSkip="0";
            		System.out.printf("\n***** SINGLE-THREAD MULTI-PACKAGE TABLE=%s ROWCOUNT=%s ROWSKIP=%s PACKAGE=%d RFM=%s HOST=%s *****\n\n",queryTable,rowCount, rowSkip, packageSize, rfm, host);
            	}
            	else
            		System.out.printf("\n***** SINGLE-THREAD SINGLE-PACKAGE TABLE=%s ROWCOUNT=%s ROWSKIPS=%s RFM=%s HOST=%s *****\n\n",queryTable,rowCount,rowSkip, rfm, host);
            
            System.out.println("START time: " + timeString);
            System.out.print("Ping Destination " + destName + "...");
            //System.out.printf("RFM=%s queryTable=%s rowCount=%s rowSkips=%s\n",rfm,queryTable,rowCount,rowSkip);         
            dest.ping();
            System.out.println(" works!");
        }
        catch(JCoException e)
        {
            e.printStackTrace();
            System.out.println("Execution on destination " + destName+ " failed");
            return;
        }   
    	
        //MULTI THREAD, MULTI PACKAGE
        if (par1.charAt(0)=='M')
        {
        	rowCount=rowSkip;
        	int rowChunk = Integer.parseInt(rowCount)/4;
        	//WorkerThread( String TimeString, JCoDestination DEST, String QueryTable, String RFM, int ID, int ROWCOUNT, int ROWSKIPS)
        	WorkerThread wt1 = new WorkerThread( timeString, dest, queryTable, rfm, 1, rowChunk, 0, packageSize);
	    	WorkerThread wt2 = new WorkerThread( timeString, dest, queryTable, rfm, 2, rowChunk, rowChunk, packageSize);
	    	WorkerThread wt3 = new WorkerThread( timeString, dest, queryTable, rfm, 3, rowChunk, 2*rowChunk, packageSize);
	    	WorkerThread wt4 = new WorkerThread( timeString, dest, queryTable, rfm, 4, rowChunk, 3*rowChunk, packageSize);
	    	Thread t1 = new Thread ( wt1 ); Thread t2 = new Thread ( wt2 );
	    	Thread t3 = new Thread ( wt3 ); Thread t4 = new Thread ( wt4 );
	        t1.start(); TimeUnit.SECONDS.sleep(1); t2.start(); TimeUnit.SECONDS.sleep(1);
	        t3.start(); TimeUnit.SECONDS.sleep(1); t4.start(); TimeUnit.SECONDS.sleep(1);
	        t1.join(); t2.join(); t3.join(); t4.join();
	        displayElapsedTime(startTime,Integer.parseInt(rowCount)); 
	        return;
        };
        
        //SINGLE THREAD, MULTI PACKAGE
        if (par1.charAt(0)=='S') {
        	int skip = Integer.parseInt(rowSkip);
        	int rowChunk = Integer.parseInt(rowCount);
        	WorkerThread wt1 = new WorkerThread( timeString, dest, queryTable, rfm, 1, rowChunk, skip, packageSize);
        	Thread t1 = new Thread ( wt1 );
        	t1.start(); t1.join();     
	        displayElapsedTime(startTime,rowChunk); 
	        return;        	
        };
        		
        //SINGLE THREAD, SINGLE PACKAGE
        JCoFunction function = dest.getRepository().getFunction(rfm);
        if(function == null)
            throw new RuntimeException(rfm + " not found in SAP.");

        function.getImportParameterList().setValue("QUERY_TABLE", queryTable);
        function.getImportParameterList().setValue("ROWCOUNT", rowCount);
        function.getImportParameterList().setValue("ROWSKIPS", rowSkip);

        try
        {
        	System.out.printf("Calling %s %s %s %s ...",rfm,queryTable,rowCount,rowSkip);
            function.execute(dest);
            System.out.println("done");
        }
        catch(AbapException e)
        {
            System.out.println(e.toString());
            return;
        }
        
        //create file
        String filename= "outTableReader_" + queryTable+ "_" + timeString + ".txt";
        String ts="";
        try {
            File myObj = new File(filename);
            if (!myObj.createNewFile())
              System.out.println("File already exists.");
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }        
        
        try {
            FileWriter myWriter = new FileWriter(filename, true);
            
            JCoTable t = function.getTableParameterList().getTable("TBLOUT8192");
            for (int i = 0; i < t.getNumRows(); i++)
            {
            	t.setRow(i);
            	myWriter.write("ROW "+i+">");
            	ts=t.getValue("WA").toString();
            	myWriter.write(ts);
            	myWriter.write("\n");
            } 
            common.getInstance().rowSize = ts.length(); //get row size
            myWriter.close();
            System.out.println("Completed "+filename);
            displayElapsedTime(startTime,Integer.parseInt(rowCount));           
          } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }       

        /*
        JCoStructure detail = function.getExportParameterList().getStructure("COMPANYCODE_DETAIL");
        
            
        System.out.println(detail.getString("COMP_CODE") + '\t' +
                               detail.getString("COUNTRY") + '\t' +
                               detail.getString("CITY"));       
                        */ 
       
    }
}
