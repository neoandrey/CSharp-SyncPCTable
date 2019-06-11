using System; 
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Linq;
using System.Data.OleDb;
using System.Configuration;
using System.Threading;
using System.Runtime.InteropServices;
using System.Data;
using System.Globalization;
using System.Net.Mail;
using System.Data.SQLite;
using System.Data.DataSetExtensions;
using System.Threading.Tasks;

namespace SyncPCTables
{
    public class SyncPCTablesProcess
{

    internal static  ArrayList destinationTableList= new ArrayList();
     internal static HashSet<string> syncedTableList= new HashSet<string>();
    internal static Thread[] syncThreads;
     internal static string   liteConnectionString  = "";
     internal static ArrayList  pcTableList         = new  ArrayList();

      public SyncPCTablesProcess(string  config ){
                
		new  SyncPCTablesLibrary(config);
		string  nuConfig   = config.Contains("\\\\")? config:config.Replace("\\", "\\\\");

		if(File.Exists(nuConfig)){ 
			
			 
             if (SyncPCTablesLibrary.pcSyncType == SyncPCTablesLibrary.USE_PC_CARD_TYPE){

                 string  tableListScript    =  File.ReadAllText(SyncPCTablesLibrary.pcTableFetchScript);
                 tableListScript            =  tableListScript.Replace("PC_TABLE_NAME",SyncPCTablesLibrary.pcTableType );
                 DataTable   tempTab        =   SyncPCTablesLibrary.getDataFromSQL(tableListScript, SyncPCTablesLibrary.sourceConnectionProps.getConnectionString());
                 foreach (DataRow row in tempTab.Rows) {
           
                    foreach (DataColumn column in tempTab.Columns){

                      destinationTableList.Add(row[column].ToString());

                    }

                 }

             } else if (SyncPCTablesLibrary.pcSyncType == SyncPCTablesLibrary.USE_PC_CARD_LIST) {

                    destinationTableList  = SyncPCTablesLibrary.pcTableSyncList;

             }
           
		    Console.WriteLine("Starting synchronization of the following  tables:");
			SyncPCTablesLibrary.writeToLog("Starting synchronization of the following  tables:");
			int k =0;
			foreach(string tableName in destinationTableList ){
				++k;
				Console.WriteLine(k.ToString()+"."+tableName);
				SyncPCTablesLibrary.writeToLog(k.ToString()+". "+tableName);
			}
		   
            int threads   = destinationTableList.Count;

            syncThreads = new Thread[threads];
            int i =0;
            foreach  (string tableName in destinationTableList){

            if(!syncedTableList.Contains(tableName)){	
              
                syncThreads[i]  = 	new Thread(() => synchTables(tableName));
                syncedTableList.Add(tableName);	
				  ++i;					
            }

            
            }

            runSync();


                 } else{
		     Console.WriteLine("The specified configuration file: "+nuConfig+" does not exist. Please review configuration file parameter( -c ).");
								
		  }

        }

       public void synchTables(string tableName){	    
			 Console.WriteLine("Synchronizing table: "+tableName);
			 try{
			new TableSynchronizer(SyncPCTablesLibrary.sourceServer,SyncPCTablesLibrary.sourceDatabase,tableName,SyncPCTablesLibrary.destinationServer,SyncPCTablesLibrary.destinationDatabase,tableName); 
       }catch(Exception e){
		    SyncPCTablesLibrary.writeToLog(e.ToString());
			Console.WriteLine(e.ToString());

	   }

	   }
   
      public void synchTables(string tableName, string tabDiffCmd){
		 lock(this){
	       Console.WriteLine("Synchronizing table: "+tableName+". With command: "+tabDiffCmd);
		   try{
	       new TableSynchronizer(SyncPCTablesLibrary.sourceServer,SyncPCTablesLibrary.sourceDatabase,tableName,SyncPCTablesLibrary.destinationServer,SyncPCTablesLibrary.destinationDatabase,tabDiffCmd );
		  }catch(Exception e){
		    SyncPCTablesLibrary.writeToLog(e.ToString());
			Console.WriteLine(e.ToString());

	   }
	   }
	      
        }
        public SyncPCTablesProcess(){
           
             if (SyncPCTablesLibrary.pcSyncType == SyncPCTablesLibrary.USE_PC_CARD_TYPE){

                 string  tableListScript    =  File.ReadAllText(SyncPCTablesLibrary.pcTableFetchScript);
                 tableListScript            =  tableListScript.Replace("PC_TABLE_NAME",SyncPCTablesLibrary.pcTableType );
                 DataTable   tempTab        =   SyncPCTablesLibrary.getDataFromSQL(tableListScript, SyncPCTablesLibrary.sourceConnectionProps.getConnectionString());
                 foreach (DataRow row in tempTab.Rows) {
           
                    foreach (DataColumn column in tempTab.Columns){

                      destinationTableList.Add(row[column].ToString());

                    }

                 }

             } else if (SyncPCTablesLibrary.pcSyncType == SyncPCTablesLibrary.USE_PC_CARD_LIST) {

                    destinationTableList  = SyncPCTablesLibrary.pcTableSyncList;

             }
           
		    Console.WriteLine("Starting synchronization of the following  tables:");
			SyncPCTablesLibrary.writeToLog("Starting synchronization of the following  tables:");
			int k =0;
			foreach(string tableName in destinationTableList ){
				++k;
				Console.WriteLine(k.ToString()+"."+tableName);
				SyncPCTablesLibrary.writeToLog(k.ToString()+"."+tableName);


			}
		   
            int threads   = SyncPCTablesLibrary.concurrentThreads;
            syncThreads = new Thread[threads];
            int i =0;
            foreach  (string tableName in destinationTableList){		

            if(!syncedTableList.Contains(tableName)){	
                  ++i;
                syncThreads[i]  = 	new Thread(() => synchTables(tableName));
                syncedTableList.Add(tableName);		
				Console.WriteLine("initialing thread "+i.ToString()+" for "+tableName);				
            }

            
            }

            runSync();



        }
 public static void runSync(){

				   int activeThreadCount               =  syncThreads.Count();
				   HashSet<Thread> startedThreadSet    =  new  HashSet<Thread>();
				   HashSet<Thread> completedThreadSet  =  new  HashSet<Thread>();
	

				   while( completedThreadSet.Count < destinationTableList.Count){
                        double  waitTime  = double.Parse(SyncPCTablesLibrary.WAIT_INTERVAL.ToString())/1000.0;

                        
						activeThreadCount = 0;
						
						foreach(Thread pcThread  in syncThreads){
							
								if(pcThread.IsAlive){
									
										++activeThreadCount; 
								}else{
									if (startedThreadSet.Contains(pcThread)){
										
										completedThreadSet.Add(pcThread);
									}else{
										 
										 pcThread.Start();
                                         startedThreadSet.Add(pcThread);
										
									}
									

								}
								            if (activeThreadCount >= SyncPCTablesLibrary.concurrentThreads)
            {
                  

                Console.WriteLine("Current completed thread count: " + completedThreadSet.Count.ToString());
                Console.WriteLine("Current running count: " + syncThreads.Count().ToString());
                SyncPCTablesLibrary.writeToLog("Current completed thread count: " + completedThreadSet.Count.ToString());
                SyncPCTablesLibrary.writeToLog("Current running thread count: " + syncThreads.Count().ToString());
				Console.WriteLine("Waiting for  " + waitTime.ToString()+" seconds");
                SyncPCTablesLibrary.writeToLog( "Waiting for  " + waitTime.ToString()+" seconds");       
                Thread.Sleep(SyncPCTablesLibrary.WAIT_INTERVAL);  
				

            }

						}

                    

				


               // Console.WriteLine("Current running count: " + syncThreads.Count.ToString());


			  }
			 SyncPCTablesLibrary.closeLogFile();
 }
    

     public static void Main(string[] args){

	string configFile 		= ""; 
	try {	
	for(int i =0; i< args.Length; i++){

	if (args[0].ToLower()=="-h" ||args[0].ToLower()=="help" || args[0].ToLower()=="/?" || args[0].ToLower()=="?" ){

	Console.WriteLine(" This application synchronizes pc tables  of postcard databases");
	Console.WriteLine(" Usage: ");	
	Console.WriteLine(" -c: This parameter is used to specify the configuration file to be used.");
	Console.WriteLine(" -h: This parameter is used to print this help message.");	

	} else if  ((i+1)< args.Length ) {
	if(args[i].ToLower()=="-c" && (args[(i+1)] != null && args[(i+1)].Length!=0)){
	configFile =  args[(i+1)];	
	}
	}
	}	
	if(string.IsNullOrEmpty(configFile)){

	new  SyncPCTablesProcess();

	}else {					
	new  SyncPCTablesProcess(configFile);
	}

	}catch(Exception e){


	Console.WriteLine(e.Message);
	Console.WriteLine(e.StackTrace);

	}

	}
     




        
        
    }
}