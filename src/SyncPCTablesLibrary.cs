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
using System.Threading;
using System.Data;
using System.Globalization;
using System.Net.Mail;
using Newtonsoft.Json.Linq;
using System.Data.SQLite;

namespace SyncPCTables
{
    public class SyncPCTablesLibrary
    {
            public static  string       sourceServer;    
            public static  string       sourceDatabase;      
            public static  int          sourcePort; 
            public static  string       destinationServer; 
            public static  string       destinationDatabase; 
            public static  int          destinationPort; 
            public static  string       pcTableType; 
            public static  string       pcTableFetchScript;
            public static  string       pcTableSyncScriptPathPrefix; 
            public static  string       configFileName;
            public  static  ArrayList   pcTableSyncList;
             public  static  int   pcSyncType;
            public  static System.IO.StreamWriter   	fs;
            public  static string 	logFile	 = AppDomain.CurrentDomain.BaseDirectory+"..\\log\\pc_sync_session_log"+DateTime.Now.ToString("yyyyMMdd_HH_mm_ss")+".log";
            public  static ConnectionProperty           sourceConnectionProps;
            public  static ConnectionProperty           destinationConnectionProps;
            public  static Dictionary<string,string>    connectionPropsMap                   = new  Dictionary<string,string> () ;
            public static bool isSrcAccessible  = false;

            public  static bool isDstAccessible  = false;

            public  static  SyncPCTablesConfig  syncPCConfig;

             public  static SQLiteConnection liteConnect;
             public static string   liteConnectionString      = "";

             public static int      concurrentThreads      = 1;

             public const int USE_PC_CARD_TYPE  = 0;  
             public const int USE_PC_CARD_LIST =  1;   

             public static int WAIT_INTERVAL   = 5000;


            public  SyncPCTablesLibrary(){       

					initSyncPCTablesLibrary();

				}
            
			
      			public  SyncPCTablesLibrary(string  cfgFile){
					 
					   if(!string.IsNullOrEmpty(cfgFile) ){

						   string   nuCfgFile  = "";
						   Console.WriteLine("Loading configurations in  configuration file: "+cfgFile);
						   nuCfgFile           =  cfgFile.Contains("\\\\")? cfgFile:cfgFile.Replace("\\", "\\\\");
                           
                          try{
                                    if(File.Exists(nuCfgFile)){

                                        configFileName          = nuCfgFile;
                                        initSyncPCTablesLibrary();

                                    }
                            }catch(Exception e){

                                        Console.WriteLine("Error reading configuration file: "+e.Message+"\n"+e.ToString());
                                        Console.WriteLine(e.StackTrace);
                                        writeToLog("Error reading configuration file: "+e.Message+"\n"+e.ToString());
                                        writeToLog(e.StackTrace);

                            }
					   }
				 	
		       	         		
				}

              public void  initLiteConnectionString(){

                 liteConnectionString =  "Data Source="+AppDomain.CurrentDomain.BaseDirectory + "..\\db\\sync_pc_inventory.sqlite;Version=3;";

               }


               internal  void initSyncPCTablesLibrary () {

                   initLiteConnectionString();

                    Console.WriteLine("Reading configuration file: "+configFileName);
                    readConfigFile(configFileName);

					if (!File.Exists(logFile))  {

							fs = File.CreateText(logFile);

					}else{

							fs = File.AppendText(logFile);

					} 

					log("===========================Started PC Tables Synchronization Session at "+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"==============================");
					

				    if (!String.IsNullOrEmpty(sourceServer) &&  !String.IsNullOrEmpty(sourceDatabase)){ 
                        
                                
                             sourceConnectionProps      = new ConnectionProperty( sourceServer, sourceDatabase );
                             connectionPropsMap.Add(sourceServer, sourceConnectionProps.getConnectionString());

                    } else {

                        Console.WriteLine("Source connection details are not complete");
                        writeToLog("Source connection details are not complete");

                    }
                   
                   
				    if (!String.IsNullOrEmpty(destinationServer) &&  !String.IsNullOrEmpty(destinationDatabase)){ 
                                               
                             destinationConnectionProps      = new ConnectionProperty( destinationServer, destinationDatabase );
                             connectionPropsMap.Add(destinationServer, destinationConnectionProps.getConnectionString());

                    } else {

                        Console.WriteLine("destination connection details are not complete");
                        writeToLog("destination connection details are not complete");

                    }


                    if  (!(canConnect(sourceConnectionProps.getConnectionString()) && canConnect( destinationConnectionProps.getConnectionString())
                       )){
                         Console.WriteLine("Cannot reach one of the servers. Please check connection details");
                         writeToLog("Cannot reach one of the servers. Please check connection details");
                         Environment.Exit(0);

                    }

               }
               	public  static  void readConfigFile(string configFileName){
					
	
                   Console.WriteLine("Reading contents of configuration file: "+configFileName);
                   try{
					   
						string  propertyString                  = File.ReadAllText(configFileName);
						syncPCConfig                            = Newtonsoft.Json.JsonConvert.DeserializeObject<SyncPCTablesConfig>(propertyString);              				
                        sourceDatabase		 	                = syncPCConfig.source_database;
                        sourcePort		 		                = syncPCConfig.source_port;
                        sourceServer		 	                = syncPCConfig.source_server;
                        logFile			 		                = syncPCConfig.log_file.Replace("DATE_SUFFIX",DateTime.Now.ToString("yyyyMMdd_HH_mm_ss"));
                        destinationServer	                    = syncPCConfig.destination_server;
                        destinationDatabase	                    = syncPCConfig.destination_database;
                        destinationPort	                        = syncPCConfig.destination_port;
                        pcTableType                             = syncPCConfig.pc_table_type;
                        pcTableFetchScript                      = syncPCConfig.pc_tables_fetch_script;
                        pcSyncType                              = syncPCConfig.pc_sync_type;
                        pcTableSyncList                         = syncPCConfig.pc_table_list;
                        concurrentThreads                       = syncPCConfig.sync_tables_at_once;
                        WAIT_INTERVAL                           = syncPCConfig.wait_interval;

			            Console.WriteLine("Configurations have been successfully initialised.");
						
                }catch(Exception e){
                    
                    Console.WriteLine("Error reading configuration file: "+e.Message);
                    Console.WriteLine(e.StackTrace);
         
                }
				
			}
                public static void  writeToLog(string logMessage){
                    fs.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"=>"+logMessage);
                }

			 public static Dictionary<string,string>readJSONMap(ArrayList rawMap){

                    Dictionary<string, string> tempDico = new  Dictionary<string, string>();
                    string tempVal  ="";
                    if(rawMap!=null)
                    foreach(var keyVal in rawMap){
                                
                        tempVal = keyVal.ToString();
                        if(!string.IsNullOrEmpty(tempVal)){
                            tempVal = tempVal.Replace("{","").Replace("}","").Replace("\"","").Trim();
                           // Console.WriteLine("tempVal: "+tempVal);
                            if(tempVal.Split(':').Count() ==2)tempDico.Add(tempVal.Split(':')[0].Trim(),tempVal.Split(':')[1].Trim());
                            else if(tempVal.Split(':').Count() ==3  && "ABCDEFGHIJKLMNOPQRSTUVQXYZ".Contains(tempVal.Split(':')[1].Trim().ToUpper() )) {
                                tempDico.Add(tempVal.Split(':')[0].Trim(),tempVal.Split(':')[1].Trim()+":"+tempVal.Split(':')[2].Trim());
                            }  
                        }  

                    }
                  return tempDico;
            }
            public static Dictionary<string, bool>convertToBoolMap(Dictionary<string,string> rawMap){

                    Dictionary<string, bool> tempDico = new  Dictionary<string, bool>();

                    if(rawMap!=null)
                   	foreach(KeyValuePair<string, string> parameter in rawMap){
						
							tempDico.Add(parameter.Key, bool.Parse(parameter.Value));

						}
                  return tempDico;
            }

			public static  void log(string logMessage){

				fs.WriteLine(logMessage);
				fs.Flush();
				
			}

           public  static System.Data.DataTable   getDataFromServer(string script, string  serverConString  ){

                            Console.WriteLine("Executing script: ");
                            Console.WriteLine(script);
                            System.Data.DataTable dt = new DataTable();

                            try{

                                using (SqlConnection serverConnection =  new SqlConnection(serverConString)){
                                SqlCommand cmd = new SqlCommand(script, serverConnection);
                                writeToLog("Executing script: "+script+" on  database.");
                                cmd.CommandTimeout =0;
                                serverConnection.Open();
                                SqlDataReader  reader = cmd.ExecuteReader();
                                dt.Load(reader);	
                                cmd.Dispose();
                            }
                        }catch(Exception e){
                             writeToLog("Error while running script: " + e.Message+"\n"+e.ToString());
                             writeToLog(e.StackTrace);
                             Console.WriteLine("Error while running script: " + e.Message+"\n"+e.ToString());
                              Console.WriteLine(e.StackTrace);

                        }
                        return dt;
                }

             public  static void executOnServer( string connectionsStr, string sqlScript){

                                        try{
                                            using  (SqlConnection  sqlConnect = new SqlConnection(connectionsStr)){
                                                    
                                                    if(sqlConnect.State ==ConnectionState.Closed){
                                                        sqlConnect.Open();
                                                        Console.WriteLine("Connection Open");
                                                    }
                                                    Console.WriteLine("Running: "+sqlScript);
                                                    SqlCommand command = new SqlCommand(sqlScript, sqlConnect);
                                                    command.CommandTimeout = 0;
                                                    command.ExecuteNonQuery();
                                                    Console.WriteLine("Script complete");  
                                                    command.Dispose();
                                                     if(sqlConnect.State ==ConnectionState.Open)sqlConnect.Close();                                              
                                       
                                                }       
                        } catch(Exception e){
       
                          Console.WriteLine("Error running script: "+sqlScript);
                          Console.WriteLine(e.ToString()+"\n"+e.Message);
                          Console.WriteLine(e.StackTrace);
                          
                        }

                        
                    }
                 public static bool canConnect(string  sourceConnectionString){
                    bool isConnectible =false;
                    try
                        {

                       using(  SqlConnection serverConnection =  new SqlConnection(sourceConnectionString)){
                        serverConnection.Open();
                        isConnectible =true;
                        serverConnection.Close();
                       }
                        }catch (Exception e){
                        
                            Console.WriteLine("Error connecting to server: " + e.Message+"\n"+e.ToString());
                            Console.WriteLine(e.StackTrace);
                            writeToLog ("Error connecting to server: " + e.Message+"\n"+e.ToString());
                            writeToLog(e.StackTrace); 
                            }
                         
                        return isConnectible;
                    }
			public static void closeLogFile(){

				fs.Close();

 			}


        public  static bool checkIfTableExists(string tableName)
        {
            try
            {

                using (SQLiteConnection liteConnect = new SQLiteConnection(liteConnectionString))
                {
                    liteConnect.Open();
                    string sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='" + tableName + "';";
                    SQLiteCommand command = new SQLiteCommand(sql, liteConnect);
                    Object result = command.ExecuteScalar();
                    command.Dispose();
                    if (result.ToString() == "1")
                    {

                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
               
            }
            return false;
        }



        public static  ArrayList getExistingTables(){
            ArrayList tableList           = new ArrayList();     
            System.Data.DataTable dt      = new DataTable();
            try
            {
 
                using (SQLiteConnection liteConnect = new SQLiteConnection(liteConnectionString))
                {
                    liteConnect.Open();
                    string sql = "SELECT name FROM sqlite_master WHERE type='table'";
                    SQLiteCommand cmd = new SQLiteCommand(sql, liteConnect);
                    cmd.CommandTimeout = 0;
                    SQLiteDataReader reader = cmd.ExecuteReader();
                    dt.Load(reader);
                    cmd.Dispose();


                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);

            }

            foreach(DataRow row in dt.Rows){
                tableList.Add(row["name"].ToString());    
            }

            return  tableList;

        }
       public  static  StringBuilder removeNLastChars(StringBuilder builder,  int  numOfChars ){
                 StringBuilder temp  = new StringBuilder();
                for (int i=0; i<numOfChars;  i++  ){
                        builder.Length--;
                 }
                 temp = builder;
             return temp;

       }
        public static bool updateData(string sqlScript)
        {

            

            try
            {
                using (SQLiteConnection liteConnect = new SQLiteConnection(liteConnectionString))
                {
                    Console.WriteLine("Running: " + sqlScript);
                    writeToLog("Running: " + sqlScript);
                    liteConnect.Open();
                    SQLiteCommand command = new SQLiteCommand(sqlScript, liteConnect);
                    command.CommandTimeout = -1;
                    command.ExecuteNonQuery();
                    Console.WriteLine("Query complete");
                    writeToLog("Query complete");
                    command.Dispose();
                    return true;
                }
            }
            catch (Exception e)
            {

                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                writeToLog(e.Message);
                writeToLog(e.StackTrace);
                return false;

            }


        }
        public static DataTable getData(string theScript)
        {
            DataTable dt = new DataTable();

            try
            {
                using (SQLiteConnection liteConnect = new SQLiteConnection(liteConnectionString))
                {
                    liteConnect.Open();
                    SQLiteCommand cmd = new SQLiteCommand(theScript, liteConnect);
                    Console.WriteLine("Executing script: " + theScript);
                    writeToLog("Executing script: " + theScript);
                    cmd.CommandTimeout = -1;
                    SQLiteDataReader reader = cmd.ExecuteReader();
                    dt.Load(reader);
                    cmd.Dispose();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                writeToLog(e.Message);
                writeToLog(e.StackTrace);
            }
            return dt;
        }




    }
}