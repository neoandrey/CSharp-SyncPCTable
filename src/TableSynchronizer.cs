using System;
using System.Collections;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Data;
using System.Linq;

namespace SyncPCTables{

    public class TableSynchronizer{

        string sourceServer = "";
        string sourceDatabase = "";
        string sourceTable = "";
        SqlConnection destinationConnection;

        string destinationServer = "";
        string destinationDatabase = "";
        string destinationTable = "";
        string tabDiffCmd = @"C:\Progra~1\Microsoft SQL Server\100\COM\tablediff.exe";
	   // string tabDiffCmd = @"tablediff.exe";
        string commandString = "";
        string sqlCompSqlFile= "";
		//ConnectionProperty conProps;
		string tabSyncSummaryFile= "";
		
		string sourceConnectionString;
		string destinationConnectionString;
		//bool isTruncated = false;
		bool isBulkInserted = false;

        public TableSynchronizer()
        {

        }
		public TableSynchronizer(string sourceServer,  string sourceDB,string sourceTable,  string destinationServer,string destinationDB, string destinationTable){
			
			this.setSourceServer(sourceServer);
			this.setDestinationServer(destinationServer);
			this.setSourceDatabase(sourceDB);
			this.setDestinationDatabase(destinationDB);
			this.setSourceTable(sourceTable);
			this.setDestinationTable(destinationTable);
			this.setSQLFile(sourceTable);
			string commandStr = getCommandString();
			runTableComparison(commandStr);
			initConnections();
			string result     = File.ReadAllText(getOutputFile());
			SyncPCTablesLibrary.writeToLog("Reading file: "+getOutputFile());
			SyncPCTablesLibrary.writeToLog("Result: "+result);
			if(result.Contains("comparison tables/views to have either a primary key, identity, rowguid or unique key column")){
				Console.WriteLine("Running Table Merge for  Source: "+this.getSourceDatabase()+".."+this.getSourceTable()+" to Destination: "+this.getDestinationTable());              
				string sourceTab =this.getSourceDatabase()+".."+this.getSourceTable();
				string destTab  =this.getDestinationDatabase()+".."+this.getDestinationTable();
				Console.WriteLine("Running Merge script for table: "+this.getDestinationTable());
				SyncPCTablesLibrary.writeToLog("Running Merge script for table: "+this.getDestinationTable());
				runTableMerge(sourceTab, destTab);
			} else {
			runSyncSQL(getSQLFile());
			}
			
		}
		
     public TableSynchronizer(string sourceServer,  string sourceDB,string sourceTable,  string destinationServer,string destinationDB, string destinationTable, string tabDiff){
			this.setTabDiffCmdStr(tabDiff);
			this.setSourceServer(sourceServer);
			this.setDestinationServer(destinationServer);
			this.setSourceDatabase(sourceDB);
			this.setDestinationDatabase(destinationDB);
			this.setSourceTable(sourceTable);
			this.setDestinationTable(destinationTable);
			this.setSQLFile(sourceTable);
			//conProps = new ConnectionProperty(sourceServer, destinationServer, sourceDB,destinationDB );
			string commandStr = getCommandString();
			runTableComparison(commandStr);
			initConnections();
			runSyncSQL(getSQLFile());
			
		}
        public string getSourceServer()
        {
            return this.sourceServer;
        }
		
        public string getSourceDatabase()
        {
            return this.sourceDatabase;
        }
        public string getSourceTable()
        {
            return this.sourceTable;
        }

        public string getDestinationServer()
        {
            return this.destinationServer;
        }
        public string getDestinationDatabase()
        {
            return this.destinationDatabase;
        }

        public string getDestinationTable()
        {
            return this.destinationTable;
        }
		
		 public string getSQLFile()
        {
            return this.sqlCompSqlFile;
        }

        public void setSourceServer(string server)
        {
            this.sourceServer = server;
        }

        public void setSourceDatabase(string database)
        {
            this.sourceDatabase = database;
        }
        public void setSourceTable(string table)
        {
            this.sourceTable = table.Replace("\'","").Replace("\"","");
        }

        public void setDestinationServer(string server)
        {
            this.destinationServer = server;
        }

      
        public void setDestinationDatabase(string database)
        {
            this.destinationDatabase = database;
        }
        public void setDestinationTable(string table)
        {
            this.destinationTable = table.Replace("\'","").Replace("\"","");
        }
        public void setSQLFile(string fileName)
        {
			
			
            this.sqlCompSqlFile =  fileName.EndsWith(".sql")?".\\sql\\"+fileName.Replace("[","").Replace("]","") :".\\sql\\"+fileName.Replace("[","").Replace("]","")+".sql";
			this.tabSyncSummaryFile = ".\\sql\\"+fileName.Replace("[","").Replace("]","")+"_sync_summary.txt";
			if(File.Exists(sqlCompSqlFile)){
				File.Delete(sqlCompSqlFile);
				File.Delete(tabSyncSummaryFile);
			}
			 this.sqlCompSqlFile =Path.GetFullPath(sqlCompSqlFile);
			 this.tabSyncSummaryFile =Path.GetFullPath(tabSyncSummaryFile);
        }
      public string getOutputFile(){
		     return this.tabSyncSummaryFile;
		  
		  
	  }
	  
	  public string getTabDiffCmdStr(){
		  
		  return this.tabDiffCmd;
		  
	  }
	  
	  public void setTabDiffCmdStr(string tabDiff){
		  
		  this.tabDiffCmd = tabDiff;
	  }
        public string getCommandString()  {
            tabDiffCmd = File.Exists(tabDiffCmd) ? tabDiffCmd : @"C:\Progra~1\Microsoft SQL Server\110\COM\tablediff.exe";
            if (File.Exists(tabDiffCmd))
            {
               if (File.Exists(getOutputFile())) File.Delete (getOutputFile());
                commandString = "\"" + tabDiffCmd + "\" -t 3600  -sourceserver  " + SyncPCTablesLibrary.sourceConnectionProps.getSourceServer() + " -sourceuser " + SyncPCTablesLibrary.sourceConnectionProps.getSourceUser() + " -sourcepassword " + SyncPCTablesLibrary.sourceConnectionProps.getSourcePassword() + " -sourcedatabase " + SyncPCTablesLibrary.sourceConnectionProps.getSourceDatabase() + " -sourcetable " + this.getSourceTable() +
                                " -destinationserver " +SyncPCTablesLibrary.destinationConnectionProps.getSourceServer() + " -destinationuser " +SyncPCTablesLibrary.destinationConnectionProps.getSourceUser() + " -destinationpassword " + SyncPCTablesLibrary.destinationConnectionProps.getSourcePassword()+ " -destinationdatabase " + SyncPCTablesLibrary.destinationConnectionProps.getSourceDatabase() + " -destinationtable " + this.getDestinationTable() + " -f \"" + this.getSQLFile()+"\"";
							//	"\" -o \""+ getOutputFile()+"\"";
				
            }else{
				tabDiffCmd="tablediff.exe";
           if (File.Exists(tabDiffCmd))
            {
               if (File.Exists(getOutputFile())) File.Delete (getOutputFile());
                commandString = "\"" + tabDiffCmd + "\" -t 3600  -sourceserver  " + SyncPCTablesLibrary.sourceConnectionProps.getSourceServer() + " -sourceuser " + SyncPCTablesLibrary.sourceConnectionProps.getSourceUser() + " -sourcepassword " + SyncPCTablesLibrary.sourceConnectionProps.getSourcePassword() + " -sourcedatabase " + SyncPCTablesLibrary.sourceConnectionProps.getSourceDatabase() + " -sourcetable " + this.getSourceTable() +
                                " -destinationserver " +SyncPCTablesLibrary.destinationConnectionProps.getSourceServer() + " -destinationuser " + SyncPCTablesLibrary.destinationConnectionProps.getSourceUser() + " -destinationpassword " + SyncPCTablesLibrary.destinationConnectionProps.getSourcePassword() + " -destinationdatabase " + SyncPCTablesLibrary.destinationConnectionProps.getSourceDatabase() + " -destinationtable " + this.getDestinationTable() + " -f \"" + this.getSQLFile()+"\"";
							//	"\" -o \""+ getOutputFile()+"\"";
            } else
            {

                Console.WriteLine("tablediff command not found");
            }
			}
			//Console.WriteLine("Command: "+commandString );
            return commandString;
        }

        public void runTableComparison(string command)
        {

            try
            {

                Process cmd = new Process();
                cmd.StartInfo.FileName = this.getCommandString();
		        Console.WriteLine("Runnning Table Sync command for :"+this.getDestinationTable());
                cmd.StartInfo.RedirectStandardInput = true;
                cmd.StartInfo.RedirectStandardOutput = true;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.UseShellExecute = false;
                cmd.Start();
				string result = cmd.StandardOutput.ReadToEnd();
			    cmd.WaitForExit();
				cmd.StandardOutput.Close();             
				cmd.StandardInput.Close();
				result = result.Trim(); 
                char[] splitter =  {'\n'};
                string[] resultComp = result.Split(splitter);
                string resultStr = resultComp[resultComp.Length - 3]+"\n"+resultComp[resultComp.Length - 2]+"\n"+resultComp[resultComp.Length - 1];
				System.IO.File.WriteAllText(getOutputFile(), resultStr);

            }
            catch (Exception e)
            {
                Console.WriteLine("Error running table comparison: " + e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        public void initConnections()
        {
            try
            {
				sourceConnectionString      =  "Network Library=DBMSSOCN;Data Source=" + SyncPCTablesLibrary.sourceConnectionProps.getSourceServer() + ","+SyncPCTablesLibrary.sourceConnectionProps.getSourcePort()+";database=" + SyncPCTablesLibrary.sourceConnectionProps.getSourceDatabase()+ ";User id=" +  SyncPCTablesLibrary.sourceConnectionProps.getSourceUser()+ ";Password=" + SyncPCTablesLibrary.sourceConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";
				destinationConnectionString =  "Network Library=DBMSSOCN;Data Source=" +  SyncPCTablesLibrary.destinationConnectionProps.getSourceServer() + ","+SyncPCTablesLibrary.destinationConnectionProps.getSourcePort()+";database=" +SyncPCTablesLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" + SyncPCTablesLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" +SyncPCTablesLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";
               
			   destinationConnection = new SqlConnection("Network Library=DBMSSOCN;Data Source=" +  SyncPCTablesLibrary.destinationConnectionProps.getSourceServer() + ","+SyncPCTablesLibrary.destinationConnectionProps.getSourcePort()+";database=" +SyncPCTablesLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" + SyncPCTablesLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" +SyncPCTablesLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;");
             
            }
            catch (Exception e)
            {
                Console.WriteLine("Error initiating connections: " + e.Message);
                Console.WriteLine(e.StackTrace);

            }

        }
        public void runSyncSQL(string queryFile)
        {
            string sql_query = "";
            queryFile = queryFile != null ? getSQLFile() : "";
            try
            {
                if (File.Exists(queryFile))
                {
						using (SqlConnection destinationConnection =  new SqlConnection(destinationConnectionString)){
						string  sql_query_all = File.ReadAllText(queryFile);
						SyncPCTablesLibrary.writeToLog("Running script: "+sql_query_all);
						string [] lineComp;
						destinationConnection.Open();
						string[] lines = sql_query_all.Split('\n');
						string identity_str = "";
						int tempCounter = 0;
						bool useBulkMethod =false;
                        if (sql_query_all.ToLower().Contains("not included in this script") ) {
								useBulkMethod =true;
						}
						tempCounter = 0;
						if(!isBulkInserted ){
						if(  useBulkMethod)  {
						//	truncateDestinationTable(this.getDestinationTable());
							string sourceTab   =this.getSourceDatabase()+".."+this.getSourceTable();
							string destTab     =this.getDestinationDatabase()+".."+this.getDestinationTable();
						//	Console.WriteLine("using bulk method for "+this.getDestinationTable());
						 string placeHolder    =  "CURRENT_TABLE_NAME";
						 string colScript      = SyncPCTablesLibrary.fetchColumnsScript.Replace(placeHolder,destTab);
						 ArrayList columnList = new ArrayList();
			             DataTable   tempTab             =   SyncPCTablesLibrary.getDataFromSQL(colScript, SyncPCTablesLibrary.destinationConnectionProps.getConnectionString());
                
							foreach (DataRow row in tempTab.Rows) {

							foreach (DataColumn column in tempTab.Columns){

							  columnList.Add(row[column].ToString());

							}

                		 }		
						//	runBulkInsert(sourceTab, destTab);
                        StringBuilder  tableUpdateClauseBuilder =  new StringBuilder();
						StringBuilder  tableInsertClauseBuilder =  new StringBuilder();
						StringBuilder  columnListBuilder        =  new StringBuilder();
						StringBuilder  searchConditionsBuilder      =  new StringBuilder();
						
						string  sourceTable       = "SOURCE";
						string  destTable         = "TARGET";

					 foreach(string col in columnList){

                        tableUpdateClauseBuilder.Append(string.Format("{1}.{0}  = {2}.{0},", col,sourceTable,destTable));
						   
					 }

					 
					 foreach(string col in SyncPCTablesLibrary.rowSpecificFields){

                        searchConditionsBuilder.Append(string.Format("{1}.{0}  = {2}.{0} AND ", col,sourceTable,destTable));
						   
					 }

				  

					   foreach(string col in columnList){

                        tableInsertClauseBuilder.Append(string.Format("{0}.{1},", sourceTable,col));
						columnListBuilder.Append(string.Format("{0},",col));

					 }
					tableUpdateClauseBuilder     = 	 SyncPCTablesLibrary.removeNLastChars(tableUpdateClauseBuilder,1);
					tableInsertClauseBuilder     =   SyncPCTablesLibrary.removeNLastChars(tableInsertClauseBuilder,1);
				    columnListBuilder            =   SyncPCTablesLibrary.removeNLastChars(columnListBuilder,1);
                    searchConditionsBuilder      =   SyncPCTablesLibrary.removeNLastChars(searchConditionsBuilder,5);

					string mergeScript       =   SyncPCTablesLibrary.mergeScript.Replace("DESTINATION_SERVER",SyncPCTablesLibrary.destinationServer)
																    .Replace("DESTINATION_DATABASE",SyncPCTablesLibrary.destinationDatabase)
																    .Replace("DESTINATION_TABLE",destTable)
																    .Replace("SOURCE_SERVER",SyncPCTablesLibrary.sourceServer)
																    .Replace("SOURCE_DATABASE",SyncPCTablesLibrary.sourceDatabase)	
																    .Replace("SOURCE_TABLE",sourceTable)
																    .Replace("SEARCH_CONDITIONS",searchConditionsBuilder.ToString())
																    .Replace("TABLE_UPDATE_LIST",tableUpdateClauseBuilder.ToString())
																    .Replace("TABLE_COLUMN_LIST",columnListBuilder.ToString())
																	.Replace("TABLE_INSERT_LIST",tableInsertClauseBuilder.ToString());															   

				    SyncPCTablesLibrary.executOnServer(SyncPCTablesLibrary.destinationConnectionProps.getConnectionString(),mergeScript);
                  

                } else{
						while (tempCounter< lines.Length){
							if(lines[tempCounter].Contains("IDENTITY_INSERT")){
								identity_str = lines[tempCounter];
								break;
							}
							 ++tempCounter;
						}
						Console.WriteLine("using insert method for "+this.getDestinationTable());
						StringBuilder sqlBuilder = new StringBuilder();
						string[] individualQueries =    sql_query_all.Split(new string[] { "INSERT INTO" }, StringSplitOptions.None);
						if(lines.Length> 50 && individualQueries.Length==0){
							Console.WriteLine("Running SQL query: " + sql_query_all+"\n GO");
							SqlCommand cmd = new SqlCommand(sql_query_all, destinationConnection);
							cmd.CommandTimeout = 0;
							cmd.ExecuteNonQuery();
						  }else{
								int div =20;
								int counter=0;
								
								for(int j = 0;j < individualQueries.Length; j++ ){
									++counter;
									if(j>=1){
										sqlBuilder.Append("\nINSERT INTO ").Append(individualQueries[j]);
									}
									if(counter ==div || j==(individualQueries.Length-1)){
										if(individualQueries[0].Contains("IDENTITY_INSERT")){
											lineComp = individualQueries[0].Split('\n'); 
											sqlBuilder.Insert(0,"\n"+identity_str+"\n");
										} 
									    sqlBuilder.Append(";");
										sql_query = sqlBuilder.ToString();
										sql_query = sql_query.Replace(",N'",",'");
										Console.WriteLine("Running SQL query: " + sql_query);
										SqlCommand cmd = new SqlCommand(sql_query, destinationConnection);
										cmd.CommandTimeout = 0;
										cmd.ExecuteNonQuery();
										counter=0;
										sqlBuilder.Remove(0, sqlBuilder.Length);
										
									}
									
								}  
				  }
						}
					Console.WriteLine(getSourceTable()+" on "+getSourceServer()+" has been successfully synchronized with "+getDestinationTable()+ " and  "+getDestinationServer());
					destinationConnection.Close();
					}
}
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error running table comparison: " + e.Message);
                Console.WriteLine(e.StackTrace);

            }
        }
		
		public void runTableMerge(string sourceTable, string destinationTable){

						string placeHolder    =  "CURRENT_TABLE_NAME";
						string colScript      = SyncPCTablesLibrary.fetchColumnsScript.Replace(placeHolder,destinationTable).Replace(SyncPCTablesLibrary.destinationDatabase+"..", "");
						ArrayList columnList = new ArrayList();
						ArrayList columnListNoBraces = new ArrayList();
						DataTable   tempTab             =   SyncPCTablesLibrary.getDataFromSQL(colScript, SyncPCTablesLibrary.destinationConnectionProps.getConnectionString());
                        string  rowData ="";
						foreach (DataRow row in tempTab.Rows) {

						foreach (DataColumn column in tempTab.Columns){
                           rowData ="["+row[column].ToString()+"]";
						   columnList.Add(rowData);
						   columnListNoBraces.Add(row[column].ToString());

							}

                		 }		
						//	runBulkInsert(sourceTab, destTab);
                        StringBuilder  tableUpdateClauseBuilder =  new StringBuilder();
						StringBuilder  tableInsertClauseBuilder =  new StringBuilder();
						StringBuilder  columnListBuilder        =  new StringBuilder();
						StringBuilder  searchConditionsBuilder      =  new StringBuilder();
						
						string  srcTable       = "SOURCE";
						string  destTable         = "TARGET";

					 foreach(string col in columnList){

                         tableUpdateClauseBuilder.Append(string.Format("{2}.{0}  = {1}.{0},", col,srcTable,destTable));
						   
					 }

					 ArrayList  searchFieldList = SyncPCTablesLibrary.rowSpecificFields.Count>0?  SyncPCTablesLibrary.rowSpecificFields:columnListNoBraces;
					 foreach(string col in searchFieldList){

                        searchConditionsBuilder.Append(string.Format("{2}.[{0}]  = {1}.[{0}] AND ", col,srcTable,destTable));
						   
					 }

				  

					   foreach(string col in columnList){

                        tableInsertClauseBuilder.Append(string.Format("{0},", col));
						columnListBuilder.Append(string.Format("{0},",col));

					 }
					tableUpdateClauseBuilder     = 	 SyncPCTablesLibrary.removeNLastChars(tableUpdateClauseBuilder,1);
					tableInsertClauseBuilder     =   SyncPCTablesLibrary.removeNLastChars(tableInsertClauseBuilder,1);
				    columnListBuilder            =   SyncPCTablesLibrary.removeNLastChars(columnListBuilder,1);
                    searchConditionsBuilder      =   SyncPCTablesLibrary.removeNLastChars(searchConditionsBuilder,5);

					string mergeScript       =   SyncPCTablesLibrary.mergeScript.Replace("DESTINATION_SERVER",SyncPCTablesLibrary.destinationServer)
																    .Replace("DESTINATION_DATABASE",SyncPCTablesLibrary.destinationDatabase)
																    .Replace("DESTINATION_TABLE",destinationTable.Replace(SyncPCTablesLibrary.destinationDatabase+"..", ""))
																    .Replace("SOURCE_SERVER",SyncPCTablesLibrary.sourceServer)
																    .Replace("SOURCE_DATABASE",SyncPCTablesLibrary.sourceDatabase)	
																    .Replace("SOURCE_TABLE",sourceTable.Replace(SyncPCTablesLibrary.sourceDatabase+"..", ""))
																    .Replace("SEARCH_CONDITIONS",searchConditionsBuilder.ToString())
																    .Replace("TABLE_UPDATE_LIST",tableUpdateClauseBuilder.ToString())
																    .Replace("TABLE_COLUMN_LIST",columnListBuilder.ToString())
																	.Replace("TABLE_INSERT_LIST",tableInsertClauseBuilder.ToString());															   

				    SyncPCTablesLibrary.executOnServer(SyncPCTablesLibrary.destinationConnectionProps.getConnectionString(),mergeScript);
                  

			
		}
		public void runBulkInsert(string sourceTable, string destinationTable){
			try{
				
				sourceConnectionString      =  "Network Library=DBMSSOCN;Data Source=" + SyncPCTablesLibrary.sourceConnectionProps.getSourceServer() + ","+SyncPCTablesLibrary.sourceConnectionProps.getSourcePort()+";database=" +SyncPCTablesLibrary.sourceConnectionProps.getSourceDatabase()+ ";User id=" + SyncPCTablesLibrary.sourceConnectionProps.getSourceUser()+ ";Password=" +SyncPCTablesLibrary.sourceConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";
				destinationConnectionString =  "Network Library=DBMSSOCN;Data Source=" + SyncPCTablesLibrary.destinationConnectionProps.getSourceServer() + ","+SyncPCTablesLibrary.destinationConnectionProps.getSourcePort()+";database=" +SyncPCTablesLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" + SyncPCTablesLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" +SyncPCTablesLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";
				using (SqlConnection destConnection =  new SqlConnection(destinationConnectionString)){
					destConnection.Open();
				//	Console.WriteLine("Running: "+string.Format("SELECT  rec_count = ISNULL(count(*),0) FROM {0} WITH (NOLOCK) OPTION (RECOMPILE, MAXDOP 3)", destinationTable));
					SqlCommand cmd2 = new SqlCommand(string.Format("SELECT  rec_count = ISNULL(count(*),0) FROM {0} WITH (NOLOCK) OPTION (RECOMPILE, MAXDOP 3)", destinationTable), destConnection);
					cmd2.CommandTimeout = 0;
				    SqlDataReader reader2 = cmd2.ExecuteReader();
					Int32  count  =0;
					if(reader2.Read())  count = Int32.Parse(reader2["rec_count"].ToString().Trim());
					if(count==0) {
					
							using (SqlConnection sourceConnection =  new SqlConnection(sourceConnectionString)){
							sourceConnection.Open();
							SqlCommand cmd = new SqlCommand(string.Format("SELECT  * FROM {0} WITH (NOLOCK) OPTION (RECOMPILE, MAXDOP 3)", sourceTable), sourceConnection);
							cmd.CommandTimeout =0;
							SqlDataReader reader = cmd.ExecuteReader();
							using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnectionString,SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls)){ 
							bulkCopy.BulkCopyTimeout = 0;
							bulkCopy.BatchSize = 1000;
							bulkCopy.DestinationTableName = destinationTable;
							bulkCopy.WriteToServer(reader);
							}
							reader.Close();
			}
						reader2.Close();
			}
			isBulkInserted = true;
			Console.WriteLine(sourceTable+" has been successfully synchronized with "+destinationTable );
			
		
		}
		}catch(Exception e){
			Console.WriteLine("Error running bulk insert: " + e.Message);
            Console.WriteLine(e.StackTrace);
			isBulkInserted = false;
			
		}
		}
		
		public  void truncateDestinationTable(string tableName) {
			    Console.WriteLine("Truncating table "+tableName+"");
		        destinationConnectionString =  "Network Library=DBMSSOCN;Data Source=" + SyncPCTablesLibrary.destinationConnectionProps.getSourceServer() + ","+SyncPCTablesLibrary.destinationConnectionProps.getSourcePort()+";database=" +SyncPCTablesLibrary.destinationConnectionProps.getSourceDatabase()+ ";User id=" + SyncPCTablesLibrary.destinationConnectionProps.getSourceUser()+ ";Password=" +SyncPCTablesLibrary.destinationConnectionProps.getSourcePassword() + ";Connection Timeout=0;Pooling=false;";
              Int32 record_count = -1;
			try{
				 while(record_count!=0){
						using (SqlConnection destinationConnection =  new SqlConnection(destinationConnectionString)){
								Console.WriteLine(string.Format("TRUNCATE TABLE {0}", tableName));
								SqlCommand cmd = new SqlCommand(string.Format("TRUNCATE TABLE {0}; ", tableName), destinationConnection);
								cmd.CommandTimeout = 0;
								destinationConnection.Open();
								cmd.ExecuteNonQuery();
								Console.WriteLine(tableName+"  truncated successfully");
							     cmd = new SqlCommand(string.Format("SELECT  ISNULL(COUNT(*),0) rec_count FROM {0}; ", tableName), destinationConnection);
								 record_count =  (int)cmd.ExecuteScalar();
								 
							}
			}
			} catch (Exception e)
            {
				
            Console.WriteLine("Error truncating  table: "+tableName+"\nError:\n"+ e.Message);
                Console.WriteLine(e.StackTrace);
            }
			
        }
	
		  public static void Main(string[] args){
				new TableSynchronizer(args[0],args[1],args[2],args[3],args[4],args[5]);
			}
  
    }
 

    }
  

  
