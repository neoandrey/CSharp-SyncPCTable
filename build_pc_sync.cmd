rem C:\Windows\Microsoft.NET\Framework64\v3.5\csc.exe 

C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe  /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll","lib\System.Threading.Tasks.dll"  /t:library  /out:bin\SyncPCTablesProcess.exe src\ConnectionCipher.cs src\ConnectionProperty.cs src\TableSynchronizer.cs src\SyncPCTablesLibrary.cs src\SyncPCTablesConfig.cs src\SyncPCTablesProcess.cs

pause