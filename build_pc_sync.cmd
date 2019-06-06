rem C:\Windows\Microsoft.NET\Framework64\v3.5\csc.exe 

C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll","lib\System.Net.Http.dll","lib\System.Threading.Tasks.dll"  /t:library  /out:lib\resources.dll src\ConnectionCipher.cs src\ConnectionProperty.cs src\TableSynchronizer.cs src\SyncPCTablesLibrary.cs src\SyncPCTablesConfig.cs 

xcopy /y  lib\resources.dll bin\

C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll","lib\System.Net.Http.dll","lib\System.Threading.Tasks.dll","lib\resources.dll" /out:bin\SyncPCTablesProcess.exe src\SyncPCTablesProcess.cs

pause