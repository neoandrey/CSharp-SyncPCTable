using  System;
using  System.Collections;
using  System.Collections.Generic;
using  System.Text;

namespace SyncPCTables
{
    public class SyncPCTablesConfig
    {
            public string       source_server  { set; get;}  
            public string       source_database  { set; get;}    
            public  int         source_port {set; get;}
            public string       destination_server { set; get;}
            public string       destination_database { set; get;}
            public int          destination_port {set; get;}
            public  string      pc_table_type {set; get;}
            public  string      pc_tables_fetch_script {set; get;}
            public  string      log_file {set; get;}
            public  int         pc_sync_type  {set; get;}
            public  ArrayList   pc_table_list {set; get;}
            public int          sync_tables_at_once    {set; get;}

            public  int         wait_interval  {set; get;}             
            public ArrayList   row_specific_fields {set; get;}

            public string      pc_tables_merge_script {set; get;}
            public bool      force_table_merge {set; get;}


    }
}