SELECT TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES where  table_type = 'BASE TABLE' AND TABLE_NAME LIKE 'PC_TABLE_NAME_[0-9]%'  ORDER BY  SUBSTRING(TABLE_NAME, (CHARINDEX('ds_', TABLE_NAME)+3), LEN(TABLE_NAME))  ASC