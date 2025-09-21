22.4. Database Configuration  
---  
[Prev](manage-ag-templatedbs.md "22.3. Template Databases") | [Up](managing-databases.md "Chapter 22. Managing Databases")| Chapter 22. Managing Databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](manage-ag-dropdb.md "22.5. Destroying a Database")  
  
* * *

## 22.4. Database Configuration #

Recall from [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") that the PostgreSQL server provides a large number of run-time configuration variables. You can set database-specific default values for many of these settings. 

For example, if for some reason you want to disable the GEQO optimizer for a given database, you'd ordinarily have to either disable it for all databases or make sure that every connecting client is careful to issue `SET geqo TO off`. To make this setting the default within a particular database, you can execute the command: 
    
    
    ALTER DATABASE mydb SET geqo TO off;
    

This will save the setting (but not set it immediately). In subsequent connections to this database it will appear as though `SET geqo TO off;` had been executed just before the session started. Note that users can still alter this setting during their sessions; it will only be the default. To undo any such setting, use `ALTER DATABASE _`dbname`_ RESET _`varname`_`. 

* * *

[Prev](manage-ag-templatedbs.md "22.3. Template Databases") | [Up](managing-databases.md "Chapter 22. Managing Databases")|  [Next](manage-ag-dropdb.md "22.5. Destroying a Database")  
---|---|---  
22.3. Template Databases | [Home](index.md "PostgreSQL 17.5 Documentation")|  22.5. Destroying a Database
