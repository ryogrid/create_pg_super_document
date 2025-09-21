22.5. Destroying a Database  
---  
[Prev](manage-ag-config.md "22.4. Database Configuration") | [Up](managing-databases.md "Chapter 22. Managing Databases")| Chapter 22. Managing Databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](manage-ag-tablespaces.md "22.6. Tablespaces")  
  
* * *

## 22.5. Destroying a Database #

Databases are destroyed with the command [DROP DATABASE](sql-dropdatabase.md "DROP DATABASE"):
    
    
    DROP DATABASE _name_ ;
    

Only the owner of the database, or a superuser, can drop a database. Dropping a database removes all objects that were contained within the database. The destruction of a database cannot be undone. 

You cannot execute the `DROP DATABASE` command while connected to the victim database. You can, however, be connected to any other database, including the `template1` database. `template1` would be the only option for dropping the last user database of a given cluster. 

For convenience, there is also a shell program to drop databases, [dropdb](app-dropdb.md "dropdb"):
    
    
    dropdb _dbname_
    

(Unlike `createdb`, it is not the default action to drop the database with the current user name.) 

* * *

[Prev](manage-ag-config.md "22.4. Database Configuration") | [Up](managing-databases.md "Chapter 22. Managing Databases")|  [Next](manage-ag-tablespaces.md "22.6. Tablespaces")  
---|---|---  
22.4. Database Configuration | [Home](index.md "PostgreSQL 17.5 Documentation")|  22.6. Tablespaces
