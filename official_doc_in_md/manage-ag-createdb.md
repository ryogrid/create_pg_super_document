22.2. Creating a Database  
---  
[Prev](manage-ag-overview.md "22.1. Overview") | [Up](managing-databases.md "Chapter 22. Managing Databases")| Chapter 22. Managing Databases| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](manage-ag-templatedbs.md "22.3. Template Databases")  
  
* * *

## 22.2. Creating a Database #

In order to create a database, the PostgreSQL server must be up and running (see [Section 18.3](server-start.md "18.3. Starting the Database Server")). 

Databases are created with the SQL command [CREATE DATABASE](sql-createdatabase.md "CREATE DATABASE"): 
    
    
    CREATE DATABASE _name_ ;
    

where _`name`_ follows the usual rules for SQL identifiers. The current role automatically becomes the owner of the new database. It is the privilege of the owner of a database to remove it later (which also removes all the objects in it, even if they have a different owner). 

The creation of databases is a restricted operation. See [Section 21.2](role-attributes.md "21.2. Role Attributes") for how to grant permission. 

Since you need to be connected to the database server in order to execute the `CREATE DATABASE` command, the question remains how the _first_ database at any given site can be created. The first database is always created by the `initdb` command when the data storage area is initialized. (See [Section 18.2](creating-cluster.md "18.2. Creating a Database Cluster").) This database is called `postgres`. So to create the first “ordinary” database you can connect to `postgres`. 

Two additional databases, `template1` and `template0`, are also created during database cluster initialization. Whenever a new database is created within the cluster, `template1` is essentially cloned. This means that any changes you make in `template1` are propagated to all subsequently created databases. Because of this, avoid creating objects in `template1` unless you want them propagated to every newly created database. `template0` is meant as a pristine copy of the original contents of `template1`. It can be cloned instead of `template1` when it is important to make a database without any such site-local additions. More details appear in [Section 22.3](manage-ag-templatedbs.md "22.3. Template Databases"). 

As a convenience, there is a program you can execute from the shell to create new databases, `createdb`.
    
    
    createdb _dbname_
    

`createdb` does no magic. It connects to the `postgres` database and issues the `CREATE DATABASE` command, exactly as described above. The [createdb](app-createdb.md "createdb") reference page contains the invocation details. Note that `createdb` without any arguments will create a database with the current user name. 

### Note

[Chapter 20](client-authentication.md "Chapter 20. Client Authentication") contains information about how to restrict who can connect to a given database. 

Sometimes you want to create a database for someone else, and have them become the owner of the new database, so they can configure and manage it themselves. To achieve that, use one of the following commands: 
    
    
    CREATE DATABASE _dbname_ OWNER _rolename_ ;
    

from the SQL environment, or: 
    
    
    createdb -O _rolename_ _dbname_
    

from the shell. Only the superuser is allowed to create a database for someone else (that is, for a role you are not a member of). 

* * *

[Prev](manage-ag-overview.md "22.1. Overview") | [Up](managing-databases.md "Chapter 22. Managing Databases")|  [Next](manage-ag-templatedbs.md "22.3. Template Databases")  
---|---|---  
22.1. Overview | [Home](index.md "PostgreSQL 17.5 Documentation")|  22.3. Template Databases
