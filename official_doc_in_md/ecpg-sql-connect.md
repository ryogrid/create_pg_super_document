CONNECT  
---  
[Prev](ecpg-sql-allocate-descriptor.md "ALLOCATE DESCRIPTOR") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-deallocate-descriptor.md "DEALLOCATE DESCRIPTOR")  
  
* * *

## CONNECT

CONNECT — establish a database connection

## Synopsis
    
    
    CONNECT TO _connection_target_ [ AS _connection_name_ ] [ USER _connection_user_ ]
    CONNECT TO DEFAULT
    CONNECT _connection_user_
    DATABASE _connection_target_
    

## Description

The `CONNECT` command establishes a connection between the client and the PostgreSQL server. 

## Parameters

 _`connection_target`_ #
    

_`connection_target`_ specifies the target server of the connection on one of several forms. 

[ _`database_name`_ ] [ `@`_`host`_ ] [ `:`_`port`_ ] #
    

Connect over TCP/IP 

`unix:postgresql://`_`host`_ [ `:`_`port`_ ] `/` [ _`database_name`_ ] [ `?`_`connection_option`_ ] #
    

Connect over Unix-domain sockets 

`tcp:postgresql://`_`host`_ [ `:`_`port`_ ] `/` [ _`database_name`_ ] [ `?`_`connection_option`_ ] #
    

Connect over TCP/IP 

SQL string constant #
    

containing a value in one of the above forms 

host variable #
    

host variable of type `char[]` or `VARCHAR[]` containing a value in one of the above forms 

_`connection_name`_ #
    

An optional identifier for the connection, so that it can be referred to in other commands. This can be an SQL identifier or a host variable. 

_`connection_user`_ #
    

The user name for the database connection. 

This parameter can also specify user name and password, using one the forms `_`user_name`_ /_`password`_`, `_`user_name`_ IDENTIFIED BY _`password`_`, or `_`user_name`_ USING _`password`_`. 

User name and password can be SQL identifiers, string constants, or host variables. 

`DEFAULT` #
    

Use all default connection parameters, as defined by libpq. 

## Examples

Here a several variants for specifying connection parameters: 
    
    
    EXEC SQL CONNECT TO "connectdb" AS main;
    EXEC SQL CONNECT TO "connectdb" AS second;
    EXEC SQL CONNECT TO "unix:postgresql://200.46.204.71/connectdb" AS main USER connectuser;
    EXEC SQL CONNECT TO "unix:postgresql://localhost/connectdb" AS main USER connectuser;
    EXEC SQL CONNECT TO 'connectdb' AS main;
    EXEC SQL CONNECT TO 'unix:postgresql://localhost/connectdb' AS main USER :user;
    EXEC SQL CONNECT TO :db AS :id;
    EXEC SQL CONNECT TO :db USER connectuser USING :pw;
    EXEC SQL CONNECT TO @localhost AS main USER connectdb;
    EXEC SQL CONNECT TO REGRESSDB1 as main;
    EXEC SQL CONNECT TO AS main USER connectdb;
    EXEC SQL CONNECT TO connectdb AS :id;
    EXEC SQL CONNECT TO connectdb AS main USER connectuser/connectdb;
    EXEC SQL CONNECT TO connectdb AS main;
    EXEC SQL CONNECT TO connectdb@localhost AS main;
    EXEC SQL CONNECT TO tcp:postgresql://localhost/ USER connectdb;
    EXEC SQL CONNECT TO tcp:postgresql://localhost/connectdb USER connectuser IDENTIFIED BY connectpw;
    EXEC SQL CONNECT TO tcp:postgresql://localhost:20/connectdb USER connectuser IDENTIFIED BY connectpw;
    EXEC SQL CONNECT TO unix:postgresql://localhost/ AS main USER connectdb;
    EXEC SQL CONNECT TO unix:postgresql://localhost/connectdb AS main USER connectuser;
    EXEC SQL CONNECT TO unix:postgresql://localhost/connectdb USER connectuser IDENTIFIED BY "connectpw";
    EXEC SQL CONNECT TO unix:postgresql://localhost/connectdb USER connectuser USING "connectpw";
    EXEC SQL CONNECT TO unix:postgresql://localhost/connectdb?connect_timeout=14 USER connectuser;
    

Here is an example program that illustrates the use of host variables to specify connection parameters: 
    
    
    int
    main(void)
    {
    EXEC SQL BEGIN DECLARE SECTION;
        char *dbname     = "testdb";    /* database name */
        char *user       = "testuser";  /* connection user name */
        char *connection = "tcp:postgresql://localhost:5432/testdb";
                                        /* connection string */
        char ver[256];                  /* buffer to store the version string */
    EXEC SQL END DECLARE SECTION;
    
        ECPGdebug(1, stderr);
    
        EXEC SQL CONNECT TO :dbname USER :user;
        EXEC SQL SELECT pg_catalog.set_config('search_path', '', false); EXEC SQL COMMIT;
        EXEC SQL SELECT version() INTO :ver;
        EXEC SQL DISCONNECT;
    
        printf("version: %s\n", ver);
    
        EXEC SQL CONNECT TO :connection USER :user;
        EXEC SQL SELECT pg_catalog.set_config('search_path', '', false); EXEC SQL COMMIT;
        EXEC SQL SELECT version() INTO :ver;
        EXEC SQL DISCONNECT;
    
        printf("version: %s\n", ver);
    
        return 0;
    }
    

## Compatibility

`CONNECT` is specified in the SQL standard, but the format of the connection parameters is implementation-specific. 

## See Also

[DISCONNECT](ecpg-sql-disconnect.md "DISCONNECT"), [SET CONNECTION](ecpg-sql-set-connection.md "SET CONNECTION")

* * *

[Prev](ecpg-sql-allocate-descriptor.md "ALLOCATE DESCRIPTOR") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-deallocate-descriptor.md "DEALLOCATE DESCRIPTOR")  
---|---|---  
ALLOCATE DESCRIPTOR | [Home](index.md "PostgreSQL 17.5 Documentation")|  DEALLOCATE DESCRIPTOR
