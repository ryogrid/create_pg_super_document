DISCONNECT  
---  
[Prev](ecpg-sql-describe.md "DESCRIBE") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")| 34.14. Embedded SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-sql-execute-immediate.md "EXECUTE IMMEDIATE")  
  
* * *

## DISCONNECT

DISCONNECT — terminate a database connection

## Synopsis
    
    
    DISCONNECT _connection_name_
    DISCONNECT [ CURRENT ]
    DISCONNECT ALL
    

## Description

`DISCONNECT` closes a connection (or all connections) to the database. 

## Parameters

 _`connection_name`_ #
    

A database connection name established by the `CONNECT` command. 

`CURRENT` #
    

Close the “current” connection, which is either the most recently opened connection, or the connection set by the `SET CONNECTION` command. This is also the default if no argument is given to the `DISCONNECT` command. 

`ALL` #
    

Close all open connections. 

## Examples
    
    
    int
    main(void)
    {
        EXEC SQL CONNECT TO testdb AS con1 USER testuser;
        EXEC SQL CONNECT TO testdb AS con2 USER testuser;
        EXEC SQL CONNECT TO testdb AS con3 USER testuser;
    
        EXEC SQL DISCONNECT CURRENT;  /* close con3          */
        EXEC SQL DISCONNECT ALL;      /* close con2 and con1 */
    
        return 0;
    }
    

## Compatibility

`DISCONNECT` is specified in the SQL standard. 

## See Also

[CONNECT](ecpg-sql-connect.md "CONNECT"), [SET CONNECTION](ecpg-sql-set-connection.md "SET CONNECTION")

* * *

[Prev](ecpg-sql-describe.md "DESCRIBE") | [Up](ecpg-sql-commands.md "34.14. Embedded SQL Commands")|  [Next](ecpg-sql-execute-immediate.md "EXECUTE IMMEDIATE")  
---|---|---  
DESCRIBE | [Home](index.md "PostgreSQL 17.5 Documentation")|  EXECUTE IMMEDIATE
