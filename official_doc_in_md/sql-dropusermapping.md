DROP USER MAPPING  
---  
[Prev](sql-dropuser.md "DROP USER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropview.md "DROP VIEW")  
  
* * *

## DROP USER MAPPING

DROP USER MAPPING — remove a user mapping for a foreign server

## Synopsis
    
    
    DROP USER MAPPING [ IF EXISTS ] FOR { _user_name_ | USER | CURRENT_ROLE | CURRENT_USER | PUBLIC } SERVER _server_name_
    

## Description

`DROP USER MAPPING` removes an existing user mapping from foreign server. 

The owner of a foreign server can drop user mappings for that server for any user. Also, a user can drop a user mapping for their own user name if `USAGE` privilege on the server has been granted to the user. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the user mapping does not exist. A notice is issued in this case. 

_`user_name`_
    

User name of the mapping. `CURRENT_ROLE`, `CURRENT_USER`, and `USER` match the name of the current user. `PUBLIC` is used to match all present and future user names in the system. 

_`server_name`_
    

Server name of the user mapping. 

## Examples

Drop a user mapping `bob`, server `foo` if it exists: 
    
    
    DROP USER MAPPING IF EXISTS FOR bob SERVER foo;
    

## Compatibility

`DROP USER MAPPING` conforms to ISO/IEC 9075-9 (SQL/MED). The `IF EXISTS` clause is a PostgreSQL extension. 

## See Also

[CREATE USER MAPPING](sql-createusermapping.md "CREATE USER MAPPING"), [ALTER USER MAPPING](sql-alterusermapping.md "ALTER USER MAPPING")

* * *

[Prev](sql-dropuser.md "DROP USER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropview.md "DROP VIEW")  
---|---|---  
DROP USER | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP VIEW
