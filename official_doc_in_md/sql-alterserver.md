ALTER SERVER  
---  
[Prev](sql-altersequence.md "ALTER SEQUENCE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterstatistics.md "ALTER STATISTICS")  
  
* * *

## ALTER SERVER

ALTER SERVER — change the definition of a foreign server

## Synopsis
    
    
    ALTER SERVER _name_ [ VERSION '_new_version_ ' ]
        [ OPTIONS ( [ ADD | SET | DROP ] _option_ ['_value_ '] [, ... ] ) ]
    ALTER SERVER _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER SERVER _name_ RENAME TO _new_name_
    

## Description

`ALTER SERVER` changes the definition of a foreign server. The first form changes the server version string or the generic options of the server (at least one clause is required). The second form changes the owner of the server. 

To alter the server you must be the owner of the server. Additionally to alter the owner, you must be able to `SET ROLE` to the new owning role, and you must have `USAGE` privilege on the server's foreign-data wrapper. (Note that superusers satisfy all these criteria automatically.) 

## Parameters

 _`name`_
    

The name of an existing server. 

_`new_version`_
    

New server version. 

`OPTIONS ( [ ADD | SET | DROP ] _`option`_ ['_`value`_ '] [, ... ] )`
    

Change options for the server. `ADD`, `SET`, and `DROP` specify the action to be performed. `ADD` is assumed if no operation is explicitly specified. Option names must be unique; names and values are also validated using the server's foreign-data wrapper library. 

_`new_owner`_
    

The user name of the new owner of the foreign server. 

_`new_name`_
    

The new name for the foreign server. 

## Examples

Alter server `foo`, add connection options: 
    
    
    ALTER SERVER foo OPTIONS (host 'foo', dbname 'foodb');
    

Alter server `foo`, change version, change `host` option: 
    
    
    ALTER SERVER foo VERSION '8.4' OPTIONS (SET host 'baz');
    

## Compatibility

`ALTER SERVER` conforms to ISO/IEC 9075-9 (SQL/MED). The `OWNER TO` and `RENAME` forms are PostgreSQL extensions. 

## See Also

[CREATE SERVER](sql-createserver.md "CREATE SERVER"), [DROP SERVER](sql-dropserver.md "DROP SERVER")

* * *

[Prev](sql-altersequence.md "ALTER SEQUENCE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterstatistics.md "ALTER STATISTICS")  
---|---|---  
ALTER SEQUENCE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER STATISTICS
