DROP EVENT TRIGGER  
---  
[Prev](sql-dropdomain.md "DROP DOMAIN") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropextension.md "DROP EXTENSION")  
  
* * *

## DROP EVENT TRIGGER

DROP EVENT TRIGGER — remove an event trigger

## Synopsis
    
    
    DROP EVENT TRIGGER [ IF EXISTS ] _name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP EVENT TRIGGER` removes an existing event trigger. To execute this command, the current user must be the owner of the event trigger. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the event trigger does not exist. A notice is issued in this case. 

_`name`_
    

The name of the event trigger to remove. 

`CASCADE`
    

Automatically drop objects that depend on the trigger, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the trigger if any objects depend on it. This is the default. 

## Examples

Destroy the trigger `snitch`: 
    
    
    DROP EVENT TRIGGER snitch;
    

## Compatibility

There is no `DROP EVENT TRIGGER` statement in the SQL standard. 

## See Also

[CREATE EVENT TRIGGER](sql-createeventtrigger.md "CREATE EVENT TRIGGER"), [ALTER EVENT TRIGGER](sql-altereventtrigger.md "ALTER EVENT TRIGGER")

* * *

[Prev](sql-dropdomain.md "DROP DOMAIN") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropextension.md "DROP EXTENSION")  
---|---|---  
DROP DOMAIN | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP EXTENSION
