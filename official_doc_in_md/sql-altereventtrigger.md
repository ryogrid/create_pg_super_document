ALTER EVENT TRIGGER  
---  
[Prev](sql-alterdomain.md "ALTER DOMAIN") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterextension.md "ALTER EXTENSION")  
  
* * *

## ALTER EVENT TRIGGER

ALTER EVENT TRIGGER — change the definition of an event trigger

## Synopsis
    
    
    ALTER EVENT TRIGGER _name_ DISABLE
    ALTER EVENT TRIGGER _name_ ENABLE [ REPLICA | ALWAYS ]
    ALTER EVENT TRIGGER _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER EVENT TRIGGER _name_ RENAME TO _new_name_
    

## Description

`ALTER EVENT TRIGGER` changes properties of an existing event trigger. 

You must be superuser to alter an event trigger. 

## Parameters

 _`name`_
    

The name of an existing trigger to alter. 

_`new_owner`_
    

The user name of the new owner of the event trigger. 

_`new_name`_
    

The new name of the event trigger. 

`DISABLE`/`ENABLE [ REPLICA | ALWAYS ]`
    

These forms configure the firing of event triggers. A disabled trigger is still known to the system, but is not executed when its triggering event occurs. See also [session_replication_role](runtime-config-client.md#GUC-SESSION-REPLICATION-ROLE). 

## Compatibility

There is no `ALTER EVENT TRIGGER` statement in the SQL standard. 

## See Also

[CREATE EVENT TRIGGER](sql-createeventtrigger.md "CREATE EVENT TRIGGER"), [DROP EVENT TRIGGER](sql-dropeventtrigger.md "DROP EVENT TRIGGER")

* * *

[Prev](sql-alterdomain.md "ALTER DOMAIN") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterextension.md "ALTER EXTENSION")  
---|---|---  
ALTER DOMAIN | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER EXTENSION
