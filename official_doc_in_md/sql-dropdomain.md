DROP DOMAIN  
---  
[Prev](sql-dropdatabase.md "DROP DATABASE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropeventtrigger.md "DROP EVENT TRIGGER")  
  
* * *

## DROP DOMAIN

DROP DOMAIN — remove a domain

## Synopsis
    
    
    DROP DOMAIN [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP DOMAIN` removes a domain. Only the owner of a domain can remove it. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the domain does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of an existing domain. 

`CASCADE`
    

Automatically drop objects that depend on the domain (such as table columns), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the domain if any objects depend on it. This is the default. 

## Examples

To remove the domain `box`: 
    
    
    DROP DOMAIN box;
    

## Compatibility

This command conforms to the SQL standard, except for the `IF EXISTS` option, which is a PostgreSQL extension. 

## See Also

[CREATE DOMAIN](sql-createdomain.md "CREATE DOMAIN"), [ALTER DOMAIN](sql-alterdomain.md "ALTER DOMAIN")

* * *

[Prev](sql-dropdatabase.md "DROP DATABASE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropeventtrigger.md "DROP EVENT TRIGGER")  
---|---|---  
DROP DATABASE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP EVENT TRIGGER
