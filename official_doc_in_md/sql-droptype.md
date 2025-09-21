DROP TYPE  
---  
[Prev](sql-droptrigger.md "DROP TRIGGER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropuser.md "DROP USER")  
  
* * *

## DROP TYPE

DROP TYPE — remove a data type

## Synopsis
    
    
    DROP TYPE [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP TYPE` removes a user-defined data type. Only the owner of a type can remove it. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the type does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of the data type to remove. 

`CASCADE`
    

Automatically drop objects that depend on the type (such as table columns, functions, and operators), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the type if any objects depend on it. This is the default. 

## Examples

To remove the data type `box`: 
    
    
    DROP TYPE box;
    

## Compatibility

This command is similar to the corresponding command in the SQL standard, apart from the `IF EXISTS` option, which is a PostgreSQL extension. But note that much of the `CREATE TYPE` command and the data type extension mechanisms in PostgreSQL differ from the SQL standard. 

## See Also

[ALTER TYPE](sql-altertype.md "ALTER TYPE"), [CREATE TYPE](sql-createtype.md "CREATE TYPE")

* * *

[Prev](sql-droptrigger.md "DROP TRIGGER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropuser.md "DROP USER")  
---|---|---  
DROP TRIGGER | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP USER
