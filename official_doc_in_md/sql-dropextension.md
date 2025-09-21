DROP EXTENSION  
---  
[Prev](sql-dropeventtrigger.md "DROP EVENT TRIGGER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropforeigndatawrapper.md "DROP FOREIGN DATA WRAPPER")  
  
* * *

## DROP EXTENSION

DROP EXTENSION — remove an extension

## Synopsis
    
    
    DROP EXTENSION [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP EXTENSION` removes extensions from the database. Dropping an extension causes its member objects, and other explicitly dependent routines (see [ALTER ROUTINE](sql-alterroutine.md "ALTER ROUTINE"), the `DEPENDS ON EXTENSION _`extension_name`_ ` action), to be dropped as well. 

You must own the extension to use `DROP EXTENSION`. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the extension does not exist. A notice is issued in this case. 

_`name`_
    

The name of an installed extension. 

`CASCADE`
    

Automatically drop objects that depend on the extension, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

This option prevents the specified extensions from being dropped if other objects, besides these extensions, their members, and their explicitly dependent routines, depend on them. This is the default. 

## Examples

To remove the extension `hstore` from the current database: 
    
    
    DROP EXTENSION hstore;
    

This command will fail if any of `hstore`'s objects are in use in the database, for example if any tables have columns of the `hstore` type. Add the `CASCADE` option to forcibly remove those dependent objects as well. 

## Compatibility

`DROP EXTENSION` is a PostgreSQL extension. 

## See Also

[CREATE EXTENSION](sql-createextension.md "CREATE EXTENSION"), [ALTER EXTENSION](sql-alterextension.md "ALTER EXTENSION")

* * *

[Prev](sql-dropeventtrigger.md "DROP EVENT TRIGGER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropforeigndatawrapper.md "DROP FOREIGN DATA WRAPPER")  
---|---|---  
DROP EVENT TRIGGER | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP FOREIGN DATA WRAPPER
