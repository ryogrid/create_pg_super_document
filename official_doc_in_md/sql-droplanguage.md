DROP LANGUAGE  
---  
[Prev](sql-dropindex.md "DROP INDEX") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropmaterializedview.md "DROP MATERIALIZED VIEW")  
  
* * *

## DROP LANGUAGE

DROP LANGUAGE — remove a procedural language

## Synopsis
    
    
    DROP [ PROCEDURAL ] LANGUAGE [ IF EXISTS ] _name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP LANGUAGE` removes the definition of a previously registered procedural language. You must be a superuser or the owner of the language to use `DROP LANGUAGE`. 

### Note

As of PostgreSQL 9.1, most procedural languages have been made into “extensions”, and should therefore be removed with [`DROP EXTENSION`](sql-dropextension.md "DROP EXTENSION") not `DROP LANGUAGE`. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the language does not exist. A notice is issued in this case. 

_`name`_
    

The name of an existing procedural language. 

`CASCADE`
    

Automatically drop objects that depend on the language (such as functions in the language), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the language if any objects depend on it. This is the default. 

## Examples

This command removes the procedural language `plsample`: 
    
    
    DROP LANGUAGE plsample;
    

## Compatibility

There is no `DROP LANGUAGE` statement in the SQL standard. 

## See Also

[ALTER LANGUAGE](sql-alterlanguage.md "ALTER LANGUAGE"), [CREATE LANGUAGE](sql-createlanguage.md "CREATE LANGUAGE")

* * *

[Prev](sql-dropindex.md "DROP INDEX") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropmaterializedview.md "DROP MATERIALIZED VIEW")  
---|---|---  
DROP INDEX | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP MATERIALIZED VIEW
