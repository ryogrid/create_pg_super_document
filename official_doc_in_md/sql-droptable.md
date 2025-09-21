DROP TABLE  
---  
[Prev](sql-dropsubscription.md "DROP SUBSCRIPTION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droptablespace.md "DROP TABLESPACE")  
  
* * *

## DROP TABLE

DROP TABLE — remove a table

## Synopsis
    
    
    DROP TABLE [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP TABLE` removes tables from the database. Only the table owner, the schema owner, and superuser can drop a table. To empty a table of rows without destroying the table, use [`DELETE`](sql-delete.md "DELETE") or [`TRUNCATE`](sql-truncate.md "TRUNCATE"). 

`DROP TABLE` always removes any indexes, rules, triggers, and constraints that exist for the target table. However, to drop a table that is referenced by a view or a foreign-key constraint of another table, `CASCADE` must be specified. (`CASCADE` will remove a dependent view entirely, but in the foreign-key case it will only remove the foreign-key constraint, not the other table entirely.) 

## Parameters

`IF EXISTS`
    

Do not throw an error if the table does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of the table to drop. 

`CASCADE`
    

Automatically drop objects that depend on the table (such as views), and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the table if any objects depend on it. This is the default. 

## Examples

To destroy two tables, `films` and `distributors`: 
    
    
    DROP TABLE films, distributors;
    

## Compatibility

This command conforms to the SQL standard, except that the standard only allows one table to be dropped per command, and apart from the `IF EXISTS` option, which is a PostgreSQL extension. 

## See Also

[ALTER TABLE](sql-altertable.md "ALTER TABLE"), [CREATE TABLE](sql-createtable.md "CREATE TABLE")

* * *

[Prev](sql-dropsubscription.md "DROP SUBSCRIPTION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droptablespace.md "DROP TABLESPACE")  
---|---|---  
DROP SUBSCRIPTION | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP TABLESPACE
