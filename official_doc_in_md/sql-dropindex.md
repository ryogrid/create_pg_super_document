DROP INDEX  
---  
[Prev](sql-dropgroup.md "DROP GROUP") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droplanguage.md "DROP LANGUAGE")  
  
* * *

## DROP INDEX

DROP INDEX — remove an index

## Synopsis
    
    
    DROP INDEX [ CONCURRENTLY ] [ IF EXISTS ] _name_ [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP INDEX` drops an existing index from the database system. To execute this command you must be the owner of the index. 

## Parameters

`CONCURRENTLY`
    

Drop the index without locking out concurrent selects, inserts, updates, and deletes on the index's table. A normal `DROP INDEX` acquires an `ACCESS EXCLUSIVE` lock on the table, blocking other accesses until the index drop can be completed. With this option, the command instead waits until conflicting transactions have completed. 

There are several caveats to be aware of when using this option. Only one index name can be specified, and the `CASCADE` option is not supported. (Thus, an index that supports a `UNIQUE` or `PRIMARY KEY` constraint cannot be dropped this way.) Also, regular `DROP INDEX` commands can be performed within a transaction block, but `DROP INDEX CONCURRENTLY` cannot. Lastly, indexes on partitioned tables cannot be dropped using this option. 

For temporary tables, `DROP INDEX` is always non-concurrent, as no other session can access them, and non-concurrent index drop is cheaper. 

`IF EXISTS`
    

Do not throw an error if the index does not exist. A notice is issued in this case. 

_`name`_
    

The name (optionally schema-qualified) of an index to remove. 

`CASCADE`
    

Automatically drop objects that depend on the index, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the index if any objects depend on it. This is the default. 

## Examples

This command will remove the index `title_idx`: 
    
    
    DROP INDEX title_idx;
    

## Compatibility

`DROP INDEX` is a PostgreSQL language extension. There are no provisions for indexes in the SQL standard. 

## See Also

[CREATE INDEX](sql-createindex.md "CREATE INDEX")

* * *

[Prev](sql-dropgroup.md "DROP GROUP") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droplanguage.md "DROP LANGUAGE")  
---|---|---  
DROP GROUP | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP LANGUAGE
