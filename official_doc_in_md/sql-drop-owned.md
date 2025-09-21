DROP OWNED  
---  
[Prev](sql-dropopfamily.md "DROP OPERATOR FAMILY") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droppolicy.md "DROP POLICY")  
  
* * *

## DROP OWNED

DROP OWNED — remove database objects owned by a database role

## Synopsis
    
    
    DROP OWNED BY { _name_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER } [, ...] [ CASCADE | RESTRICT ]
    

## Description

`DROP OWNED` drops all the objects within the current database that are owned by one of the specified roles. Any privileges granted to the given roles on objects in the current database or on shared objects (databases, tablespaces, configuration parameters) will also be revoked. 

## Parameters

 _`name`_
    

The name of a role whose objects will be dropped, and whose privileges will be revoked. 

`CASCADE`
    

Automatically drop objects that depend on the affected objects, and in turn all objects that depend on those objects (see [Section 5.15](ddl-depend.md "5.15. Dependency Tracking")). 

`RESTRICT`
    

Refuse to drop the objects owned by a role if any other database objects depend on one of the affected objects. This is the default. 

## Notes

`DROP OWNED` is often used to prepare for the removal of one or more roles. Because `DROP OWNED` only affects the objects in the current database, it is usually necessary to execute this command in each database that contains objects owned by a role that is to be removed. 

Using the `CASCADE` option might make the command recurse to objects owned by other users. 

The [`REASSIGN OWNED`](sql-reassign-owned.md "REASSIGN OWNED") command is an alternative that reassigns the ownership of all the database objects owned by one or more roles. However, `REASSIGN OWNED` does not deal with privileges for other objects. 

Databases and tablespaces owned by the role(s) will not be removed. 

See [Section 21.4](role-removal.md "21.4. Dropping Roles") for more discussion. 

## Compatibility

The `DROP OWNED` command is a PostgreSQL extension. 

## See Also

[REASSIGN OWNED](sql-reassign-owned.md "REASSIGN OWNED"), [DROP ROLE](sql-droprole.md "DROP ROLE")

* * *

[Prev](sql-dropopfamily.md "DROP OPERATOR FAMILY") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droppolicy.md "DROP POLICY")  
---|---|---  
DROP OPERATOR FAMILY | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP POLICY
