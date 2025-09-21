ALTER MATERIALIZED VIEW  
---  
[Prev](sql-alterlargeobject.md "ALTER LARGE OBJECT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alteroperator.md "ALTER OPERATOR")  
  
* * *

## ALTER MATERIALIZED VIEW

ALTER MATERIALIZED VIEW — change the definition of a materialized view

## Synopsis
    
    
    ALTER MATERIALIZED VIEW [ IF EXISTS ] _name_
        _action_ [, ... ]
    ALTER MATERIALIZED VIEW _name_
        [ NO ] DEPENDS ON EXTENSION _extension_name_
    ALTER MATERIALIZED VIEW [ IF EXISTS ] _name_
        RENAME [ COLUMN ] _column_name_ TO _new_column_name_
    ALTER MATERIALIZED VIEW [ IF EXISTS ] _name_
        RENAME TO _new_name_
    ALTER MATERIALIZED VIEW [ IF EXISTS ] _name_
        SET SCHEMA _new_schema_
    ALTER MATERIALIZED VIEW ALL IN TABLESPACE _name_ [ OWNED BY _role_name_ [, ... ] ]
        SET TABLESPACE _new_tablespace_ [ NOWAIT ]
    
    where _action_ is one of:
    
        ALTER [ COLUMN ] _column_name_ SET STATISTICS _integer_
        ALTER [ COLUMN ] _column_name_ SET ( _attribute_option_ = _value_ [, ... ] )
        ALTER [ COLUMN ] _column_name_ RESET ( _attribute_option_ [, ... ] )
        ALTER [ COLUMN ] _column_name_ SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN | DEFAULT }
        ALTER [ COLUMN ] _column_name_ SET COMPRESSION _compression_method_
        CLUSTER ON _index_name_
        SET WITHOUT CLUSTER
        SET ACCESS METHOD _new_access_method_
        SET TABLESPACE _new_tablespace_
        SET ( _storage_parameter_ [= _value_] [, ... ] )
        RESET ( _storage_parameter_ [, ... ] )
        OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    

## Description

`ALTER MATERIALIZED VIEW` changes various auxiliary properties of an existing materialized view. 

You must own the materialized view to use `ALTER MATERIALIZED VIEW`. To change a materialized view's schema, you must also have `CREATE` privilege on the new schema. To alter the owner, you must be able to `SET ROLE` to the new owning role, and that role must have `CREATE` privilege on the materialized view's schema. (These restrictions enforce that altering the owner doesn't do anything you couldn't do by dropping and recreating the materialized view. However, a superuser can alter ownership of any view anyway.) 

The statement subforms and actions available for `ALTER MATERIALIZED VIEW` are a subset of those available for `ALTER TABLE`, and have the same meaning when used for materialized views. See the descriptions for [`ALTER TABLE`](sql-altertable.md "ALTER TABLE") for details. 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing materialized view. 

_`column_name`_
    

Name of an existing column. 

_`extension_name`_
    

The name of the extension that the materialized view is to depend on (or no longer dependent on, if `NO` is specified). A materialized view that's marked as dependent on an extension is automatically dropped when the extension is dropped. 

_`new_column_name`_
    

New name for an existing column. 

_`new_owner`_
    

The user name of the new owner of the materialized view. 

_`new_name`_
    

The new name for the materialized view. 

_`new_schema`_
    

The new schema for the materialized view. 

## Examples

To rename the materialized view `foo` to `bar`: 
    
    
    ALTER MATERIALIZED VIEW foo RENAME TO bar;
    

## Compatibility

`ALTER MATERIALIZED VIEW` is a PostgreSQL extension. 

## See Also

[CREATE MATERIALIZED VIEW](sql-creatematerializedview.md "CREATE MATERIALIZED VIEW"), [DROP MATERIALIZED VIEW](sql-dropmaterializedview.md "DROP MATERIALIZED VIEW"), [REFRESH MATERIALIZED VIEW](sql-refreshmaterializedview.md "REFRESH MATERIALIZED VIEW")

* * *

[Prev](sql-alterlargeobject.md "ALTER LARGE OBJECT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alteroperator.md "ALTER OPERATOR")  
---|---|---  
ALTER LARGE OBJECT | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER OPERATOR
