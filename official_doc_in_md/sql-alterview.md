ALTER VIEW  
---  
[Prev](sql-alterusermapping.md "ALTER USER MAPPING") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-analyze.md "ANALYZE")  
  
* * *

## ALTER VIEW

ALTER VIEW — change the definition of a view

## Synopsis
    
    
    ALTER VIEW [ IF EXISTS ] _name_ ALTER [ COLUMN ] _column_name_ SET DEFAULT _expression_
    ALTER VIEW [ IF EXISTS ] _name_ ALTER [ COLUMN ] _column_name_ DROP DEFAULT
    ALTER VIEW [ IF EXISTS ] _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER VIEW [ IF EXISTS ] _name_ RENAME [ COLUMN ] _column_name_ TO _new_column_name_
    ALTER VIEW [ IF EXISTS ] _name_ RENAME TO _new_name_
    ALTER VIEW [ IF EXISTS ] _name_ SET SCHEMA _new_schema_
    ALTER VIEW [ IF EXISTS ] _name_ SET ( _view_option_name_ [= _view_option_value_] [, ... ] )
    ALTER VIEW [ IF EXISTS ] _name_ RESET ( _view_option_name_ [, ... ] )
    

## Description

`ALTER VIEW` changes various auxiliary properties of a view. (If you want to modify the view's defining query, use `CREATE OR REPLACE VIEW`.) 

You must own the view to use `ALTER VIEW`. To change a view's schema, you must also have `CREATE` privilege on the new schema. To alter the owner, you must be able to `SET ROLE` to the new owning role, and that role must have `CREATE` privilege on the view's schema. (These restrictions enforce that altering the owner doesn't do anything you couldn't do by dropping and recreating the view. However, a superuser can alter ownership of any view anyway.) 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing view. 

_`column_name`_
    

Name of an existing column. 

_`new_column_name`_
    

New name for an existing column. 

`IF EXISTS`
    

Do not throw an error if the view does not exist. A notice is issued in this case. 

`SET`/`DROP DEFAULT`
    

These forms set or remove the default value for a column. A view column's default value is substituted into any `INSERT` or `UPDATE` command whose target is the view, before applying any rules or triggers for the view. The view's default will therefore take precedence over any default values from underlying relations. 

_`new_owner`_
    

The user name of the new owner of the view. 

_`new_name`_
    

The new name for the view. 

_`new_schema`_
    

The new schema for the view. 

`SET ( _`view_option_name`_ [= _`view_option_value`_] [, ... ] )`  
`RESET ( _`view_option_name`_ [, ... ] )`
    

Sets or resets a view option. Currently supported options are: 

`check_option` (`enum`)
    

Changes the check option of the view. The value must be `local` or `cascaded`. 

`security_barrier` (`boolean`)
    

Changes the security-barrier property of the view. The value must be a Boolean value, such as `true` or `false`. 

`security_invoker` (`boolean`)
    

Changes the security-invoker property of the view. The value must be a Boolean value, such as `true` or `false`. 

## Notes

For historical reasons, `ALTER TABLE` can be used with views too; but the only variants of `ALTER TABLE` that are allowed with views are equivalent to the ones shown above. 

## Examples

To rename the view `foo` to `bar`: 
    
    
    ALTER VIEW foo RENAME TO bar;
    

To attach a default column value to an updatable view: 
    
    
    CREATE TABLE base_table (id int, ts timestamptz);
    CREATE VIEW a_view AS SELECT * FROM base_table;
    ALTER VIEW a_view ALTER COLUMN ts SET DEFAULT now();
    INSERT INTO base_table(id) VALUES(1);  -- ts will receive a NULL
    INSERT INTO a_view(id) VALUES(2);  -- ts will receive the current time
    

## Compatibility

`ALTER VIEW` is a PostgreSQL extension of the SQL standard. 

## See Also

[CREATE VIEW](sql-createview.md "CREATE VIEW"), [DROP VIEW](sql-dropview.md "DROP VIEW")

* * *

[Prev](sql-alterusermapping.md "ALTER USER MAPPING") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-analyze.md "ANALYZE")  
---|---|---  
ALTER USER MAPPING | [Home](index.md "PostgreSQL 17.5 Documentation")|  ANALYZE
