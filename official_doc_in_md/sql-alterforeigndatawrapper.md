ALTER FOREIGN DATA WRAPPER  
---  
[Prev](sql-alterextension.md "ALTER EXTENSION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterforeigntable.md "ALTER FOREIGN TABLE")  
  
* * *

## ALTER FOREIGN DATA WRAPPER

ALTER FOREIGN DATA WRAPPER — change the definition of a foreign-data wrapper

## Synopsis
    
    
    ALTER FOREIGN DATA WRAPPER _name_
        [ HANDLER _handler_function_ | NO HANDLER ]
        [ VALIDATOR _validator_function_ | NO VALIDATOR ]
        [ OPTIONS ( [ ADD | SET | DROP ] _option_ ['_value_ '] [, ... ]) ]
    ALTER FOREIGN DATA WRAPPER _name_ OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    ALTER FOREIGN DATA WRAPPER _name_ RENAME TO _new_name_
    

## Description

`ALTER FOREIGN DATA WRAPPER` changes the definition of a foreign-data wrapper. The first form of the command changes the support functions or the generic options of the foreign-data wrapper (at least one clause is required). The second form changes the owner of the foreign-data wrapper. 

Only superusers can alter foreign-data wrappers. Additionally, only superusers can own foreign-data wrappers. 

## Parameters

 _`name`_
    

The name of an existing foreign-data wrapper. 

`HANDLER _`handler_function`_`
    

Specifies a new handler function for the foreign-data wrapper. 

`NO HANDLER`
    

This is used to specify that the foreign-data wrapper should no longer have a handler function. 

Note that foreign tables that use a foreign-data wrapper with no handler cannot be accessed. 

`VALIDATOR _`validator_function`_`
    

Specifies a new validator function for the foreign-data wrapper. 

Note that it is possible that pre-existing options of the foreign-data wrapper, or of dependent servers, user mappings, or foreign tables, are invalid according to the new validator. PostgreSQL does not check for this. It is up to the user to make sure that these options are correct before using the modified foreign-data wrapper. However, any options specified in this `ALTER FOREIGN DATA WRAPPER` command will be checked using the new validator. 

`NO VALIDATOR`
    

This is used to specify that the foreign-data wrapper should no longer have a validator function. 

`OPTIONS ( [ ADD | SET | DROP ] _`option`_ ['_`value`_ '] [, ... ] )`
    

Change options for the foreign-data wrapper. `ADD`, `SET`, and `DROP` specify the action to be performed. `ADD` is assumed if no operation is explicitly specified. Option names must be unique; names and values are also validated using the foreign data wrapper's validator function, if any. 

_`new_owner`_
    

The user name of the new owner of the foreign-data wrapper. 

_`new_name`_
    

The new name for the foreign-data wrapper. 

## Examples

Change a foreign-data wrapper `dbi`, add option `foo`, drop `bar`: 
    
    
    ALTER FOREIGN DATA WRAPPER dbi OPTIONS (ADD foo '1', DROP bar);
    

Change the foreign-data wrapper `dbi` validator to `bob.myvalidator`: 
    
    
    ALTER FOREIGN DATA WRAPPER dbi VALIDATOR bob.myvalidator;
    

## Compatibility

`ALTER FOREIGN DATA WRAPPER` conforms to ISO/IEC 9075-9 (SQL/MED), except that the `HANDLER`, `VALIDATOR`, `OWNER TO`, and `RENAME` clauses are extensions. 

## See Also

[CREATE FOREIGN DATA WRAPPER](sql-createforeigndatawrapper.md "CREATE FOREIGN DATA WRAPPER"), [DROP FOREIGN DATA WRAPPER](sql-dropforeigndatawrapper.md "DROP FOREIGN DATA WRAPPER")

* * *

[Prev](sql-alterextension.md "ALTER EXTENSION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterforeigntable.md "ALTER FOREIGN TABLE")  
---|---|---  
ALTER EXTENSION | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER FOREIGN TABLE
