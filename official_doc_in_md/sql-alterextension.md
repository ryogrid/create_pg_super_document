ALTER EXTENSION  
---  
[Prev](sql-altereventtrigger.md "ALTER EVENT TRIGGER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterforeigndatawrapper.md "ALTER FOREIGN DATA WRAPPER")  
  
* * *

## ALTER EXTENSION

ALTER EXTENSION — change the definition of an extension 

## Synopsis
    
    
    ALTER EXTENSION _name_ UPDATE [ TO _new_version_ ]
    ALTER EXTENSION _name_ SET SCHEMA _new_schema_
    ALTER EXTENSION _name_ ADD _member_object_
    ALTER EXTENSION _name_ DROP _member_object_
    
    where _member_object_ is:
    
      ACCESS METHOD _object_name_ |
      AGGREGATE _aggregate_name_ ( _aggregate_signature_ ) |
      CAST (_source_type_ AS _target_type_) |
      COLLATION _object_name_ |
      CONVERSION _object_name_ |
      DOMAIN _object_name_ |
      EVENT TRIGGER _object_name_ |
      FOREIGN DATA WRAPPER _object_name_ |
      FOREIGN TABLE _object_name_ |
      FUNCTION _function_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      MATERIALIZED VIEW _object_name_ |
      OPERATOR _operator_name_ (_left_type_ , _right_type_) |
      OPERATOR CLASS _object_name_ USING _index_method_ |
      OPERATOR FAMILY _object_name_ USING _index_method_ |
      [ PROCEDURAL ] LANGUAGE _object_name_ |
      PROCEDURE _procedure_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      ROUTINE _routine_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      SCHEMA _object_name_ |
      SEQUENCE _object_name_ |
      SERVER _object_name_ |
      TABLE _object_name_ |
      TEXT SEARCH CONFIGURATION _object_name_ |
      TEXT SEARCH DICTIONARY _object_name_ |
      TEXT SEARCH PARSER _object_name_ |
      TEXT SEARCH TEMPLATE _object_name_ |
      TRANSFORM FOR _type_name_ LANGUAGE _lang_name_ |
      TYPE _object_name_ |
      VIEW _object_name_
    
    and _aggregate_signature_ is:
    
    * |
    [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] |
    [ [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] ] ORDER BY [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ]
    

## Description

`ALTER EXTENSION` changes the definition of an installed extension. There are several subforms: 

`UPDATE`
    

This form updates the extension to a newer version. The extension must supply a suitable update script (or series of scripts) that can modify the currently-installed version into the requested version. 

`SET SCHEMA`
    

This form moves the extension's objects into another schema. The extension has to be _relocatable_ for this command to succeed. 

`ADD _`member_object`_`
    

This form adds an existing object to the extension. This is mainly useful in extension update scripts. The object will subsequently be treated as a member of the extension; notably, it can only be dropped by dropping the extension. 

`DROP _`member_object`_`
    

This form removes a member object from the extension. This is mainly useful in extension update scripts. The object is not dropped, only disassociated from the extension. 

See [Section 36.17](extend-extensions.md "36.17. Packaging Related Objects into an Extension") for more information about these operations. 

You must own the extension to use `ALTER EXTENSION`. The `ADD`/`DROP` forms require ownership of the added/dropped object as well. 

## Parameters

_`name`_
    

The name of an installed extension. 

_`new_version`_
    

The desired new version of the extension. This can be written as either an identifier or a string literal. If not specified, `ALTER EXTENSION UPDATE` attempts to update to whatever is shown as the default version in the extension's control file. 

_`new_schema`_
    

The new schema for the extension. 

_`object_name`_  
 _`aggregate_name`_  
 _`function_name`_  
 _`operator_name`_  
 _`procedure_name`_  
 _`routine_name`_
    

The name of an object to be added to or removed from the extension. Names of tables, aggregates, domains, foreign tables, functions, operators, operator classes, operator families, procedures, routines, sequences, text search objects, types, and views can be schema-qualified. 

_`source_type`_
    

The name of the source data type of the cast. 

_`target_type`_
    

The name of the target data type of the cast. 

_`argmode`_
    

The mode of a function, procedure, or aggregate argument: `IN`, `OUT`, `INOUT`, or `VARIADIC`. If omitted, the default is `IN`. Note that `ALTER EXTENSION` does not actually pay any attention to `OUT` arguments, since only the input arguments are needed to determine the function's identity. So it is sufficient to list the `IN`, `INOUT`, and `VARIADIC` arguments. 

_`argname`_
    

The name of a function, procedure, or aggregate argument. Note that `ALTER EXTENSION` does not actually pay any attention to argument names, since only the argument data types are needed to determine the function's identity. 

_`argtype`_
    

The data type of a function, procedure, or aggregate argument. 

_`left_type`_  
 _`right_type`_
    

The data type(s) of the operator's arguments (optionally schema-qualified). Write `NONE` for the missing argument of a prefix operator. 

`PROCEDURAL`
    

This is a noise word. 

_`type_name`_
    

The name of the data type of the transform. 

_`lang_name`_
    

The name of the language of the transform. 

## Examples

To update the `hstore` extension to version 2.0: 
    
    
    ALTER EXTENSION hstore UPDATE TO '2.0';
    

To change the schema of the `hstore` extension to `utils`: 
    
    
    ALTER EXTENSION hstore SET SCHEMA utils;
    

To add an existing function to the `hstore` extension: 
    
    
    ALTER EXTENSION hstore ADD FUNCTION populate_record(anyelement, hstore);
    

## Compatibility

`ALTER EXTENSION` is a PostgreSQL extension. 

## See Also

[CREATE EXTENSION](sql-createextension.md "CREATE EXTENSION"), [DROP EXTENSION](sql-dropextension.md "DROP EXTENSION")

* * *

[Prev](sql-altereventtrigger.md "ALTER EVENT TRIGGER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterforeigndatawrapper.md "ALTER FOREIGN DATA WRAPPER")  
---|---|---  
ALTER EVENT TRIGGER | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER FOREIGN DATA WRAPPER
