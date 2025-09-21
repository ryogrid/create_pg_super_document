SECURITY LABEL  
---  
[Prev](sql-savepoint.md "SAVEPOINT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-select.md "SELECT")  
  
* * *

## SECURITY LABEL

SECURITY LABEL — define or change a security label applied to an object

## Synopsis
    
    
    SECURITY LABEL [ FOR _provider_ ] ON
    {
      TABLE _object_name_ |
      COLUMN _table_name_._column_name_ |
      AGGREGATE _aggregate_name_ ( _aggregate_signature_ ) |
      DATABASE _object_name_ |
      DOMAIN _object_name_ |
      EVENT TRIGGER _object_name_ |
      FOREIGN TABLE _object_name_ |
      FUNCTION _function_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      LARGE OBJECT _large_object_oid_ |
      MATERIALIZED VIEW _object_name_ |
      [ PROCEDURAL ] LANGUAGE _object_name_ |
      PROCEDURE _procedure_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      PUBLICATION _object_name_ |
      ROLE _object_name_ |
      ROUTINE _routine_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      SCHEMA _object_name_ |
      SEQUENCE _object_name_ |
      SUBSCRIPTION _object_name_ |
      TABLESPACE _object_name_ |
      TYPE _object_name_ |
      VIEW _object_name_
    } IS { _string_literal_ | NULL }
    
    where _aggregate_signature_ is:
    
    * |
    [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] |
    [ [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] ] ORDER BY [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ]
    

## Description

`SECURITY LABEL` applies a security label to a database object. An arbitrary number of security labels, one per label provider, can be associated with a given database object. Label providers are loadable modules which register themselves by using the function `register_label_provider`. 

### Note

`register_label_provider` is not an SQL function; it can only be called from C code loaded into the backend. 

The label provider determines whether a given label is valid and whether it is permissible to assign that label to a given object. The meaning of a given label is likewise at the discretion of the label provider. PostgreSQL places no restrictions on whether or how a label provider must interpret security labels; it merely provides a mechanism for storing them. In practice, this facility is intended to allow integration with label-based mandatory access control (MAC) systems such as SELinux. Such systems make all access control decisions based on object labels, rather than traditional discretionary access control (DAC) concepts such as users and groups. 

You must own the database object to use `SECURITY LABEL`. 

## Parameters

 _`object_name`_  
 _`table_name.column_name`_  
 _`aggregate_name`_  
 _`function_name`_  
 _`procedure_name`_  
 _`routine_name`_
    

The name of the object to be labeled. Names of objects that reside in schemas (tables, functions, etc.) can be schema-qualified. 

_`provider`_
    

The name of the provider with which this label is to be associated. The named provider must be loaded and must consent to the proposed labeling operation. If exactly one provider is loaded, the provider name may be omitted for brevity. 

_`argmode`_
    

The mode of a function, procedure, or aggregate argument: `IN`, `OUT`, `INOUT`, or `VARIADIC`. If omitted, the default is `IN`. Note that `SECURITY LABEL` does not actually pay any attention to `OUT` arguments, since only the input arguments are needed to determine the function's identity. So it is sufficient to list the `IN`, `INOUT`, and `VARIADIC` arguments. 

_`argname`_
    

The name of a function, procedure, or aggregate argument. Note that `SECURITY LABEL` does not actually pay any attention to argument names, since only the argument data types are needed to determine the function's identity. 

_`argtype`_
    

The data type of a function, procedure, or aggregate argument. 

_`large_object_oid`_
    

The OID of the large object. 

`PROCEDURAL`
    

This is a noise word. 

_`string_literal`_
    

The new setting of the security label, written as a string literal. 

`NULL`
    

Write `NULL` to drop the security label. 

## Examples

The following example shows how the security label of a table could be set or changed: 
    
    
    SECURITY LABEL FOR selinux ON TABLE mytable IS 'system_u:object_r:sepgsql_table_t:s0';
    

To remove the label: 
    
    
    SECURITY LABEL FOR selinux ON TABLE mytable IS NULL;
    

## Compatibility

There is no `SECURITY LABEL` command in the SQL standard. 

## See Also

[sepgsql](sepgsql.md "F.38. sepgsql — SELinux-, label-based mandatory access control \(MAC\) security module"), `src/test/modules/dummy_seclabel`

* * *

[Prev](sql-savepoint.md "SAVEPOINT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-select.md "SELECT")  
---|---|---  
SAVEPOINT | [Home](index.md "PostgreSQL 17.5 Documentation")|  SELECT
