COMMENT  
---  
[Prev](sql-cluster.md "CLUSTER") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-commit.md "COMMIT")  
  
* * *

## COMMENT

COMMENT — define or change the comment of an object

## Synopsis
    
    
    COMMENT ON
    {
      ACCESS METHOD _object_name_ |
      AGGREGATE _aggregate_name_ ( _aggregate_signature_ ) |
      CAST (_source_type_ AS _target_type_) |
      COLLATION _object_name_ |
      COLUMN _relation_name_._column_name_ |
      CONSTRAINT _constraint_name_ ON _table_name_ |
      CONSTRAINT _constraint_name_ ON DOMAIN _domain_name_ |
      CONVERSION _object_name_ |
      DATABASE _object_name_ |
      DOMAIN _object_name_ |
      EXTENSION _object_name_ |
      EVENT TRIGGER _object_name_ |
      FOREIGN DATA WRAPPER _object_name_ |
      FOREIGN TABLE _object_name_ |
      FUNCTION _function_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      INDEX _object_name_ |
      LARGE OBJECT _large_object_oid_ |
      MATERIALIZED VIEW _object_name_ |
      OPERATOR _operator_name_ (_left_type_ , _right_type_) |
      OPERATOR CLASS _object_name_ USING _index_method_ |
      OPERATOR FAMILY _object_name_ USING _index_method_ |
      POLICY _policy_name_ ON _table_name_ |
      [ PROCEDURAL ] LANGUAGE _object_name_ |
      PROCEDURE _procedure_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      PUBLICATION _object_name_ |
      ROLE _object_name_ |
      ROUTINE _routine_name_ [ ( [ [ _argmode_ ] [ _argname_ ] _argtype_ [, ...] ] ) ] |
      RULE _rule_name_ ON _table_name_ |
      SCHEMA _object_name_ |
      SEQUENCE _object_name_ |
      SERVER _object_name_ |
      STATISTICS _object_name_ |
      SUBSCRIPTION _object_name_ |
      TABLE _object_name_ |
      TABLESPACE _object_name_ |
      TEXT SEARCH CONFIGURATION _object_name_ |
      TEXT SEARCH DICTIONARY _object_name_ |
      TEXT SEARCH PARSER _object_name_ |
      TEXT SEARCH TEMPLATE _object_name_ |
      TRANSFORM FOR _type_name_ LANGUAGE _lang_name_ |
      TRIGGER _trigger_name_ ON _table_name_ |
      TYPE _object_name_ |
      VIEW _object_name_
    } IS { _string_literal_ | NULL }
    
    where _aggregate_signature_ is:
    
    * |
    [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] |
    [ [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ] ] ORDER BY [ _argmode_ ] [ _argname_ ] _argtype_ [ , ... ]
    

## Description

`COMMENT` stores a comment about a database object. 

Only one comment string is stored for each object, so to modify a comment, issue a new `COMMENT` command for the same object. To remove a comment, write `NULL` in place of the text string. Comments are automatically dropped when their object is dropped. 

A `SHARE UPDATE EXCLUSIVE` lock is acquired on the object to be commented. 

For most kinds of object, only the object's owner can set the comment. Roles don't have owners, so the rule for `COMMENT ON ROLE` is that you must be superuser to comment on a superuser role, or have the `CREATEROLE` privilege and have been granted `ADMIN OPTION` on the target role. Likewise, access methods don't have owners either; you must be superuser to comment on an access method. Of course, a superuser can comment on anything. 

Comments can be viewed using psql's `\d` family of commands. Other user interfaces to retrieve comments can be built atop the same built-in functions that psql uses, namely `obj_description`, `col_description`, and `shobj_description` (see [Table 9.80](functions-info.md#FUNCTIONS-INFO-COMMENT-TABLE "Table 9.80. Comment Information Functions")). 

## Parameters

 _`object_name`_  
 _`relation_name`_._`column_name`_  
 _`aggregate_name`_  
 _`constraint_name`_  
 _`function_name`_  
 _`operator_name`_  
 _`policy_name`_  
 _`procedure_name`_  
 _`routine_name`_  
 _`rule_name`_  
 _`trigger_name`_
    

The name of the object to be commented. Names of objects that reside in schemas (tables, functions, etc.) can be schema-qualified. When commenting on a column, _`relation_name`_ must refer to a table, view, composite type, or foreign table. 

_`table_name`_  
 _`domain_name`_
    

When creating a comment on a constraint, a trigger, a rule or a policy these parameters specify the name of the table or domain on which that object is defined. 

_`source_type`_
    

The name of the source data type of the cast. 

_`target_type`_
    

The name of the target data type of the cast. 

_`argmode`_
    

The mode of a function, procedure, or aggregate argument: `IN`, `OUT`, `INOUT`, or `VARIADIC`. If omitted, the default is `IN`. Note that `COMMENT` does not actually pay any attention to `OUT` arguments, since only the input arguments are needed to determine the function's identity. So it is sufficient to list the `IN`, `INOUT`, and `VARIADIC` arguments. 

_`argname`_
    

The name of a function, procedure, or aggregate argument. Note that `COMMENT` does not actually pay any attention to argument names, since only the argument data types are needed to determine the function's identity. 

_`argtype`_
    

The data type of a function, procedure, or aggregate argument. 

_`large_object_oid`_
    

The OID of the large object. 

_`left_type`_  
 _`right_type`_
    

The data type(s) of the operator's arguments (optionally schema-qualified). Write `NONE` for the missing argument of a prefix operator. 

`PROCEDURAL`
    

This is a noise word. 

_`type_name`_
    

The name of the data type of the transform. 

_`lang_name`_
    

The name of the language of the transform. 

_`string_literal`_
    

The new comment contents, written as a string literal. 

`NULL`
    

Write `NULL` to drop the comment. 

## Notes

There is presently no security mechanism for viewing comments: any user connected to a database can see all the comments for objects in that database. For shared objects such as databases, roles, and tablespaces, comments are stored globally so any user connected to any database in the cluster can see all the comments for shared objects. Therefore, don't put security-critical information in comments. 

## Examples

Attach a comment to the table `mytable`: 
    
    
    COMMENT ON TABLE mytable IS 'This is my table.';
    

Remove it again: 
    
    
    COMMENT ON TABLE mytable IS NULL;
    

Some more examples: 
    
    
    COMMENT ON ACCESS METHOD gin IS 'GIN index access method';
    COMMENT ON AGGREGATE my_aggregate (double precision) IS 'Computes sample variance';
    COMMENT ON CAST (text AS int4) IS 'Allow casts from text to int4';
    COMMENT ON COLLATION "fr_CA" IS 'Canadian French';
    COMMENT ON COLUMN my_table.my_column IS 'Employee ID number';
    COMMENT ON CONVERSION my_conv IS 'Conversion to UTF8';
    COMMENT ON CONSTRAINT bar_col_cons ON bar IS 'Constrains column col';
    COMMENT ON CONSTRAINT dom_col_constr ON DOMAIN dom IS 'Constrains col of domain';
    COMMENT ON DATABASE my_database IS 'Development Database';
    COMMENT ON DOMAIN my_domain IS 'Email Address Domain';
    COMMENT ON EVENT TRIGGER abort_ddl IS 'Aborts all DDL commands';
    COMMENT ON EXTENSION hstore IS 'implements the hstore data type';
    COMMENT ON FOREIGN DATA WRAPPER mywrapper IS 'my foreign data wrapper';
    COMMENT ON FOREIGN TABLE my_foreign_table IS 'Employee Information in other database';
    COMMENT ON FUNCTION my_function (timestamp) IS 'Returns Roman Numeral';
    COMMENT ON INDEX my_index IS 'Enforces uniqueness on employee ID';
    COMMENT ON LANGUAGE plpython IS 'Python support for stored procedures';
    COMMENT ON LARGE OBJECT 346344 IS 'Planning document';
    COMMENT ON MATERIALIZED VIEW my_matview IS 'Summary of order history';
    COMMENT ON OPERATOR ^ (text, text) IS 'Performs intersection of two texts';
    COMMENT ON OPERATOR - (NONE, integer) IS 'Unary minus';
    COMMENT ON OPERATOR CLASS int4ops USING btree IS '4 byte integer operators for btrees';
    COMMENT ON OPERATOR FAMILY integer_ops USING btree IS 'all integer operators for btrees';
    COMMENT ON POLICY my_policy ON mytable IS 'Filter rows by users';
    COMMENT ON PROCEDURE my_proc (integer, integer) IS 'Runs a report';
    COMMENT ON PUBLICATION alltables IS 'Publishes all operations on all tables';
    COMMENT ON ROLE my_role IS 'Administration group for finance tables';
    COMMENT ON ROUTINE my_routine (integer, integer) IS 'Runs a routine (which is a function or procedure)';
    COMMENT ON RULE my_rule ON my_table IS 'Logs updates of employee records';
    COMMENT ON SCHEMA my_schema IS 'Departmental data';
    COMMENT ON SEQUENCE my_sequence IS 'Used to generate primary keys';
    COMMENT ON SERVER myserver IS 'my foreign server';
    COMMENT ON STATISTICS my_statistics IS 'Improves planner row estimations';
    COMMENT ON SUBSCRIPTION alltables IS 'Subscription for all operations on all tables';
    COMMENT ON TABLE my_schema.my_table IS 'Employee Information';
    COMMENT ON TABLESPACE my_tablespace IS 'Tablespace for indexes';
    COMMENT ON TEXT SEARCH CONFIGURATION my_config IS 'Special word filtering';
    COMMENT ON TEXT SEARCH DICTIONARY swedish IS 'Snowball stemmer for Swedish language';
    COMMENT ON TEXT SEARCH PARSER my_parser IS 'Splits text into words';
    COMMENT ON TEXT SEARCH TEMPLATE snowball IS 'Snowball stemmer';
    COMMENT ON TRANSFORM FOR hstore LANGUAGE plpython3u IS 'Transform between hstore and Python dict';
    COMMENT ON TRIGGER my_trigger ON my_table IS 'Used for RI';
    COMMENT ON TYPE complex IS 'Complex number data type';
    COMMENT ON VIEW my_view IS 'View of departmental costs';
    

## Compatibility

There is no `COMMENT` command in the SQL standard. 

* * *

[Prev](sql-cluster.md "CLUSTER") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-commit.md "COMMIT")  
---|---|---  
CLUSTER | [Home](index.md "PostgreSQL 17.5 Documentation")|  COMMIT
