ALTER OPERATOR  
---  
[Prev](sql-altermaterializedview.md "ALTER MATERIALIZED VIEW") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alteropclass.md "ALTER OPERATOR CLASS")  
  
* * *

## ALTER OPERATOR

ALTER OPERATOR — change the definition of an operator

## Synopsis
    
    
    ALTER OPERATOR _name_ ( { _left_type_ | NONE } , _right_type_ )
        OWNER TO { _new_owner_ | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
    
    ALTER OPERATOR _name_ ( { _left_type_ | NONE } , _right_type_ )
        SET SCHEMA _new_schema_
    
    ALTER OPERATOR _name_ ( { _left_type_ | NONE } , _right_type_ )
        SET ( {  RESTRICT = { _res_proc_ | NONE }
               | JOIN = { _join_proc_ | NONE }
               | COMMUTATOR = _com_op_
               | NEGATOR = _neg_op_
               | HASHES
               | MERGES
              } [, ... ] )
    

## Description

`ALTER OPERATOR` changes the definition of an operator. 

You must own the operator to use `ALTER OPERATOR`. To alter the owner, you must be able to `SET ROLE` to the new owning role, and that role must have `CREATE` privilege on the operator's schema. (These restrictions enforce that altering the owner doesn't do anything you couldn't do by dropping and recreating the operator. However, a superuser can alter ownership of any operator anyway.) 

## Parameters

 _`name`_
    

The name (optionally schema-qualified) of an existing operator. 

_`left_type`_
    

The data type of the operator's left operand; write `NONE` if the operator has no left operand. 

_`right_type`_
    

The data type of the operator's right operand. 

_`new_owner`_
    

The new owner of the operator. 

_`new_schema`_
    

The new schema for the operator. 

_`res_proc`_
    

The restriction selectivity estimator function for this operator; write NONE to remove existing selectivity estimator. 

_`join_proc`_
    

The join selectivity estimator function for this operator; write NONE to remove existing selectivity estimator. 

_`com_op`_
    

The commutator of this operator. Can only be changed if the operator does not have an existing commutator. 

_`neg_op`_
    

The negator of this operator. Can only be changed if the operator does not have an existing negator. 

`HASHES`
    

Indicates this operator can support a hash join. Can only be enabled and not disabled. 

`MERGES`
    

Indicates this operator can support a merge join. Can only be enabled and not disabled. 

## Notes

Refer to [Section 36.14](xoper.md "36.14. User-Defined Operators") and [Section 36.15](xoper-optimization.md "36.15. Operator Optimization Information") for further information. 

Since commutators come in pairs that are commutators of each other, `ALTER OPERATOR SET COMMUTATOR` will also set the commutator of the _`com_op`_ to be the target operator. Likewise, `ALTER OPERATOR SET NEGATOR` will also set the negator of the _`neg_op`_ to be the target operator. Therefore, you must own the commutator or negator operator as well as the target operator. 

## Examples

Change the owner of a custom operator `a @@ b` for type `text`: 
    
    
    ALTER OPERATOR @@ (text, text) OWNER TO joe;
    

Change the restriction and join selectivity estimator functions of a custom operator `a && b` for type `int[]`: 
    
    
    ALTER OPERATOR && (int[], int[]) SET (RESTRICT = _int_contsel, JOIN = _int_contjoinsel);
    

Mark the `&&` operator as being its own commutator: 
    
    
    ALTER OPERATOR && (int[], int[]) SET (COMMUTATOR = &&);
    

## Compatibility

There is no `ALTER OPERATOR` statement in the SQL standard. 

## See Also

[CREATE OPERATOR](sql-createoperator.md "CREATE OPERATOR"), [DROP OPERATOR](sql-dropoperator.md "DROP OPERATOR")

* * *

[Prev](sql-altermaterializedview.md "ALTER MATERIALIZED VIEW") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alteropclass.md "ALTER OPERATOR CLASS")  
---|---|---  
ALTER MATERIALIZED VIEW | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER OPERATOR CLASS
