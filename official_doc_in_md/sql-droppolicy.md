DROP POLICY  
---  
[Prev](sql-drop-owned.md "DROP OWNED") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropprocedure.md "DROP PROCEDURE")  
  
* * *

## DROP POLICY

DROP POLICY — remove a row-level security policy from a table

## Synopsis
    
    
    DROP POLICY [ IF EXISTS ] _name_ ON _table_name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP POLICY` removes the specified policy from the table. Note that if the last policy is removed for a table and the table still has row-level security enabled via `ALTER TABLE`, then the default-deny policy will be used. `ALTER TABLE ... DISABLE ROW LEVEL SECURITY` can be used to disable row-level security for a table, whether policies for the table exist or not. 

## Parameters

`IF EXISTS`
    

Do not throw an error if the policy does not exist. A notice is issued in this case. 

_`name`_
    

The name of the policy to drop. 

_`table_name`_
    

The name (optionally schema-qualified) of the table that the policy is on. 

`CASCADE`  
`RESTRICT`
    

These key words do not have any effect, since there are no dependencies on policies. 

## Examples

To drop the policy called `p1` on the table named `my_table`: 
    
    
    DROP POLICY p1 ON my_table;
    

## Compatibility

`DROP POLICY` is a PostgreSQL extension. 

## See Also

[CREATE POLICY](sql-createpolicy.md "CREATE POLICY"), [ALTER POLICY](sql-alterpolicy.md "ALTER POLICY")

* * *

[Prev](sql-drop-owned.md "DROP OWNED") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropprocedure.md "DROP PROCEDURE")  
---|---|---  
DROP OWNED | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP PROCEDURE
