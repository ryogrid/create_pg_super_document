MERGE  
---  
[Prev](sql-lock.md "LOCK") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-move.md "MOVE")  
  
* * *

## MERGE

MERGE — conditionally insert, update, or delete rows of a table

## Synopsis
    
    
    [ WITH _with_query_ [, ...] ]
    MERGE INTO [ ONLY ] _target_table_name_ [ * ] [ [ AS ] _target_alias_ ]
    USING _data_source_ ON _join_condition_
    _when_clause_ [...]
    [ RETURNING { * | _output_expression_ [ [ AS ] _output_name_ ] } [, ...] ]
    
    where _data_source_ is:
    
    { [ ONLY ] _source_table_name_ [ * ] | ( _source_query_ ) } [ [ AS ] _source_alias_ ]
    
    and _when_clause_ is:
    
    { WHEN MATCHED [ AND _condition_ ] THEN { _merge_update_ | _merge_delete_ | DO NOTHING } |
      WHEN NOT MATCHED BY SOURCE [ AND _condition_ ] THEN { _merge_update_ | _merge_delete_ | DO NOTHING } |
      WHEN NOT MATCHED [ BY TARGET ] [ AND _condition_ ] THEN { _merge_insert_ | DO NOTHING } }
    
    and _merge_insert_ is:
    
    INSERT [( _column_name_ [, ...] )]
    [ OVERRIDING { SYSTEM | USER } VALUE ]
    { VALUES ( { _expression_ | DEFAULT } [, ...] ) | DEFAULT VALUES }
    
    and _merge_update_ is:
    
    UPDATE SET { _column_name_ = { _expression_ | DEFAULT } |
                 ( _column_name_ [, ...] ) = [ ROW ] ( { _expression_ | DEFAULT } [, ...] ) |
                 ( _column_name_ [, ...] ) = ( _sub-SELECT_ )
               } [, ...]
    
    and _merge_delete_ is:
    
    DELETE
    

## Description

`MERGE` performs actions that modify rows in the target table identified as _`target_table_name`_ , using the _`data_source`_. `MERGE` provides a single SQL statement that can conditionally `INSERT`, `UPDATE` or `DELETE` rows, a task that would otherwise require multiple procedural language statements. 

First, the `MERGE` command performs a join from _`data_source`_ to the target table producing zero or more candidate change rows. For each candidate change row, the status of `MATCHED`, `NOT MATCHED BY SOURCE`, or `NOT MATCHED [BY TARGET]` is set just once, after which `WHEN` clauses are evaluated in the order specified. For each candidate change row, the first clause to evaluate as true is executed. No more than one `WHEN` clause is executed for any candidate change row. 

`MERGE` actions have the same effect as regular `UPDATE`, `INSERT`, or `DELETE` commands of the same names. The syntax of those commands is different, notably that there is no `WHERE` clause and no table name is specified. All actions refer to the target table, though modifications to other tables may be made using triggers. 

When `DO NOTHING` is specified, the source row is skipped. Since actions are evaluated in their specified order, `DO NOTHING` can be handy to skip non-interesting source rows before more fine-grained handling. 

The optional `RETURNING` clause causes `MERGE` to compute and return value(s) based on each row inserted, updated, or deleted. Any expression using the source or target table's columns, or the [`merge_action()`](functions-merge-support.md#MERGE-ACTION) function can be computed. When an `INSERT` or `UPDATE` action is performed, the new values of the target table's columns are used. When a `DELETE` is performed, the old values of the target table's columns are used. The syntax of the `RETURNING` list is identical to that of the output list of `SELECT`. 

There is no separate `MERGE` privilege. If you specify an update action, you must have the `UPDATE` privilege on the column(s) of the target table that are referred to in the `SET` clause. If you specify an insert action, you must have the `INSERT` privilege on the target table. If you specify a delete action, you must have the `DELETE` privilege on the target table. If you specify a `DO NOTHING` action, you must have the `SELECT` privilege on at least one column of the target table. You will also need `SELECT` privilege on any column(s) of the _`data_source`_ and of the target table referred to in any `condition` (including `join_condition`) or `expression`. Privileges are tested once at statement start and are checked whether or not particular `WHEN` clauses are executed. 

`MERGE` is not supported if the target table is a materialized view, foreign table, or if it has any rules defined on it. 

## Parameters

 _`with_query`_
    

The `WITH` clause allows you to specify one or more subqueries that can be referenced by name in the `MERGE` query. See [Section 7.8](queries-with.md "7.8. WITH Queries \(Common Table Expressions\)") and [SELECT](sql-select.md "SELECT") for details. Note that `WITH RECURSIVE` is not supported by `MERGE`. 

_`target_table_name`_
    

The name (optionally schema-qualified) of the target table or view to merge into. If `ONLY` is specified before a table name, matching rows are updated or deleted in the named table only. If `ONLY` is not specified, matching rows are also updated or deleted in any tables inheriting from the named table. Optionally, `*` can be specified after the table name to explicitly indicate that descendant tables are included. The `ONLY` keyword and `*` option do not affect insert actions, which always insert into the named table only. 

If _`target_table_name`_ is a view, it must either be automatically updatable with no `INSTEAD OF` triggers, or it must have `INSTEAD OF` triggers for every type of action (`INSERT`, `UPDATE`, and `DELETE`) specified in the `WHEN` clauses. Views with rules are not supported. 

_`target_alias`_
    

A substitute name for the target table. When an alias is provided, it completely hides the actual name of the table. For example, given `MERGE INTO foo AS f`, the remainder of the `MERGE` statement must refer to this table as `f` not `foo`. 

_`source_table_name`_
    

The name (optionally schema-qualified) of the source table, view, or transition table. If `ONLY` is specified before the table name, matching rows are included from the named table only. If `ONLY` is not specified, matching rows are also included from any tables inheriting from the named table. Optionally, `*` can be specified after the table name to explicitly indicate that descendant tables are included. 

_`source_query`_
    

A query (`SELECT` statement or `VALUES` statement) that supplies the rows to be merged into the target table. Refer to the [SELECT](sql-select.md "SELECT") statement or [VALUES](sql-values.md "VALUES") statement for a description of the syntax. 

_`source_alias`_
    

A substitute name for the data source. When an alias is provided, it completely hides the actual name of the table or the fact that a query was issued. 

_`join_condition`_
    

_`join_condition`_ is an expression resulting in a value of type `boolean` (similar to a `WHERE` clause) that specifies which rows in the _`data_source`_ match rows in the target table. 

### Warning

Only columns from the target table that attempt to match _`data_source`_ rows should appear in _`join_condition`_. _`join_condition`_ subexpressions that only reference the target table's columns can affect which action is taken, often in surprising ways. 

If both `WHEN NOT MATCHED BY SOURCE` and `WHEN NOT MATCHED [BY TARGET]` clauses are specified, the `MERGE` command will perform a `FULL` join between _`data_source`_ and the target table. For this to work, at least one _`join_condition`_ subexpression must use an operator that can support a hash join, or all of the subexpressions must use operators that can support a merge join. 

_`when_clause`_
    

At least one `WHEN` clause is required. 

The `WHEN` clause may specify `WHEN MATCHED`, `WHEN NOT MATCHED BY SOURCE`, or `WHEN NOT MATCHED [BY TARGET]`. Note that the SQL standard only defines `WHEN MATCHED` and `WHEN NOT MATCHED` (which is defined to mean no matching target row). `WHEN NOT MATCHED BY SOURCE` is an extension to the SQL standard, as is the option to append `BY TARGET` to `WHEN NOT MATCHED`, to make its meaning more explicit. 

If the `WHEN` clause specifies `WHEN MATCHED` and the candidate change row matches a row in the _`data_source`_ to a row in the target table, the `WHEN` clause is executed if the _`condition`_ is absent or it evaluates to `true`. 

If the `WHEN` clause specifies `WHEN NOT MATCHED BY SOURCE` and the candidate change row represents a row in the target table that does not match a row in the _`data_source`_ , the `WHEN` clause is executed if the _`condition`_ is absent or it evaluates to `true`. 

If the `WHEN` clause specifies `WHEN NOT MATCHED [BY TARGET]` and the candidate change row represents a row in the _`data_source`_ that does not match a row in the target table, the `WHEN` clause is executed if the _`condition`_ is absent or it evaluates to `true`. 

_`condition`_
    

An expression that returns a value of type `boolean`. If this expression for a `WHEN` clause returns `true`, then the action for that clause is executed for that row. 

A condition on a `WHEN MATCHED` clause can refer to columns in both the source and the target relations. A condition on a `WHEN NOT MATCHED BY SOURCE` clause can only refer to columns from the target relation, since by definition there is no matching source row. A condition on a `WHEN NOT MATCHED [BY TARGET]` clause can only refer to columns from the source relation, since by definition there is no matching target row. Only the system attributes from the target table are accessible. 

_`merge_insert`_
    

The specification of an `INSERT` action that inserts one row into the target table. The target column names can be listed in any order. If no list of column names is given at all, the default is all the columns of the table in their declared order. 

Each column not present in the explicit or implicit column list will be filled with a default value, either its declared default value or null if there is none. 

If the target table is a partitioned table, each row is routed to the appropriate partition and inserted into it. If the target table is a partition, an error will occur if any input row violates the partition constraint. 

Column names may not be specified more than once. `INSERT` actions cannot contain sub-selects. 

Only one `VALUES` clause can be specified. The `VALUES` clause can only refer to columns from the source relation, since by definition there is no matching target row. 

_`merge_update`_
    

The specification of an `UPDATE` action that updates the current row of the target table. Column names may not be specified more than once. 

Neither a table name nor a `WHERE` clause are allowed. 

_`merge_delete`_
    

Specifies a `DELETE` action that deletes the current row of the target table. Do not include the table name or any other clauses, as you would normally do with a [DELETE](sql-delete.md "DELETE") command. 

_`column_name`_
    

The name of a column in the target table. The column name can be qualified with a subfield name or array subscript, if needed. (Inserting into only some fields of a composite column leaves the other fields null.) Do not include the table's name in the specification of a target column. 

`OVERRIDING SYSTEM VALUE`
    

Without this clause, it is an error to specify an explicit value (other than `DEFAULT`) for an identity column defined as `GENERATED ALWAYS`. This clause overrides that restriction. 

`OVERRIDING USER VALUE`
    

If this clause is specified, then any values supplied for identity columns defined as `GENERATED BY DEFAULT` are ignored and the default sequence-generated values are applied. 

`DEFAULT VALUES`
    

All columns will be filled with their default values. (An `OVERRIDING` clause is not permitted in this form.) 

_`expression`_
    

An expression to assign to the column. If used in a `WHEN MATCHED` clause, the expression can use values from the original row in the target table, and values from the _`data_source`_ row. If used in a `WHEN NOT MATCHED BY SOURCE` clause, the expression can only use values from the original row in the target table. If used in a `WHEN NOT MATCHED [BY TARGET]` clause, the expression can only use values from the _`data_source`_ row. 

`DEFAULT`
    

Set the column to its default value (which will be `NULL` if no specific default expression has been assigned to it). 

_`sub-SELECT`_
    

A `SELECT` sub-query that produces as many output columns as are listed in the parenthesized column list preceding it. The sub-query must yield no more than one row when executed. If it yields one row, its column values are assigned to the target columns; if it yields no rows, NULL values are assigned to the target columns. If used in a `WHEN MATCHED` clause, the sub-query can refer to values from the original row in the target table, and values from the _`data_source`_ row. If used in a `WHEN NOT MATCHED BY SOURCE` clause, the sub-query can only refer to values from the original row in the target table. 

_`output_expression`_
    

An expression to be computed and returned by the `MERGE` command after each row is changed (whether inserted, updated, or deleted). The expression can use any columns of the source or target tables, or the [`merge_action()`](functions-merge-support.md#MERGE-ACTION) function to return additional information about the action executed. 

Writing `*` will return all columns from the source table, followed by all columns from the target table. Often this will lead to a lot of duplication, since it is common for the source and target tables to have a lot of the same columns. This can be avoided by qualifying the `*` with the name or alias of the source or target table. 

_`output_name`_
    

A name to use for a returned column. 

## Outputs

On successful completion, a `MERGE` command returns a command tag of the form 
    
    
    MERGE _total_count_
    

The _`total_count`_ is the total number of rows changed (whether inserted, updated, or deleted). If _`total_count`_ is 0, no rows were changed in any way. 

If the `MERGE` command contains a `RETURNING` clause, the result will be similar to that of a `SELECT` statement containing the columns and values defined in the `RETURNING` list, computed over the row(s) inserted, updated, or deleted by the command. 

## Notes

The following steps take place during the execution of `MERGE`. 

  1. Perform any `BEFORE STATEMENT` triggers for all actions specified, whether or not their `WHEN` clauses match. 

  2. Perform a join from source to target table. The resulting query will be optimized normally and will produce a set of candidate change rows. For each candidate change row, 

     1. Evaluate whether each row is `MATCHED`, `NOT MATCHED BY SOURCE`, or `NOT MATCHED [BY TARGET]`. 

     2. Test each `WHEN` condition in the order specified until one returns true. 

     3. When a condition returns true, perform the following actions: 

        1. Perform any `BEFORE ROW` triggers that fire for the action's event type. 

        2. Perform the specified action, invoking any check constraints on the target table. 

        3. Perform any `AFTER ROW` triggers that fire for the action's event type. 

If the target relation is a view with `INSTEAD OF ROW` triggers for the action's event type, they are used to perform the action instead. 

  3. Perform any `AFTER STATEMENT` triggers for actions specified, whether or not they actually occur. This is similar to the behavior of an `UPDATE` statement that modifies no rows. 




In summary, statement triggers for an event type (say, `INSERT`) will be fired whenever we _specify_ an action of that kind. In contrast, row-level triggers will fire only for the specific event type being _executed_. So a `MERGE` command might fire statement triggers for both `UPDATE` and `INSERT`, even though only `UPDATE` row triggers were fired. 

You should ensure that the join produces at most one candidate change row for each target row. In other words, a target row shouldn't join to more than one data source row. If it does, then only one of the candidate change rows will be used to modify the target row; later attempts to modify the row will cause an error. This can also occur if row triggers make changes to the target table and the rows so modified are then subsequently also modified by `MERGE`. If the repeated action is an `INSERT`, this will cause a uniqueness violation, while a repeated `UPDATE` or `DELETE` will cause a cardinality violation; the latter behavior is required by the SQL standard. This differs from historical PostgreSQL behavior of joins in `UPDATE` and `DELETE` statements where second and subsequent attempts to modify the same row are simply ignored. 

If a `WHEN` clause omits an `AND` sub-clause, it becomes the final reachable clause of that kind (`MATCHED`, `NOT MATCHED BY SOURCE`, or `NOT MATCHED [BY TARGET]`). If a later `WHEN` clause of that kind is specified it would be provably unreachable and an error is raised. If no final reachable clause is specified of either kind, it is possible that no action will be taken for a candidate change row. 

The order in which rows are generated from the data source is indeterminate by default. A _`source_query`_ can be used to specify a consistent ordering, if required, which might be needed to avoid deadlocks between concurrent transactions. 

When `MERGE` is run concurrently with other commands that modify the target table, the usual transaction isolation rules apply; see [Section 13.2](transaction-iso.md "13.2. Transaction Isolation") for an explanation on the behavior at each isolation level. You may also wish to consider using `INSERT ... ON CONFLICT` as an alternative statement which offers the ability to run an `UPDATE` if a concurrent `INSERT` occurs. There are a variety of differences and restrictions between the two statement types and they are not interchangeable. 

## Examples

Perform maintenance on `customer_accounts` based upon new `recent_transactions`. 
    
    
    MERGE INTO customer_account ca
    USING recent_transactions t
    ON t.customer_id = ca.customer_id
    WHEN MATCHED THEN
      UPDATE SET balance = balance + transaction_value
    WHEN NOT MATCHED THEN
      INSERT (customer_id, balance)
      VALUES (t.customer_id, t.transaction_value);
    

Notice that this would be exactly equivalent to the following statement because the `MATCHED` result does not change during execution. 
    
    
    MERGE INTO customer_account ca
    USING (SELECT customer_id, transaction_value FROM recent_transactions) AS t
    ON t.customer_id = ca.customer_id
    WHEN MATCHED THEN
      UPDATE SET balance = balance + transaction_value
    WHEN NOT MATCHED THEN
      INSERT (customer_id, balance)
      VALUES (t.customer_id, t.transaction_value);
    

Attempt to insert a new stock item along with the quantity of stock. If the item already exists, instead update the stock count of the existing item. Don't allow entries that have zero stock. Return details of all changes made. 
    
    
    MERGE INTO wines w
    USING wine_stock_changes s
    ON s.winename = w.winename
    WHEN NOT MATCHED AND s.stock_delta > 0 THEN
      INSERT VALUES(s.winename, s.stock_delta)
    WHEN MATCHED AND w.stock + s.stock_delta > 0 THEN
      UPDATE SET stock = w.stock + s.stock_delta
    WHEN MATCHED THEN
      DELETE
    RETURNING merge_action(), w.*;
    

The `wine_stock_changes` table might be, for example, a temporary table recently loaded into the database. 

Update `wines` based on a replacement wine list, inserting rows for any new stock, updating modified stock entries, and deleting any wines not present in the new list. 
    
    
    MERGE INTO wines w
    USING new_wine_list s
    ON s.winename = w.winename
    WHEN NOT MATCHED BY TARGET THEN
      INSERT VALUES(s.winename, s.stock)
    WHEN MATCHED AND w.stock != s.stock THEN
      UPDATE SET stock = s.stock
    WHEN NOT MATCHED BY SOURCE THEN
      DELETE;
    

## Compatibility

This command conforms to the SQL standard. 

The `WITH` clause, `BY SOURCE` and `BY TARGET` qualifiers to `WHEN NOT MATCHED`, `DO NOTHING` action, and `RETURNING` clause are extensions to the SQL standard. 

* * *

[Prev](sql-lock.md "LOCK") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-move.md "MOVE")  
---|---|---  
LOCK | [Home](index.md "PostgreSQL 17.5 Documentation")|  MOVE
