# ValuesScan

## Location
[src/include/nodes/plannodes.h:620-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L620-L624)

## Overview
ValuesScan represents a plan node for scanning literal value lists in PostgreSQL's query execution tree, typically used for VALUES clauses that provide constant data rows.

## Definition
```c
typedef struct ValuesScan
{
    Scan        scan;
    List       *values_lists;    /* list of expression lists */
} ValuesScan;
```

## Detailed Description
ValuesScan is a specialized plan node that handles the execution of VALUES clauses in SQL queries. It extends the base Scan node to provide functionality for scanning through lists of literal values or expressions that represent constant table data. This node type is commonly used when queries contain VALUES clauses that specify explicit row data, such as `VALUES (1, 'a'), (2, 'b'), (3, 'c')`.

The node stores multiple lists of expressions, where each inner list represents a single row of values, and the outer list contains all the rows. During execution, the node iterates through these expression lists, evaluating each expression and producing tuples that can be consumed by other parts of the query execution tree.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scanning information like target lists, qualifications, and plan node metadata
- `values_lists`: List of expression lists where each inner list represents one row of values to be scanned

## Dependencies
- Functions called/Symbols referenced:
  - [Scan](../S/Scan.md) (base structure)
  - [List](../L/List.md) (for values_lists storage)
  
- Called from (representative examples):
  - [ExecInitValuesScan](../E/ExecInitValuesScan.md) (executor initialization)
  - [create_valuesscan_plan](../c/create_valuesscan_plan.md) (plan creation)
  - [make_valuesscan](../m/make_valuesscan.md) (plan node construction)
  - [set_plan_refs](../s/set_plan_refs.md) (plan reference setting)
  - [finalize_plan](../f/finalize_plan.md) (plan finalization)

## Notes and Other Information
- Essential for implementing SQL VALUES clauses that provide constant data
- Part of PostgreSQL's comprehensive plan node hierarchy for different data sources
- Efficiently handles literal value scanning without requiring actual table storage
- Supports complex expressions within VALUES clauses, not just simple literals
- Integrates seamlessly with the executor framework for consistent query processing
- Used in scenarios like INSERT statements with explicit values, subqueries with VALUES, and common table expressions containing constant data