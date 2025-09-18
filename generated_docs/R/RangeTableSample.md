# RangeTableSample

## Location
src/include/nodes/parsenodes.h: 695 - 703

## Overview
RangeTableSample represents a TABLESAMPLE clause in PostgreSQL's raw parse tree, encapsulating the sampling method, parameters, and repeatability settings for statistical sampling of table data.

## Definition
```c
typedef struct RangeTableSample
{
    NodeTag     type;
    Node       *relation;       /* relation to be sampled */
    List       *method;         /* sampling method name (possibly qualified) */
    List       *args;           /* argument(s) for sampling method */
    Node       *repeatable;     /* REPEATABLE expression, or NULL if none */
    ParseLoc    location;       /* method name location, or -1 if unknown */
} RangeTableSample;
```

## Detailed Description
RangeTableSample represents the TABLESAMPLE clause that appears in raw FROM clauses, implementing the SQL standard's table sampling functionality. The structure wraps around the relation node rather than being a subfield, representing the syntax: `<relation> TABLESAMPLE <method> (<params>) REPEATABLE (<num>)`. Currently, the relation is typically a RangeVar, but the design allows for future extension to support subselects and other relation types. This enables statistical sampling for large tables using various sampling methods.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a RangeTableSample node
- `relation`: Pointer to the relation node to be sampled (typically RangeVar)
- `method`: List representing the sampling method name, possibly schema-qualified
- `args`: List of argument expressions for the sampling method (e.g., percentage or row count)
- `repeatable`: Expression for the REPEATABLE clause seed value, or NULL if not specified
- `location`: Parse location of the method name for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - [transformRangeTableSample](../t/transformRangeTableSample.md)
  - [transformFromClauseItem](../t/transformFromClauseItem.md)
  - [exprLocation](../e/exprLocation.md)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:695-703
- Only appears in raw parse trees before transformation
- Wraps around the relation node rather than being embedded within it
- Supports the SQL standard TABLESAMPLE syntax with method-specific parameters
- The REPEATABLE clause ensures reproducible sampling results when specified
- Future versions may support sampling from subselects and other relation types