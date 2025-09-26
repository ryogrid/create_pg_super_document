# GroupingFunc

## Location
src/include/nodes/primnodes.h: 537 - 555

## Overview
The GroupingFunc structure represents a GROUPING(...) expression in PostgreSQL's query processing system, used to determine which columns are included in the current grouping set when using GROUP BY with multiple grouping sets.

## Definition
```c
typedef struct GroupingFunc
{
    Expr        xpr;
    List       *args pg_node_attr(query_jumble_ignore);        /* arguments, not evaluated but kept for benefit of EXPLAIN etc. */
    List       *refs pg_node_attr(equal_ignore);              /* ressortgrouprefs of arguments */
    List       *cols pg_node_attr(equal_ignore, query_jumble_ignore);  /* actual column positions set by planner */
    Index       agglevelsup;                                   /* same as Aggref.agglevelsup */
    ParseLoc    location;                                      /* token location */
} GroupingFunc;
```

## Detailed Description
The GroupingFunc structure implements the SQL GROUPING() function, which is essential for advanced GROUP BY operations involving multiple grouping sets, CUBE, and ROLLUP. Unlike regular aggregate functions, GROUPING() never evaluates its arguments; instead, the arguments serve as designators for expressions from the GROUP BY clause. The function returns a bitmask indicating which columns from its argument list are included in the current grouping set being processed. This allows queries to distinguish between different levels of aggregation and handle NULL values that represent "don't care" conditions versus actual NULL data values. The structure maintains the original arguments for EXPLAIN output while storing processed references and column positions for efficient runtime evaluation.

## Parameters / Member Variables
- `xpr`: Base expression node structure containing common expression properties
- `args`: List of expressions from the original GROUPING() call, preserved for EXPLAIN and debugging purposes
- `refs`: List of ressortgroupref values corresponding to the arguments, filled during parse analysis
- `cols`: List of actual column positions within the grouping set, filled by the planner for runtime evaluation
- `agglevelsup`: Query nesting level indicator, same semantics as Aggref.agglevelsup (0 for current level, >0 for outer levels)
- `location`: Token location in the original query text for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - ExecInitExprRec (expression initialization)
  - transformGroupingFunc (parser processing)
  - Various optimizer functions for grouping set planning
  - Expression evaluation and manipulation utilities

## Notes and Other Information
- Equality comparison ignores refs and cols annotations, focusing on args and agglevelsup for semantic equivalence
- Arguments are ignored during query jumbling as they represent structural information rather than data
- Essential for implementing SQL standard GROUPING SETS, CUBE, and ROLLUP functionality
- Enables sophisticated reporting queries that need to distinguish between different aggregation levels
- The three-phase processing (args → refs → cols) allows for progressive refinement during query processing
- Unlike other aggregate-like functions, never actually evaluates its argument expressions at runtime
- Critical for generating correct results when mixing aggregated and non-aggregated data in grouping set queries