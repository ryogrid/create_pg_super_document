# Aggref

## Location
[src/include/nodes/primnodes.h:439-508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L439-L508)

## Overview
The Aggref structure represents an aggregate function call in PostgreSQL's expression tree, encapsulating all information needed for aggregate computation including arguments, ordering, grouping, and execution context.

## Definition
```c
typedef struct Aggref
{
    Expr        xpr;
    Oid         aggfnoid;                                          /* pg_proc Oid of the aggregate */
    Oid         aggtype pg_node_attr(query_jumble_ignore);         /* type Oid of result of the aggregate */
    Oid         aggcollid pg_node_attr(query_jumble_ignore);       /* OID of collation of result */
    Oid         inputcollid pg_node_attr(query_jumble_ignore);     /* OID of collation that function should use */
    Oid         aggtranstype pg_node_attr(equal_ignore, query_jumble_ignore);  /* type Oid of aggregate's transition value */
    List       *aggargtypes pg_node_attr(query_jumble_ignore);     /* type Oids of direct and aggregated args */
    List       *aggdirectargs;                                     /* direct arguments, if an ordered-set agg */
    List       *args;                                              /* aggregated arguments and sort expressions */
    List       *aggorder;                                          /* ORDER BY (list of SortGroupClause) */
    List       *aggdistinct;                                       /* DISTINCT (list of SortGroupClause) */
    Expr       *aggfilter;                                         /* FILTER expression, if any */
    bool        aggstar pg_node_attr(query_jumble_ignore);         /* true if argument list was really '*' */
    bool        aggvariadic pg_node_attr(query_jumble_ignore);     /* true if variadic arguments combined into array */
    char        aggkind pg_node_attr(query_jumble_ignore);         /* aggregate kind (see pg_aggregate.h) */
    bool        aggpresorted pg_node_attr(equal_ignore, query_jumble_ignore);  /* aggregate input already sorted */
    Index       agglevelsup pg_node_attr(query_jumble_ignore);     /* > 0 if agg belongs to outer query */
    AggSplit    aggsplit pg_node_attr(query_jumble_ignore);        /* expected agg-splitting mode of parent Agg */
    int         aggno pg_node_attr(query_jumble_ignore);           /* unique ID within the Agg node */
    int         aggtransno pg_node_attr(query_jumble_ignore);      /* unique ID of transition state in the Agg */
    ParseLoc    location;                                          /* token location, or -1 if unknown */
} Aggref;
```

## Detailed Description
The Aggref structure is a comprehensive representation of aggregate function calls within PostgreSQL's expression system. It supports both regular aggregates (like SUM, COUNT) and ordered-set aggregates (like percentile functions), handling complex features like ORDER BY clauses, DISTINCT operations, and FILTER conditions. The structure maintains detailed type information, execution context, and optimization hints that enable PostgreSQL's query planner to efficiently execute aggregate operations. It supports partial aggregation for parallel processing and maintains unique identifiers for sharing aggregate computations and transition states.

## Parameters / Member Variables
- `xpr`: Base expression node structure containing common expression properties
- `aggfnoid`: PostgreSQL procedure OID identifying the specific aggregate function
- `aggtype`: Result type OID of the aggregate function
- `aggcollid`: Collation OID for the aggregate result
- `inputcollid`: Collation OID that the aggregate function should use for input processing
- `aggtranstype`: Type OID of the aggregate's internal transition state (set during planning)
- `aggargtypes`: List of type OIDs for all direct and regular arguments
- `aggdirectargs`: Direct arguments for ordered-set aggregates (plain expressions, not TargetEntry nodes)
- `args`: List of TargetEntry nodes representing aggregated arguments and sort expressions
- `aggorder`: List of SortGroupClause nodes specifying ORDER BY operations on aggregate input
- `aggdistinct`: List of SortGroupClause nodes specifying DISTINCT operations on aggregate input
- `aggfilter`: Optional FILTER expression to conditionally include/exclude input rows
- `aggstar`: Boolean flag indicating if the argument list was specified as '*' (e.g., COUNT(*))
- `aggvariadic`: Boolean flag indicating if variadic arguments were combined into an array
- `aggkind`: Character indicating the aggregate kind (normal, ordered-set, hypothetical)
- `aggpresorted`: Boolean flag set by planner when input is already sorted for this aggregate
- `agglevelsup`: Nesting level indicator (0 for current level, >0 for outer query levels)
- `aggsplit`: Expected partial aggregation mode for parallel processing
- `aggno`: Unique identifier within the Agg node for result sharing
- `aggtransno`: Unique identifier for transition state sharing
- `location`: Token location in original query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - AggSplit
  - ParseLoc
- Called from (representative examples):
  - find_expr_references_walker (dependency analysis)
  - ExecInitExprRec (expression initialization)
  - ExecInitAgg (aggregate node initialization)
  - transformAggregateCall (parser aggregate processing)
  - Various optimizer functions for aggregate planning and optimization

## Notes and Other Information
- Central to PostgreSQL's aggregate processing system with extensive usage throughout the codebase
- Supports complex aggregate features including ordered-set aggregates, window functions, and partial aggregation
- Many fields ignored during query jumbling to focus on structural equivalence rather than execution details
- Handles both simple aggregates (SUM, COUNT) and complex ordered-set aggregates (percentile functions)
- Critical for parallel aggregate execution through aggsplit and shared transition states
- Location tracking enables precise error reporting for aggregate-related syntax errors