# execTuplesMatchPrepare

## Location
[src/backend/executor/execGrouping.c:58-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L58-L94)

## Overview
Creates an ExprState that evaluates whether two tuples are NOT DISTINCT for grouping operations in PostgreSQL execution engine.

## Definition
```c
ExprState *execTuplesMatchPrepare(TupleDesc desc,
                                  int numCols,
                                  const AttrNumber *keyColIdx,
                                  const Oid *eqOperators,
                                  const Oid *collations,
                                  PlanState *parent);
```

## Detailed Description
This function builds an expression that can be evaluated using ExecQual() to determine whether an ExprContext's inner/outer tuples are NOT DISTINCT. It is a core utility for grouping tuples together in various PostgreSQL execution nodes. The function converts equality operators into their corresponding function OIDs and delegates the actual expression building to ExecBuildGroupingEqual. If no columns are specified (numCols == 0), it returns NULL indicating no comparison is needed.

## Parameters / Member Variables
- `desc`: TupleDesc describing the structure of tuples to be compared
- `numCols`: Number of columns to compare for equality
- `keyColIdx`: Array of column indices (AttrNumber) that serve as grouping keys
- `eqOperators`: Array of equality operator OIDs for each comparison column
- `collations`: Array of collation OIDs for each comparison column
- `parent`: PlanState node that will own this expression

## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md) (converts operator OID to function OID)
  - [ExecBuildGroupingEqual](../E/ExecBuildGroupingEqual.md) (builds the actual grouping expression)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md) (aggregate initialization)
  - [ExecInitGroup](../E/ExecInitGroup.md) (group node initialization)
  - [ExecInitSetOp](../E/ExecInitSetOp.md) (set operation initialization)
  - [ExecInitUnique](../E/ExecInitUnique.md) (unique node initialization)
  - [ExecInitWindowAgg](../E/ExecInitWindowAgg.md) (window aggregate initialization)
  - [ExecInitLimit](../E/ExecInitLimit.md) (limit node initialization)

## Notes and Other Information
- Returns NULL when numCols is 0, indicating no grouping comparison is needed
- The function is part of the grouping utilities in execGrouping.c
- Uses both inner and outer tuple descriptors (same desc parameter) for comparison
- Essential for implementing SQL GROUP BY, DISTINCT, and similar operations that require tuple equality testing
- The returned ExprState can be efficiently evaluated during query execution using ExecQual()

## Simplified Source

```c
ExprState *execTuplesMatchPrepare(TupleDesc desc,
                                  int numCols,
                                  const AttrNumber *keyColIdx,
                                  const Oid *eqOperators,
                                  const Oid *collations,
                                  PlanState *parent) {

    // No columns to compare means no grouping needed
    if (numCols == 0)
        return NULL;

    // Convert equality operators to their corresponding function OIDs
    Oid *equality_functions = (Oid *) palloc(numCols * sizeof(Oid));
    for (int i = 0; i < numCols; i++) {
        equality_functions[i] = get_opcode(eqOperators[i]);
    }

    // Build the actual expression for tuple comparison
    ExprState *comparison_expr = ExecBuildGroupingEqual(desc, desc, NULL, NULL,
                                                       numCols, keyColIdx,
                                                       equality_functions,
                                                       collations, parent);

    return comparison_expr;
}
```