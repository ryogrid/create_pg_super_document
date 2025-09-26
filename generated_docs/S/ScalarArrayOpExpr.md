# ScalarArrayOpExpr

## Location
[src/include/nodes/primnodes.h:893-920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L893-L920)

## Overview
ScalarArrayOpExpr represents "scalar op ANY/ALL (array)" expressions in PostgreSQL, combining a scalar value with array elements using boolean operators.

## Definition
```c
typedef struct ScalarArrayOpExpr
{
    Expr        xpr;

    /* PG_OPERATOR OID of the operator */
    Oid         opno;

    /* PG_PROC OID of comparison function */
    Oid         opfuncid pg_node_attr(equal_ignore_if_zero, query_jumble_ignore);

    /* PG_PROC OID of hash func or InvalidOid */
    Oid         hashfuncid pg_node_attr(equal_ignore_if_zero, query_jumble_ignore);

    /* PG_PROC OID of negator of opfuncid function or InvalidOid.  See above */
    Oid         negfuncid pg_node_attr(equal_ignore_if_zero, query_jumble_ignore);

    /* true for ANY, false for ALL */
    bool        useOr;

    /* OID of collation that operator should use */
    Oid         inputcollid pg_node_attr(query_jumble_ignore);

    /* the scalar and array operands */
    List       *args;

    /* token location, or -1 if unknown */
    ParseLoc    location;
} ScalarArrayOpExpr;
```

## Detailed Description
ScalarArrayOpExpr handles expressions of the form "scalar op ANY/ALL (array)" where the operator must yield a boolean result. The operator is applied between the left scalar operand and each element of the right-hand array, with results combined using OR (for ANY) or AND (for ALL).

The node supports optimized execution through hash-based evaluation. When hashfuncid is set, the executor builds a hash table containing constant values from the array and probes it during evaluation, significantly improving performance for large arrays. For hashed NOT IN operations, negfuncid contains the equality function used for hash table operations while opno/opfuncid remain set to the <> operator.

Similar to OpExpr, function OIDs (opfuncid, hashfuncid, negfuncid) may not be filled immediately and are ignored in equality comparisons when zero. The result type is always boolean, so it doesn't need to be stored.

## Parameters / Member Variables
- `xpr`: Base Expr node structure containing common expression fields
- `opno`: OID of the operator definition in pg_operator catalog
- `opfuncid`: OID of the comparison function implementation (may be 0 during parsing)
- `hashfuncid`: OID of hash function for optimized execution, or InvalidOid for traditional evaluation
- `negfuncid`: OID of equality function for hashed NOT IN operations, or InvalidOid for non-hash evaluation
- `useOr`: Boolean flag - true for ANY semantics (OR combination), false for ALL semantics (AND combination)
- `inputcollid`: OID of collation that the operator should use for comparisons
- `args`: List containing exactly 2 expressions - the scalar operand and the array operand
- `location`: Parse location of the operator token in the original query, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - make_scalar_array_op (creates ScalarArrayOpExpr nodes)
  - ExecInitExprRec (expression initialization)
  - ExecEvalHashedScalarArrayOp (optimized hash-based evaluation)
  - clause_selectivity_ext (selectivity estimation)
  - match_saopclause_to_indexcol (index usage analysis)
  - convert_saop_to_hashed_saop_walker (hash optimization)
  - scalararraysel (selectivity calculation)
  - negate_clause (query transformation)

## Notes and Other Information
- Exclusively handles IN, NOT IN, and similar array membership tests with ANY/ALL semantics
- Hash-based execution optimization provides significant performance improvements for large constant arrays
- Result type is always boolean, unlike regular OpExpr which can return any type
- Critical for efficient execution of queries with IN clauses and array comparisons
- Supports both traditional element-by-element evaluation and hash table-based lookups
- The useOr flag determines the logical combination: ANY uses OR (true if any match), ALL uses AND (true if all match)
- Query jumbling considers the operator and arguments but ignores function OIDs for plan cache efficiency