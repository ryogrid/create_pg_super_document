# brin_inclusion_consistent

## Location
[src/backend/access/brin/brin_inclusion.c:250-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_inclusion.c#L250-L473)

## Overview
BRIN inclusion consistent function that determines whether a BRIN index tuple could contain values matching a scan key predicate.

## Definition
```c
Datum brin_inclusion_consistent(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the consistent handler for BRIN inclusion operator classes, implementing the core logic for index scans. It evaluates whether a page range represented by a BRIN tuple could possibly contain tuples satisfying a given scan key. The function handles multiple strategy classes: placement strategies (left/right, above/below), overlap and containment strategies, contained-by strategies, adjacency, and basic comparison operations. For placement strategies, it uses logical negation of converse operators. The function accounts for special cases like empty elements and unmergeable values, ensuring accurate scan results while minimizing false positives.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN index descriptor containing metadata and operator class information  
- `column` (BrinValues *): BRIN values structure representing the current page range
- `key` (ScanKey): Scan key containing the search predicate with strategy, argument, and metadata
- `colloid` (Oid): Collation OID for comparison operations
- `subtype` (Oid): Subtype OID from the scan key for polymorphic operators

## Dependencies
- Functions called/Symbols referenced:
  - [inclusion_get_strategy_procinfo](../i/inclusion_get_strategy_procinfo.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)  
  - [DatumGetBool](../D/DatumGetBool.md)
  - PG_GET_COLLATION
  - PG_NARGS
  - PG_RETURN_BOOL
  - PG_RETURN_DATUM
  - elog
- Constants:
  - INCLUSION_UNMERGEABLE
  - INCLUSION_UNION
  - INCLUSION_CONTAINS_EMPTY
  - Strategy number constants (RT*StrategyNumber)
- Data structures:
  - [BrinDesc](../B/BrinDesc.md)
  - [BrinValues](../B/BrinValues.md)  
  - ScanKey
  - [FmgrInfo](../F/FmgrInfo.md)
- Called from (representative examples):
  - No direct references found (typically called via BRIN framework during index scans)

## Notes and Other Information
- Uses the old function signature with only three arguments (asserted via PG_NARGS())
- Returns true for ranges marked as containing unmergeable elements (conservative approach)
- Implements placement strategies by negating results of converse operators
- Handles empty elements specially in contained-by and comparison strategies
- For contained-by strategies, uses overlap test first, then checks contains-empty flag
- Adjacent strategy combines overlap test with actual adjacency operator call
- Basic comparison strategies account for empty elements being considered less than others
- Contains extensive strategy-specific logic optimized for different geometric and set operations

## Simplified Source

```c
Datum brin_inclusion_consistent(PG_FUNCTION_ARGS) {
    BrinDesc *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
    BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
    ScanKey key = (ScanKey) PG_GETARG_POINTER(2);
    Oid colloid = PG_GET_COLLATION();

    // Early return for unmergeable ranges (conservative: might contain anything)
    if (DatumGetBool(column->bv_values[INCLUSION_UNMERGEABLE]))
        PG_RETURN_BOOL(true);

    AttrNumber attno = key->sk_attno;
    Oid subtype = key->sk_subtype;
    Datum query = key->sk_argument;
    Datum unionval = column->bv_values[INCLUSION_UNION];
    FmgrInfo *finfo;
    Datum result;

    switch (key->sk_strategy) {
        // Placement strategies - use negation of converse operators
        case RTLeftStrategyNumber:
            finfo = inclusion_get_strategy_procinfo(bdesc, attno, subtype, RTOverRightStrategyNumber);
            result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL(!DatumGetBool(result));

        case RTRightStrategyNumber:
            finfo = inclusion_get_strategy_procinfo(bdesc, attno, subtype, RTOverLeftStrategyNumber);
            result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_BOOL(!DatumGetBool(result));

        // Overlap and containment strategies - direct operator call
        case RTOverlapStrategyNumber:
        case RTContainsStrategyNumber:
        case RTContainsElemStrategyNumber:
            finfo = inclusion_get_strategy_procinfo(bdesc, attno, subtype, key->sk_strategy);
            result = FunctionCall2Coll(finfo, colloid, unionval, query);
            PG_RETURN_DATUM(result);

        // Contained-by strategies - use overlap + check empty elements
        case RTContainedByStrategyNumber:
            finfo = inclusion_get_strategy_procinfo(bdesc, attno, subtype, RTOverlapStrategyNumber);
            result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if (DatumGetBool(result))
                PG_RETURN_BOOL(true);
            PG_RETURN_DATUM(column->bv_values[INCLUSION_CONTAINS_EMPTY]);

        // Equality strategies - use contains operator + check empty elements
        case RTEqualStrategyNumber:
            finfo = inclusion_get_strategy_procinfo(bdesc, attno, subtype, RTContainsStrategyNumber);
            result = FunctionCall2Coll(finfo, colloid, unionval, query);
            if (DatumGetBool(result))
                PG_RETURN_BOOL(true);
            PG_RETURN_DATUM(column->bv_values[INCLUSION_CONTAINS_EMPTY]);

        // Additional strategies follow similar patterns...
        default:
            elog(ERROR, "invalid strategy number %d", key->sk_strategy);
            PG_RETURN_BOOL(false);
    }
}
```