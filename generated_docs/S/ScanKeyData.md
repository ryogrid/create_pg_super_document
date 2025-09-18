# ScanKeyData

## Location
[src/include/access/skey.h:64-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/skey.h#L64-L73)

## Overview
ScanKeyData is a fundamental data structure in PostgreSQL that represents the application of a comparison operator between a table or index column and a constant value for use in scans and searches.

## Definition


## Detailed Description
ScanKeyData is a versatile structure that serves multiple purposes in PostgreSQL's scanning operations:

1. **Basic Comparisons**: Represents conditions like "column = value" or "column > value" where the index column is the left argument of a binary operator.

2. **Array Operations**: Can represent ScalarArrayOpExpr conditions ("column op ANY(ARRAY[...])") when the SK_SEARCHARRAY flag is set, with sk_argument containing an array of values for OR operations.

3. **NULL Conditions**: Supports "column IS NULL" and "column IS NOT NULL" conditions via SK_SEARCHNULL and SK_SEARCHNOTNULL flags respectively.

4. **Ordering Operations**: Can represent ordering requirements ("ORDER BY indexedcol op constant") marked with SK_ORDER_BY flag.

5. **Row Comparisons**: Supports ordered tuple comparisons like "(x, y) > (c1, c2)" through a header-subsidiary array structure (btree indexes only).

When used in arrays, multiple ScanKeys are implicitly ANDed together. The structure is designed to work with both heap scans and index scans, though some features (like search arrays and null searches) are index-specific.

## Parameters / Member Variables
- : Control flags indicating the type of scan key operation (SK_SEARCHARRAY, SK_SEARCHNULL, SK_SEARCHNOTNULL, SK_ORDER_BY, SK_ROW_HEADER, SK_ROW_MEMBER, SK_ROW_END, etc.)
- : The column number in the table or index that this scan key applies to
- : Strategy number identifying the operator for index scans (not used for heap scans)
- : Strategy subtype for the operator (not used for heap scans)
- : Collation OID to use for collation-sensitive operators
- : Function manager info structure containing lookup information for the comparison function
- : The constant value to compare against (or array of values for SK_SEARCHARRAY operations)

## Dependencies
- Functions called/Symbols referenced:
  - StrategyNumber (typedef for operator strategy)
  - AttrNumber (typedef for attribute numbers)
  - Oid (typedef for object identifiers)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager information structure)
  - Datum (generic data type for PostgreSQL values)

- Called from (representative examples):
  - Various index access method functions
  - Heap scan initialization routines
  - [Query](../Q/Query.md) executor scan operations
  - Index build and maintenance operations

## Notes and Other Information
- [ScanKeyData](ScanKeyData.md) is often used via the ScanKey typedef (ScanKeyData *)
- Not all index access methods support all ScanKey features (e.g., search arrays, null searches)
- For heap scans, sk_strategy and sk_subtype fields are unused and may be set to InvalidStrategy/InvalidOid
- Row comparison functionality is currently only implemented for btree indexes
- The structure supports both system-wide flags (bits 0-15) and index-AM-specific flags (bits 16-31)
- When representing access method support procedure invocations, sk_strategy/sk_subtype may not be meaningful
- The design allows for future extension to support unary indexable operators (not currently implemented)