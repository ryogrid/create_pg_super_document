# gistcanreturn

## Location
[src/backend/access/gist/gistget.c:793-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistget.c#L793-L801)

## Overview
gistcanreturn determines whether a specific index column supports index-only scans by checking for fetch functions, included attributes, or absence of compression.

## Definition
```c
bool gistcanreturn(Relation index, int attno)
```

## Detailed Description
gistcanreturn evaluates whether a given column in a GiST index can participate in index-only scans. Index-only scans are an optimization where query results can be satisfied entirely from index data without accessing the heap table, providing significant performance benefits.

The function determines this capability based on three conditions:
1. **Included attributes**: Columns beyond the key attributes (attno > number of key attributes) are always returnable as they're stored uncompressed in the index
2. **Fetch function availability**: If the operator class implements a GIST_FETCH_PROC, it can reconstruct original values from index tuples
3. **No compression**: If no GIST_COMPRESS_PROC is defined, the index stores values in their original form, making them directly returnable

This function is critical for the query planner's decision to use index-only scan plans, which can dramatically improve performance for queries that only need indexed columns.

## Parameters
- `index`: Relation representing the GiST index to check
- `attno`: Attribute number (1-based) of the column to check for returnability

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes (get count of key attributes)
  - [index_getprocid](../i/index_getprocid.md) (look up operator class procedures)
  - GIST_FETCH_PROC (fetch function procedure number)
  - GIST_COMPRESS_PROC (compression function procedure number)
- Called from (representative examples):
  - [gisthandler](gisthandler.md) (index AM handler setup)

## Notes and Other Information
- Returns true if the column can be returned in index-only scans, false otherwise
- Part of PostgreSQL's index-only scan optimization framework
- Critical for query planning decisions regarding scan method selection
- Included attributes (non-key columns) are always returnable regardless of operator class
- The presence of a fetch function allows reconstruction of original values from compressed index data
- Absence of compression means values are stored directly and can be returned as-is
- Used by the query planner to determine feasibility of index-only scan plans

## Simplified Source

```c
bool
gistcanreturn(Relation index, int attno)
{
    // Check three conditions for index-only scan support:
    // 1. Included attributes (beyond key attributes) are always returnable
    // 2. Operator class has a fetch function to reconstruct values
    // 3. No compression function means values stored directly

    if (attno > IndexRelationGetNumberOfKeyAttributes(index) ||  // Included attribute
        OidIsValid(index_getprocid(index, attno, GIST_FETCH_PROC)) ||  // Has fetch function
        !OidIsValid(index_getprocid(index, attno, GIST_COMPRESS_PROC)))  // No compression
        return true;
    else
        return false;
}
```