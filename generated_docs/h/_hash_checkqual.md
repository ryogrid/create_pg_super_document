# _hash_checkqual

## Location
[src/backend/access/hash/hashutil.c:31-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L31-L81)

## Overview
Function that determines whether an index tuple satisfies the scan conditions during a hash index search.

## Definition

```c
bool
_hash_checkqual(IndexScanDesc scan, IndexTuple itup)
```
## Detailed Description
This function is designed to evaluate whether a given index tuple meets the scan conditions specified in an index scan descriptor. However, the current implementation always returns  and does not perform actual qualification checking. The function contains commented-out code (under ) that shows the intended implementation for checking scan conditions.

The function's design reflects a limitation in the hash index implementation: it cannot directly check scan conditions on the index tuple because it lacks access to the original index entry value needed for the scan key function (). Instead, the hash index system relies on the  function to set a recheck flag, which causes the main indexscan code to perform the qualification checking at a higher level.

## Parameters / Member Variables
- : IndexScanDesc pointer containing the scan descriptor with key data and scan conditions
- : IndexTuple pointer to the index tuple being evaluated for qualification

## Dependencies
- Functions called/Symbols referenced:
  - [IndexScanDesc](../I/IndexScanDesc.md) (scan descriptor type)
  - [IndexTuple](../I/IndexTuple.md) (index tuple type)
  - ScanKey (scan key type - in commented code)
  - [index_getattr](../i/index_getattr.md) (for extracting tuple attributes - in commented code)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (for calling scan key functions - in commented code)
- Called from (representative examples):
  - [_hash_load_qualified_items](_hash_load_qualified_items.md) (in hashsearch.c at lines 637 and 683)

## Notes and Other Information
- The function currently serves as a placeholder that always returns
- The actual qualification checking is deferred to the main indexscan code via a recheck mechanism
- The commented-out implementation shows how proper qualification checking would work if the necessary data were available
- This design is specific to hash indexes due to their inability to directly evaluate scan conditions on stored hash values
- The function is part of PostgreSQL's hash index access method implementation

## Simplified Source

```c
bool _hash_checkqual(IndexScanDesc scan, IndexTuple itup)
{
    /*
     * Hash indexes cannot check scan conditions directly because we don't
     * have the original index entry value for the sk_func. The recheck flag
     * is set by hashgettuple to defer qualification to the main indexscan code.
     */
    return true;
}
```