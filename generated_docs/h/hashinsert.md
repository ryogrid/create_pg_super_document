# hashinsert

## Location
[src/backend/access/hash/hash.c:251-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L251-L282)

## Overview
Inserts a single index tuple into an existing hash index by hashing the key values and placing the tuple in the appropriate bucket.

## Definition
```c
bool hashinsert(Relation rel, Datum *values, bool *isnull, ItemPointer ht_ctid, Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged, IndexInfo *indexInfo)
```

## Detailed Description
The hashinsert function handles insertion of individual tuples into an existing hash index during normal database operations (INSERT, UPDATE, etc.). It converts the provided heap tuple values into a hash index key, creates an IndexTuple pointing to the heap tuple, and inserts it into the appropriate hash bucket.

The function follows a standard pattern: first converting the raw values into hash key format using _hash_convert_tuple, then creating an IndexTuple with the hash key, and finally calling _hash_doinsert to handle the actual bucket insertion logic including bucket splitting if necessary.

Unlike unique indexes, hash indexes don't enforce uniqueness constraints, so the checkUnique parameter is effectively ignored and the function always returns false to indicate no uniqueness violation occurred.

## Parameters / Member Variables
- `rel`: The hash index relation
- `values`: Array of Datum values to be indexed
- `isnull`: Array of null indicators for the values
- `ht_ctid`: ItemPointer to the heap tuple being indexed
- `heapRel`: The heap relation containing the tuple
- `checkUnique`: Uniqueness checking mode (ignored for hash indexes)
- `indexUnchanged`: Whether the index value is unchanged (optimization hint)
- `indexInfo`: Index metadata information

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_convert_tuple](_hash_convert_tuple.md)
  - [index_form_tuple](../i/index_form_tuple.md)
  - RelationGetDescr
  - [_hash_doinsert](_hash_doinsert.md)
  - [pfree](../p/pfree.md)
- Called from:
  - [hashhandler](hashhandler.md) (as amroutine->aminsert callback)
  - PostgreSQL index insertion system during DML operations

## Notes and Other Information
- Always returns false since hash indexes don't support uniqueness constraints
- Silently fails (returns false) if tuple cannot be converted to a valid hash key
- Properly manages memory by freeing the temporary IndexTuple after insertion
- The function signature matches the standard IndexAmRoutine->aminsert interface
- [Hash](../H/Hash.md) indexes support only single-column keys, reflected in the single-element arrays used internally

## Simplified Source

```c
bool hashinsert(Relation rel, Datum *values, bool *isnull,
               ItemPointer ht_ctid, Relation heapRel,
               IndexUniqueCheck checkUnique,
               bool indexUnchanged,
               IndexInfo *indexInfo)
{
    Datum index_values[1];
    bool index_isnull[1];
    IndexTuple itup;

    // Convert heap tuple values to hash key
    if (!_hash_convert_tuple(rel, values, isnull, index_values, index_isnull))
        return false;

    // Create index tuple pointing to heap tuple
    itup = index_form_tuple(RelationGetDescr(rel), index_values, index_isnull);
    itup->t_tid = *ht_ctid;

    // Insert into appropriate hash bucket
    _hash_doinsert(rel, itup, heapRel, false);

    // Clean up memory
    pfree(itup);

    return false;  // Hash indexes don't support uniqueness
}
```