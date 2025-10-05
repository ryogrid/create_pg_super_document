# _bt_keep_natts

## Location
[src/backend/access/nbtree/nbtutils.c:4802-4875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4802-L4875)

## Overview
The _bt_keep_natts function determines the minimum number of key attributes that must be retained when creating a truncated pivot tuple during B-tree page splits.

## Definition
```c
static int _bt_keep_natts(Relation rel, IndexTuple lastleft, IndexTuple firstright, BTScanInsert itup_key)
```

## Detailed Description
This function analyzes two tuples that enclose a split point and determines how many leading key attributes are required to properly distinguish between them. It performs attribute-by-attribute comparison using the provided scan key to find the first differing attribute position.

The algorithm works by:
1. Checking if the index uses heapkeyspace (required for safe truncation)
2. Comparing corresponding attributes from both tuples using the scan key's comparison functions
3. Stopping at the first attribute where values differ (including null vs non-null)
4. Returning the number of attributes needed to maintain proper ordering

For non-heapkeyspace indexes, the function always returns the full number of key attributes since truncation could break search operations where truncated attributes are treated as minus infinity.

The function may return a value one greater than the number of key attributes, indicating that a heap TID tiebreaker is needed when all key attributes are equal between the two tuples.

## Parameters / Member Variables
- `rel`: Relation object for the B-tree index
- `lastleft`: Last tuple that will remain on the left page after split
- `firstright`: First tuple that will go to the right page after split  
- `itup_key`: Insertion scan key containing comparison functions and collation info

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [index_getattr](../i/index_getattr.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [_bt_keep_natts_fast](_bt_keep_natts_fast.md)
  - BTScanInsert (type)
  - ScanKey (type)
- Called from (representative examples):
  - [_bt_truncate](_bt_truncate.md)

## Notes and Other Information
This function is critical for B-tree suffix truncation optimization. It ensures that truncated pivot tuples retain just enough information to maintain correct ordering while maximizing space savings. The function includes an assertion to verify consistency with _bt_keep_natts_fast() for indexes that support fast equality image comparisons. The heapkeyspace requirement reflects the need for proper handling of truncated attributes in comparison operations.

## Simplified Source

```c
static int
_bt_keep_natts(Relation rel, IndexTuple lastleft, IndexTuple firstright,
               BTScanInsert itup_key)
{
    int nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    TupleDesc itupdesc = RelationGetDescr(rel);
    int keepnatts;
    ScanKey scankey;

    // Can't truncate safely in non-heapkeyspace indexes
    if (!itup_key->heapkeyspace)
        return nkeyatts;

    scankey = itup_key->scankeys;
    keepnatts = 1;

    // Compare attributes until we find a difference
    for (int attnum = 1; attnum <= nkeyatts; attnum++, scankey++) {
        Datum datum1, datum2;
        bool isNull1, isNull2;

        // Extract attribute values from both tuples
        datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
        datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);

        // Different null status means we found distinguishing attribute
        if (isNull1 != isNull2)
            break;

        // Compare non-null values using scan key's comparison function
        if (!isNull1 &&
            DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
                                            scankey->sk_collation,
                                            datum1, datum2)) != 0)
            break;

        keepnatts++;
    }

    // Verify consistency with fast path in allequalimage indexes
    Assert(!itup_key->allequalimage ||
           keepnatts == _bt_keep_natts_fast(rel, lastleft, firstright));

    return keepnatts;
}
```