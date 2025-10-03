# _hash_binsearch_last

## Location
[src/backend/access/hash/hashutil.c:388-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L388-L421)

## Overview
Returns the offset number of the last occurrence of a specified hash value in a hash index page, designed for backwards scanning operations.

## Definition
OffsetNumber _hash_binsearch_last(Page page, uint32 hash_value)

## Detailed Description
This function is similar to _hash_binsearch but with a key difference: when there are multiple index tuples with the same hash value, it returns the offset of the last one instead of the first one. The possible range of outputs is 0..maxoffset rather than 1..maxoffset+1. This makes it particularly useful for starting a new page during backwards scan operations.

The algorithm uses binary search but with a slightly different approach than the standard version. It adjusts the midpoint calculation by adding 1 to ensure proper handling of the "last occurrence" requirement, and the comparison logic is inverted to find the rightmost position where the hash value matches.

## Parameters / Member Variables
- `page`: The hash index page to search within
- `hash_value`: The target hash value to locate the last occurrence of

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - FirstOffsetNumber  
  - OffsetNumberIsValid
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [_hash_get_indextuple_hashkey](_hash_get_indextuple_hashkey.md)
- Called from (representative examples):
  - [_hash_readpage](_hash_readpage.md)

## Simplified Source
```c
OffsetNumber _hash_binsearch_last(Page page, uint32 hash_value) {
    // Initialize search bounds for finding last occurrence
    OffsetNumber upper = PageGetMaxOffsetNumber(page);
    OffsetNumber lower = FirstOffsetNumber - 1;

    // Binary search: lower <= desired position <= upper
    while (upper > lower) {
        OffsetNumber mid = (upper + lower + 1) / 2;  // +1 for last occurrence

        // Get hash key from tuple at middle position
        IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, mid));
        uint32 hashkey = _hash_get_indextuple_hashkey(itup);

        // Adjust search bounds (inverted logic for last occurrence)
        if (hashkey > hash_value)
            upper = mid - 1;
        else
            lower = mid;
    }

    return lower;
}
```

## Notes and Other Information
The function is specifically optimized for backwards scanning scenarios where you need to start from the last occurrence of a particular hash value. The different bounds (0..maxoffset vs 1..maxoffset+1) reflect this specialized use case. The midpoint calculation uses (upper + lower + 1) / 2 instead of (upper + lower) / 2 to ensure correct behavior when searching for the last occurrence.