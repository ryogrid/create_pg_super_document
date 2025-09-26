# _hash_binsearch

## Location
[src/backend/access/hash/hashutil.c:350-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L350-L387)

## Overview
Returns the offset number in a hash index page where a specified hash value should be sought or inserted using binary search.

## Definition
OffsetNumber _hash_binsearch(Page page, uint32 hash_value)

## Detailed Description
This function performs a binary search on a hash index page to find the appropriate position for a given hash value. It relies on the assumption that existing index tuple entries are ordered by their hash keys. The function returns the offset of the first index entry that has a hashkey greater than or equal to the target hash_value, or returns the page's maximum offset plus one if the hash_value is greater than all existing hash keys. This position indicates either where to start searching for an existing entry or where to insert a new entry to maintain the sorted order.

The algorithm maintains a loop invariant where the desired position is always between the lower and upper bounds (inclusive). It uses the standard binary search technique of comparing the middle element and adjusting the search bounds accordingly.

## Parameters / Member Variables
- : The hash index page to search within
- : The target hash value to locate or find insertion position for

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - FirstOffsetNumber
  - OffsetNumberIsValid
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [_hash_get_indextuple_hashkey](_hash_get_indextuple_hashkey.md)
- Called from (representative examples):
  - [_hash_pgaddtup](_hash_pgaddtup.md)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md)
  - [_hash_readpage](_hash_readpage.md)

## Notes and Other Information
The function assumes that index tuples on the page are already sorted by hash key, which is a fundamental requirement for hash indexes. The binary search approach provides O(log n) time complexity for finding the correct position, making it efficient even for pages with many index tuples. The returned offset can be used directly for insertion operations or as a starting point for sequential searches.