# mbms_is_member

## Location
[src/backend/nodes/multibitmapset.c:126-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/multibitmapset.c#L126-L145)

## Overview
Tests whether a specific member (identified by list index and bit index) is present in a multibitmapset.

## Definition

```c
bool
mbms_is_member(int listidx, int bitidx, const List *a)
```
## Detailed Description
This function checks for membership in a multibitmapset, which is represented as a List of Bitmapset structures. It determines whether a specific bit is set within a specific Bitmapset in the List. The function is analogous to bms_is_member but operates on the more complex multibitmapset data structure.

The function first validates that both indices are non-negative, then checks if the list index is within bounds. If the list index is beyond the List's length, it returns false (indicating the member is not present). Otherwise, it retrieves the target Bitmapset and uses bms_is_member to check if the specified bit is set.

## Parameters / Member Variables
- `listidx`: Zero-based index of the List element (Bitmapset) to check
- `bitidx`: Bit number to test within the target Bitmapset
- `*a`: The List representing the multibitmapset to query (read-only)
## Dependencies
- Functions called/Symbols referenced:
  - list_nth_node
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - (No specific usage examples found in the indexed code)

## Notes and Other Information
- Returns true if the specified member is present, false otherwise
- Both listidx and bitidx must be non-negative; the function will throw an ERROR if negative values are provided
- If listidx is beyond the List's length, the function returns false rather than extending the List
- This is a read-only operation that does not modify the input multibitmapset
- Used for membership testing in PostgreSQL's query optimizer algorithms
- The function includes a comment suggesting that returning false for negative indexes might be better than throwing an error