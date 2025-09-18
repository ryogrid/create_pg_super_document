# list_union_oid

## Location
[src/backend/nodes/list.c:1136-1173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1136-L1173)

## Overview
Creates a new list containing the union of two OID (Object Identifier) lists, eliminating duplicate values.

## Definition
```c
List *list_union_oid(const List *list1, const List *list2)
```

## Detailed Description
This function performs a set union operation on two lists containing OID (Object Identifier) values. It creates a new list that contains all unique OID values from both input lists. The function starts by copying the first list, then iterates through the second list and appends any OIDs that are not already present in the result. This ensures no duplicate values exist in the final union.

The function includes assertions to verify that both input lists contain only OID values using `IsOidList()`. After construction, it validates the result using `check_list_invariants()` to ensure list consistency.

## Parameters / Member Variables
- `list1`: The first input list of OIDs (const List *)
- `list2`: The second input list of OIDs (const List *)

## Dependencies
- Functions called/Symbols referenced:
  - IsOidList (validation)
  - [list_copy](list_copy.md) (copy first list)
  - [list_member_oid](list_member_oid.md) (check membership)
  - lfirst_oid (extract OID values)
  - lappend_oid (append OID values)
  - [check_list_invariants](../c/check_list_invariants.md) (validation)
- Called from (representative examples):
  - forfive (pg_list.h:648)

## Notes and Other Information
- Both input lists must contain only OID values, enforced by assertions
- The function allocates a new list; callers are responsible for memory management
- Order of elements follows list1 first, then unique elements from list2
- Time complexity is O(n*m) where n and m are the sizes of the input lists due to membership testing
- OIDs are PostgreSQL's internal object identifiers used to reference database objects