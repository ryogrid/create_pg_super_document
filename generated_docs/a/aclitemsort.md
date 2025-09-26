# aclitemsort

## Location
[src/backend/utils/adt/acl.c:545-558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L545-L558)

## Overview
Sorts the entries in an Access Control List (ACL) into a consistent canonical order to enable reliable comparison and processing.

## Definition
```c
void aclitemsort(Acl *acl)
```

## Detailed Description
The `aclitemsort` function arranges ACL entries in a deterministic order using the standard library's `qsort` function with a custom comparator. This sorting is crucial for ensuring that ACLs with identical permissions but different entry orders are considered equivalent and can be reliably compared. The function operates in-place, modifying the original ACL structure. It includes a safety check to avoid unnecessary work when the ACL is NULL or contains only one or zero entries.

## Parameters / Member Variables
- `acl`: Pointer to the ACL to be sorted (modified in place)

## Dependencies
- Functions called/Symbols referenced:
  - `qsort` - Standard library sorting function
  - `aclitemComparator` - Custom comparison function for ACL items
  - `ACL_NUM` - Macro to get the number of entries in an ACL
  - `ACL_DAT` - Macro to access the data portion of an ACL
  - `AclItem` - Structure type representing individual ACL entries
  - `Acl` - ACL structure type definition
- Called from (representative examples):
  - `SetDefaultACL` - After setting default ACL permissions to ensure consistent ordering
  - `get_user_default_acl` - When retrieving user default ACL permissions
  - Referenced in `AclResult` type definitions

## Notes and Other Information
- Sorting is performed in-place, modifying the original ACL structure
- The sort order is arbitrary but consistent, determined by `aclitemComparator`
- Essential for ACL canonicalization, enabling reliable equality testing
- No-op for NULL ACLs or ACLs with 0-1 entries (optimization)
- Typically called after ACL construction or modification to ensure consistent state
- The consistent ordering helps with debugging and reduces false differences in ACL comparisons
- Performance: O(n log n) where n is the number of ACL entries