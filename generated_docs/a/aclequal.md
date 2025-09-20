# aclequal

## Location
[src/backend/utils/adt/acl.c:559-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L559-L589)

## Overview
Compares two Access Control Lists (ACLs) for exact equality, checking if they contain identical ACL items in the same order.

## Definition

```c
bool
aclequal(const Acl *left_acl, const Acl *right_acl)
```
## Detailed Description
The aclequal function performs a byte-by-byte comparison of two ACL structures to determine if they are exactly equal. It handles edge cases where one or both ACLs might be NULL or empty, and performs efficient comparison using memcmp for the ACL data arrays. The function returns true only if both ACLs have the same number of items and identical content in the same order. Note that this function will not detect equality if the ACLs contain the same items in different orders - for such cases, the inputs should be sorted first using aclitemsort().

## Parameters / Member Variables
- : Pointer to the first ACL to compare (may be NULL)
- : Pointer to the second ACL to compare (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - ACL_NUM (macro to get number of ACL items)
  - ACL_DAT (macro to get ACL data array)
  - memcmp (standard library function for memory comparison)
- Called from (representative examples):
  - [SetDefaultACL](../S/SetDefaultACL.md) (src/backend/catalog/aclchk.c:1343)
  - [ExecGrant_Parameter](../E/ExecGrant_Parameter.md) (src/backend/catalog/aclchk.c:2572)
  - [get_user_default_acl](../g/get_user_default_acl.md) (src/backend/catalog/aclchk.c:4372)

## Notes and Other Information
- Returns true if both ACLs are NULL or empty
- Does not perform order-independent comparison - items must be in identical order
- Uses efficient memcmp for bulk comparison after validating ACL structure
- Part of PostgreSQL's access control system for managing object permissions