# aclmerge

## Location
[src/backend/utils/adt/acl.c:501-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L501-L544)

## Overview
Intelligently merges two Access Control Lists (ACLs) by properly combining permissions, eliminating redundant entries and resolving conflicts for the same grantees.

## Definition
```c
Acl *aclmerge(const Acl *left_acl, const Acl *right_acl, Oid ownerId)
```

## Detailed Description
The `aclmerge` function provides sophisticated ACL combination logic that goes beyond simple concatenation. It creates a properly merged ACL with no redundant entries by iteratively adding each item from the right ACL to a copy of the left ACL using `aclupdate`. This ensures that when the same grantee appears in both ACLs, their permissions are properly combined rather than creating duplicate entries. The function handles edge cases including NULL inputs and empty ACLs, returning appropriate results for each scenario.

## Parameters / Member Variables
- `left_acl`: First ACL to merge (serves as the base)
- `right_acl`: Second ACL whose entries will be merged into the first
- `ownerId`: Object identifier of the owner, used for permission validation during merge operations

## Dependencies
- Functions called/Symbols referenced:
  - `aclcopy` - Creates copies of ACLs for modification
  - `aclupdate` - Updates ACL with individual items, handling permission merging
  - `ACL_NUM` - Macro to get the number of entries in an ACL
  - `ACL_DAT` - Macro to access the data portion of an ACL
  - `pfree` - PostgreSQL memory deallocation function
  - `ACL_MODECHG_ADD` - Constant indicating permission addition mode
  - `DROP_RESTRICT` - Constant for drop behavior specification
  - `Acl` - ACL structure type definition
  - `AclItem` - Structure type representing individual ACL entries
- Called from (representative examples):
  - `get_user_default_acl` - When retrieving user default ACL permissions
  - Referenced in `AclResult` type definitions

## Notes and Other Information
- Unlike `aclconcat`, this function produces a clean ACL with no redundant entries
- Returns NULL if both input ACLs are NULL or empty
- Returns a copy of the non-empty ACL if only one input is non-empty
- Uses iterative `aclupdate` calls to properly merge permissions for duplicate grantees
- Memory management: Frees intermediate ACL copies to prevent memory leaks
- The `ownerId` parameter is essential for proper permission validation and conflict resolution
- More computationally expensive than `aclconcat` but produces cleaner, more consistent results
- Preferred function for combining ACLs when logical correctness is more important than performance