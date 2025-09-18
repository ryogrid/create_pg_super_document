# aclitem_match

## Location
src/backend/utils/adt/acl.c: 713 - 723

## Overview
Determines if two AclItem structures represent the same grantee-grantor relationship, ignoring the specific privileges granted.

## Definition
```c
static bool aclitem_match(const AclItem *a1, const AclItem *a2)
```

## Detailed Description
The aclitem_match function performs a comparison between two AclItem structures to determine if they represent the same access control relationship. It considers two AclItems to match if they have identical grantee (the role receiving privileges) and grantor (the role granting privileges) fields. The specific privileges and grant options are explicitly ignored in this comparison. This function is essential for ACL operations that need to locate existing ACL entries for modification or removal, regardless of the current privilege state.

## Parameters / Member Variables
- `a1`: Pointer to the first AclItem to compare
- `a2`: Pointer to the second AclItem to compare

## Dependencies
- Functions called/Symbols referenced:
  - Direct field access to ai_grantee and ai_grantor members of AclItem
- Called from (representative examples):
  - aclupdate (src/backend/utils/adt/acl.c:1025)
  - aclnewowner (src/backend/utils/adt/acl.c:1187)

## Notes and Other Information
- This is a static (internal) helper function within the ACL module
- Ignores privileges and grant options, focusing only on the relationship identity
- Used primarily for finding existing ACL entries that need to be updated or replaced
- Simple equality check on two OID fields (ai_grantee and ai_grantor)
- Essential for ACL consolidation and update operations where multiple privileges may exist for the same grantee-grantor pair