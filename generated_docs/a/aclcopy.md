# aclcopy

## Location
[src/backend/utils/adt/acl.c:457-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L457-L476)

## Overview
Creates a deep copy of an existing Access Control List (ACL), duplicating all entries and structure in newly allocated memory.

## Definition
```c
Acl *aclcopy(const Acl *orig_acl)
```

## Detailed Description
The `aclcopy` function performs a complete deep copy of an ACL structure, creating an independent duplicate that can be modified without affecting the original. The function allocates new memory space sufficient to hold the same number of ACL entries as the source, then performs a byte-for-byte copy of all ACL item data. This is essential for operations that need to modify ACL permissions while preserving the original ACL intact.

## Parameters / Member Variables
- `orig_acl`: Pointer to the source ACL to be copied (const to ensure it remains unmodified)

## Dependencies
- Functions called/Symbols referenced:
  - `[allocacl](allocacl.md)` - Allocates memory for the new ACL with specified entry count
  - `ACL_NUM` - Macro to get the number of entries in an ACL
  - `ACL_DAT` - Macro to access the data portion of an ACL
  - `memcpy` - Standard library function for memory copying
  - `AclItem` - Structure type representing individual ACL entries
  - `[Acl](../A/Acl.md)` - ACL structure type definition
- Called from (representative examples):
  - `[SetDefaultACL](../S/SetDefaultACL.md)` - When setting default ACL permissions
  - `[ExecGrant_Relation](../E/ExecGrant_Relation.md)` - During relation permission grants
  - `[aclmerge](aclmerge.md)` - As part of ACL merging operations

## Notes and Other Information
- The function creates a completely independent copy; changes to the copy do not affect the original
- Memory for the new ACL is allocated in the current memory context
- The copy includes all ACL entries with their complete permission information
- Essential for implementing copy-on-write semantics in ACL operations
- Used extensively in permission management operations where original ACLs must be preserved