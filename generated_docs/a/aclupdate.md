# aclupdate

## Location
[src/backend/utils/adt/acl.c:992-1118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L992-L1118)

## Overview
Updates an ACL (Access Control List) array to add, remove, or replace specified privileges for a given grantee-grantor pair.

## Definition
```c
Acl *aclupdate(const Acl *old_acl, const AclItem *mod_aip, int modechg, Oid ownerId, DropBehavior behavior)
```

## Detailed Description
This function is the core mechanism for modifying ACL arrays in PostgreSQL. It creates a modified copy of an existing ACL by applying privilege changes specified in the modification item. The function handles three types of operations: adding privileges (ACL_MODECHG_ADD), removing privileges (ACL_MODECHG_DEL), or setting privileges to an exact value (ACL_MODECHG_EQL). When granting options are involved, it checks for circular grant relationships to prevent privilege loops. For revoke operations that remove grant options, it can cascade the revocation to dependent privileges.

## Parameters / Member Variables
- `old_acl` (const Acl *): The input ACL array to be modified
- `mod_aip` (const AclItem *): Defines the privileges to be added, removed, or substituted, including grantee and grantor information
- `modechg` (int): Operation type - ACL_MODECHG_ADD, ACL_MODECHG_DEL, or ACL_MODECHG_EQL
- `ownerId` (Oid): Object identifier of the object owner (relevant for cascading revoke operations)
- `behavior` (DropBehavior): RESTRICT or CASCADE behavior for recursive privilege removal

## Dependencies
- Functions called/Symbols referenced:
  - [check_acl](../c/check_acl.md): Validates ACL structure
  - [check_circularity](../c/check_circularity.md): Prevents circular grant relationships
  - [aclitem_match](aclitem_match.md): Matches grantee-grantor pairs in ACL items
  - [allocacl](allocacl.md): Allocates memory for new ACL array
  - [recursive_revoke](../r/recursive_revoke.md): Handles cascading privilege revocation
  - Various ACL manipulation macros (ACLITEM_GET_RIGHTS, ACLITEM_SET_RIGHTS, etc.)
- Called from (representative examples):
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md): Merging ACLs during privilege operations
  - [aclmerge](aclmerge.md): ACL merging operations
  - [check_circularity](../c/check_circularity.md): Circular dependency detection
  - [recursive_revoke](../r/recursive_revoke.md): Cascading revoke operations

## Notes and Other Information
- Returns a modified copy of the input ACL; the original ACL is not changed
- Caller is responsible for detoasting the input ACL if needed
- Automatically removes ACL entries that have no remaining privileges
- For grant option removal, performs cascading revoke when behavior is CASCADE
- Cannot handle cascading revoke for PUBLIC grantees
- Maintains ACL array structure and proper memory management
- Located in src/backend/utils/adt/acl.c:992-1118

## Simplified Source

```c
Acl *aclupdate(const Acl *old_acl, const AclItem *mod_aip,
               int modechg, Oid ownerId, DropBehavior behavior) {
    Acl *new_acl = NULL;
    AclItem *new_aip = NULL;
    int dst, num;

    // Validate input ACL
    check_acl(old_acl);

    // Check for circular grants when adding grant options
    if (modechg != ACL_MODECHG_DEL &&
        ACLITEM_GET_GOPTIONS(*mod_aip) != ACL_NO_RIGHTS) {
        check_circularity(old_acl, mod_aip, ownerId);
    }

    num = ACL_NUM(old_acl);

    // Search for existing entry matching grantee/grantor
    for (dst = 0; dst < num; ++dst) {
        if (aclitem_match(mod_aip, old_aip + dst)) {
            // Found match: create copy and modify in place
            new_acl = allocacl(num);
            new_aip = ACL_DAT(new_acl);
            memcpy(new_acl, old_acl, ACL_SIZE(old_acl));
            break;
        }
    }

    if (dst == num) {
        // No match found: append new item
        new_acl = allocacl(num + 1);
        new_aip = ACL_DAT(new_acl);
        memcpy(new_aip, old_aip, num * sizeof(AclItem));

        // Initialize new entry with no permissions
        new_aip[dst].ai_grantee = mod_aip->ai_grantee;
        new_aip[dst].ai_grantor = mod_aip->ai_grantor;
        ACLITEM_SET_PRIVS_GOPTIONS(new_aip[dst], ACL_NO_RIGHTS, ACL_NO_RIGHTS);
        num++;
    }

    // Apply privilege change based on operation type
    AclMode old_rights = ACLITEM_GET_RIGHTS(new_aip[dst]);
    AclMode old_goptions = ACLITEM_GET_GOPTIONS(new_aip[dst]);

    switch (modechg) {
        case ACL_MODECHG_ADD:
            // Add specified rights to existing
            ACLITEM_SET_RIGHTS(new_aip[dst],
                              old_rights | ACLITEM_GET_RIGHTS(*mod_aip));
            break;
        case ACL_MODECHG_DEL:
            // Remove specified rights
            ACLITEM_SET_RIGHTS(new_aip[dst],
                              old_rights & ~ACLITEM_GET_RIGHTS(*mod_aip));
            break;
        case ACL_MODECHG_EQL:
            // Set rights to exact value
            ACLITEM_SET_RIGHTS(new_aip[dst], ACLITEM_GET_RIGHTS(*mod_aip));
            break;
    }

    // Remove entry if no privileges remain
    AclMode new_rights = ACLITEM_GET_RIGHTS(new_aip[dst]);
    if (new_rights == ACL_NO_RIGHTS) {
        // Shift remaining entries down
        memmove(new_aip + dst, new_aip + dst + 1,
                (num - dst - 1) * sizeof(AclItem));
        // Adjust ACL size
        ARR_DIMS(new_acl)[0] = num - 1;
        SET_VARSIZE(new_acl, ACL_N_SIZE(num - 1));
    }

    // Handle cascading revoke for removed grant options
    AclMode new_goptions = ACLITEM_GET_GOPTIONS(new_aip[dst]);
    if ((old_goptions & ~new_goptions) != 0) {
        new_acl = recursive_revoke(new_acl, mod_aip->ai_grantee,
                                   (old_goptions & ~new_goptions),
                                   ownerId, behavior);
    }

    return new_acl;
}
```