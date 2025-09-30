# aclnewowner

## Location
[src/backend/utils/adt/acl.c:1119-1221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1119-L1221)

## Overview
Updates an ACL array to reflect a change of ownership, substituting the new owner ID for the old owner ID wherever it appears as either grantor or grantee.

## Definition
```c
Acl *aclnewowner(const Acl *old_acl, Oid oldOwnerId, Oid newOwnerId)
```

## Detailed Description
This function creates a modified copy of an ACL array by replacing all occurrences of the old owner ID with the new owner ID, whether the old owner appears as a grantor or grantee. The function handles the complexities that arise when the new owner ID already exists in the ACL, which can create duplicate entries. It implements a deduplication algorithm that merges duplicate entries by combining their privileges, ensuring the resulting ACL has no duplicate grantee-grantor pairs. Despite its name suggesting it only works with actual owners, the function will substitute any role ID mentioned in the ACL.

## Parameters / Member Variables
- `old_acl` (const Acl *): The input ACL array that must not be NULL
- `oldOwnerId` (Oid): Object identifier of the old owner role to be replaced
- `newOwnerId` (Oid): Object identifier of the new owner role to substitute

## Dependencies
- Functions called/Symbols referenced:
  - [check_acl](../c/check_acl.md): Validates the input ACL structure
  - [allocacl](allocacl.md): Allocates memory for the new ACL array
  - [aclitem_match](aclitem_match.md): Determines if two ACL items have the same grantee-grantor pair
  - ACL manipulation macros (ACL_NUM, ACL_DAT, ACLITEM_GET_RIGHTS, ACLITEM_SET_RIGHTS, etc.)
  - Memory management functions (memcpy, ARR_DIMS, SET_VARSIZE)
- Called from (representative examples):
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md): Generic object ownership change
  - [AlterDatabaseOwner](../A/AlterDatabaseOwner.md): Database ownership changes
  - [ATExecChangeOwner](../A/ATExecChangeOwner.md): Table ownership changes
  - [AlterSchemaOwner_internal](../A/AlterSchemaOwner_internal.md): Schema ownership changes
  - Various other ALTER OWNER commands

## Notes and Other Information
- Returns a modified copy; the input ACL is not changed
- Caller is responsible for detoasting the input ACL if needed
- Uses an O(N²) algorithm for duplicate detection and merging when necessary
- Automatically removes privilege-free entries from the result
- The deduplication process is only triggered when the new owner ID was already present in the original ACL
- Function name is somewhat misleading as it works for any role substitution, not just actual owners
- Primarily used in ALTER OWNER operations across different PostgreSQL object types
- Located in src/backend/utils/adt/acl.c:1119-1221

## Simplified Source

```c
Acl *aclnewowner(const Acl *old_acl, Oid oldOwnerId, Oid newOwnerId)
{
    Acl *new_acl;
    AclItem *new_aip;
    AclItem *old_aip;
    bool newpresent = false;
    int dst, src, targ, num;

    check_acl(old_acl);

    // Create copy of ACL with owner ID substitutions
    num = ACL_NUM(old_acl);
    old_aip = ACL_DAT(old_acl);
    new_acl = allocacl(num);
    new_aip = ACL_DAT(new_acl);
    memcpy(new_aip, old_aip, num * sizeof(AclItem));

    // Replace old owner ID with new owner ID in all entries
    for (dst = 0; dst < num; dst++)
    {
        if (new_aip[dst].ai_grantor == oldOwnerId)
            new_aip[dst].ai_grantor = newOwnerId;
        else if (new_aip[dst].ai_grantor == newOwnerId)
            newpresent = true;

        if (new_aip[dst].ai_grantee == oldOwnerId)
            new_aip[dst].ai_grantee = newOwnerId;
        else if (new_aip[dst].ai_grantee == newOwnerId)
            newpresent = true;
    }

    // If new owner was already present, merge duplicate entries
    if (newpresent)
    {
        dst = 0;
        for (targ = 0; targ < num; targ++)
        {
            // Skip entries marked as deleted
            if (ACLITEM_GET_RIGHTS(new_aip[targ]) == ACL_NO_RIGHTS)
                continue;

            // Find and merge any duplicates
            for (src = targ + 1; src < num; src++)
            {
                if (ACLITEM_GET_RIGHTS(new_aip[src]) == ACL_NO_RIGHTS)
                    continue;
                if (aclitem_match(&new_aip[targ], &new_aip[src]))
                {
                    // Merge privileges and mark duplicate for deletion
                    ACLITEM_SET_RIGHTS(new_aip[targ],
                                      ACLITEM_GET_RIGHTS(new_aip[targ]) |
                                      ACLITEM_GET_RIGHTS(new_aip[src]));
                    ACLITEM_SET_RIGHTS(new_aip[src], ACL_NO_RIGHTS);
                }
            }

            // Copy non-duplicate entry to output position
            new_aip[dst] = new_aip[targ];
            dst++;
        }

        // Adjust array size to exclude deleted entries
        ARR_DIMS(new_acl)[0] = dst;
        SET_VARSIZE(new_acl, ACL_N_SIZE(dst));
    }

    return new_acl;
}
```