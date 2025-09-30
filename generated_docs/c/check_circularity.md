# check_circularity

## Location
[src/backend/utils/adt/acl.c:1222-1301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1222-L1301)

## Overview
Prevents circular chains of grant options by verifying that granting grant options would not create a situation where privileges cannot be effectively revoked.

## Definition
```c
static void check_circularity(const Acl *old_acl, const AclItem *mod_aip, Oid ownerId)
```

## Detailed Description
This function implements a critical security check in PostgreSQL's privilege system to prevent circular grant option chains. When a user attempts to grant privileges with grant options, this function ensures that the resulting configuration would not create a circular dependency that would make it impossible for the object owner to effectively revoke privileges. The check works by simulating the removal of all grant options belonging to the target grantee (and their dependencies) from a working copy of the ACL, then verifying that the would-be grantor still independently possesses the necessary grant options to perform the grant operation.

## Parameters / Member Variables
- `old_acl` (const Acl *): The current ACL array before the proposed grant operation
- `mod_aip` (const AclItem *): The ACL item describing the proposed grant, including grantee, grantor, and privileges with grant options
- `ownerId` (Oid): Object identifier of the object owner (who always has implicit grant options)

## Dependencies
- Functions called/Symbols referenced:
  - [check_acl](check_acl.md): Validates ACL structure
  - [allocacl](../a/allocacl.md): Allocates memory for working copy of ACL
  - [aclupdate](../a/aclupdate.md): Updates ACL by removing grant options (called recursively via goto)
  - [aclmask](../a/aclmask.md): Computes effective privileges for the grantor
  - ACL manipulation macros (ACL_NUM, ACL_DAT, ACLITEM_GET_GOPTIONS, etc.)
  - Memory management functions (memcpy, pfree)
- Called from (representative examples):
  - [aclupdate](../a/aclupdate.md): During grant option operations to prevent circular dependencies

## Notes and Other Information
- Static function, only accessible within the ACL module
- Currently only supports role-based grantees, not PUBLIC (asserted at runtime)
- Object owners always have grant options, so no check is needed for owner grants
- Uses a restart mechanism (goto cc_restart) to handle cascading removals
- Throws ERROR with ERRCODE_INVALID_GRANT_OPERATION if circularity is detected
- The algorithm simulates privilege revocation to test for independence of grant options
- Essential for maintaining the integrity of PostgreSQL's privilege revocation system
- Located in src/backend/utils/adt/acl.c:1222-1301

## Simplified Source
```c
static void check_circularity(const Acl *old_acl, const AclItem *mod_aip, Oid ownerId) {
    // Quick exit for owner grants - owners always have grant options
    if (mod_aip->ai_grantor == ownerId)
        return;

    // Create working copy of ACL
    Acl *acl = allocacl(ACL_NUM(old_acl));
    memcpy(acl, old_acl, ACL_SIZE(old_acl));

    // Remove all grant options from target grantee and dependencies
    while (true) {
        bool found_removable = false;
        AclItem *aip = ACL_DAT(acl);

        for (int i = 0; i < ACL_NUM(acl); i++) {
            if (aip[i].ai_grantee == mod_aip->ai_grantee &&
                ACLITEM_GET_GOPTIONS(aip[i]) != ACL_NO_RIGHTS) {

                // Remove this entry and restart
                acl = aclupdate(acl, &aip[i], ACL_MODECHG_DEL, ownerId, DROP_CASCADE);
                found_removable = true;
                break;
            }
        }

        if (!found_removable)
            break;
    }

    // Check if grantor still has independent grant options
    AclMode grantor_privs = aclmask(acl, mod_aip->ai_grantor, ownerId,
                                   ACL_GRANT_OPTION_FOR(ACLITEM_GET_GOPTIONS(*mod_aip)),
                                   ACLMASK_ALL);
    grantor_privs = ACL_OPTION_TO_PRIVS(grantor_privs);

    // Error if trying to grant back to own grantor
    if ((ACLITEM_GET_GOPTIONS(*mod_aip) & ~grantor_privs) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                       errmsg("grant options cannot be granted back to your own grantor")));
    }

    pfree(acl);
}
```