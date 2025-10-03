# merge_acl_with_grant

## Location
[src/backend/catalog/aclchk.c:182-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L182-L240)

## Overview
Merges ACL (Access Control List) entries by either adding or removing specified privileges for given grantees, modifying an existing ACL structure.

## Definition

```c
static Acl *
merge_acl_with_grant(Acl *old_acl, bool is_grant,
					 bool grant_option, DropBehavior behavior,
					 List *grantees, AclMode privileges,
					 Oid grantorId, Oid ownerId)
```
## Detailed Description
This static function performs the core ACL modification logic for PostgreSQL's GRANT and REVOKE operations. It takes an existing ACL and either adds privileges (when is_grant is true) or removes privileges (when is_grant is false) for a list of grantees. The function iterates through each grantee in the list, creates an AclItem structure for each one, and calls aclupdate() to perform the actual ACL modification. The function handles the asymmetric semantics between GRANT and REVOKE operations as specified by SQL standards - GRANT WITH GRANT OPTION grants both basic privileges and grant options, while REVOKE removes both unless specifically revoking only the grant option. The original old_acl is freed to prevent memory leaks.

## Parameters / Member Variables
- `*old_acl`: The existing ACL structure to be modified (will be freed by this function)
- `is_grant`: Boolean flag indicating whether this is a GRANT (true) or REVOKE (false) operation
- `grant_option`: Boolean flag indicating whether grant options are being manipulated
- `behavior`: DropBehavior enum specifying how to handle dependencies during privilege removal
- `*grantees`: List of OIDs representing the users/roles to grant privileges to or revoke from
- `privileges`: AclMode bitmask representing the specific privileges being granted or revoked
- `grantorId`: OID of the user/role performing the grant or revoke operation
- `ownerId`: OID of the object owner (used in aclupdate for privilege validation)
## Dependencies
- Functions called/Symbols referenced:
  - [aclupdate](../a/aclupdate.md)
  - ACLITEM_SET_PRIVS_GOPTIONS
  - [pfree](../p/pfree.md)
  - lfirst_oid
  - ereport
- Types used:
  - [Acl](../A/Acl.md)
  - AclItem
  - AclMode
  - DropBehavior
- Constants referenced:
  - ACL_MODECHG_ADD
  - ACL_MODECHG_DEL
  - ACL_ID_PUBLIC
  - ACL_NO_RIGHTS
- Called from:
  - [SetDefaultACL](../S/SetDefaultACL.md)
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)
  - [ExecGrant_common](../E/ExecGrant_common.md)
  - [ExecGrant_Largeobject](../E/ExecGrant_Largeobject.md)
  - [ExecGrant_Parameter](../E/ExecGrant_Parameter.md)
  - [RemoveRoleFromInitPriv](../R/RemoveRoleFromInitPriv.md)

## Notes and Other Information
- The function enforces a critical security restriction: grant options can only be granted to individual roles, not to PUBLIC, to prevent uncleanable privilege escalation scenarios
- Memory management is carefully handled - the original old_acl is freed and intermediate ACLs are freed during iteration to prevent leaks when processing multiple grantees
- The function implements the SQL standard's asymmetric GRANT/REVOKE semantics where GRANT WITH GRANT OPTION affects both privileges and grant options, while REVOKE has different behavior for regular revoke vs REVOKE GRANT OPTION
- This is a core utility function used by all PostgreSQL object types that support ACL-based permissions

## Simplified Source

```c
static Acl *
merge_acl_with_grant(Acl *old_acl, bool is_grant,
                     bool grant_option, DropBehavior behavior,
                     List *grantees, AclMode privileges,
                     Oid grantorId, Oid ownerId)
{
    unsigned modechg = is_grant ? ACL_MODECHG_ADD : ACL_MODECHG_DEL;
    Acl *new_acl = old_acl;

    // Process each grantee in the list
    foreach(ListCell *j, grantees) {
        AclItem aclitem;

        aclitem.ai_grantee = lfirst_oid(j);
        aclitem.ai_grantor = grantorId;

        // Security check: grant options can only go to individual roles, not PUBLIC
        if (is_grant && grant_option && aclitem.ai_grantee == ACL_ID_PUBLIC)
            ereport(ERROR, (errcode(ERRCODE_INVALID_GRANT_OPERATION),
                           errmsg("grant options can only be granted to roles")));

        // Handle asymmetric GRANT/REVOKE semantics per SQL standard:
        // - GRANT WITH GRANT OPTION: grants both privilege and grant option
        // - REVOKE: removes both privilege and grant option by default
        // - REVOKE GRANT OPTION: removes only the grant option
        ACLITEM_SET_PRIVS_GOPTIONS(aclitem,
                                   (is_grant || !grant_option) ? privileges : ACL_NO_RIGHTS,
                                   (!is_grant || grant_option) ? privileges : ACL_NO_RIGHTS);

        // Update the ACL with this item
        Acl *newer_acl = aclupdate(new_acl, &aclitem, modechg, ownerId, behavior);

        // Prevent memory leaks when processing multiple grantees
        pfree(new_acl);
        new_acl = newer_acl;
    }

    return new_acl;
}
```