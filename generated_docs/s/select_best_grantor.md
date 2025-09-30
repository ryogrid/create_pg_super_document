# select_best_grantor

## Location
[src/backend/utils/adt/acl.c:5361-5436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5361-L5436)

## Overview
Selects the effective grantor ID for a GRANT or REVOKE operation by finding the most suitable role with appropriate grant options when the requesting role doesn't own the object.

## Definition

```c
void
select_best_grantor(Oid roleId, AclMode privileges,
					const Acl *acl, Oid ownerId,
					Oid *grantorId, AclMode *grantOptions)
```
## Detailed Description
This function implements a sophisticated algorithm to determine which role should be used as the grantor in GRANT/REVOKE operations. The grantor must always be either the object owner or a role that has been explicitly granted grant options. This ensures that all granted privileges appear to flow from the object owner, preventing multiple "original sources" of a privilege.

When the requesting role is a member of multiple roles with different subsets of the desired grant options, the function picks the role with the largest number of desired options. Ties are broken in favor of closer ancestors in the role hierarchy.

The function first checks if the requesting role is the object owner or a superuser (which are treated as having all grant options). If not, it searches through all roles that the requesting role is a member of to find the best candidate grantor.

## Parameters / Member Variables
- : The role attempting to perform the GRANT/REVOKE operation
- : The privileges to be granted or revoked
- : The Access Control List of the object in question
- : The role that owns the object in question
- : Output parameter that receives the OID of the role to use as grantor
- : Output parameter that receives the grant options actually held by the selected grantor

## Dependencies
- Functions called/Symbols referenced:
  - ACL_GRANT_OPTION_FOR (macro to convert privileges to grant options)
  - [superuser_arg](superuser_arg.md) (checks if role is superuser)
  - [roles_is_member_of](../r/roles_is_member_of.md) (gets list of roles the user is a member of)
  - [aclmask_direct](../a/aclmask_direct.md) (checks privileges directly held by a role)
  - [count_one_bits](../c/count_one_bits.md) (utility function to count set bits)
  - ACL_NO_RIGHTS (constant for no privileges)
  - ACLMASK_ALL (mask for all privileges)
  - ROLERECURSE_PRIVS (flag for privilege-based role recursion)
- Called from:
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md) (for column-level grants)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md) (for table-level grants)
  - [ExecGrant_common](../E/ExecGrant_common.md) (common grant processing)
  - [ExecGrant_Largeobject](../E/ExecGrant_Largeobject.md) (for large object grants)
  - [ExecGrant_Parameter](../E/ExecGrant_Parameter.md) (for parameter grants)

## Notes and Other Information
- If no suitable grant options exist, the function defaults to using the original roleId as grantor with no grant options
- The algorithm ensures privilege consistency by always making grants appear to flow from object owners
- Superusers are treated as implicit members of every role and act as object owners
- The function is critical for maintaining PostgreSQL's role-based access control security model

## Simplified Source

```c
void
select_best_grantor(Oid roleId, AclMode privileges,
                    const Acl *acl, Oid ownerId,
                    Oid *grantorId, AclMode *grantOptions)
{
    AclMode needed_goptions = ACL_GRANT_OPTION_FOR(privileges);
    List *roles_list;
    int nrights;

    // Object owner or superuser can grant any privilege
    if (roleId == ownerId || superuser_arg(roleId)) {
        *grantorId = ownerId;
        *grantOptions = needed_goptions;
        return;
    }

    // Search through all roles the user is a member of
    roles_list = roles_is_member_of(roleId, ROLERECURSE_PRIVS, InvalidOid, NULL);

    // Initialize with default (no grant options available)
    *grantorId = roleId;
    *grantOptions = ACL_NO_RIGHTS;
    nrights = 0;

    // Find the best role with grant options
    foreach(ListCell *l, roles_list) {
        Oid otherrole = lfirst_oid(l);
        AclMode otherprivs;

        // Check what grant options this role has
        otherprivs = aclmask_direct(acl, otherrole, ownerId,
                                   needed_goptions, ACLMASK_ALL);

        if (otherprivs == needed_goptions) {
            // Perfect match - this role has all needed grant options
            *grantorId = otherrole;
            *grantOptions = otherprivs;
            return;
        }

        // Remember the best partial match (most grant options)
        if (otherprivs != ACL_NO_RIGHTS) {
            int nnewrights = count_one_bits(otherprivs);
            if (nnewrights > nrights) {
                *grantorId = otherrole;
                *grantOptions = otherprivs;
                nrights = nnewrights;
            }
        }
    }

    // Return the best available grantor (may have no grant options)
}
```