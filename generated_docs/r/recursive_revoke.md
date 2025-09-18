# recursive_revoke

## Location
[src/backend/utils/adt/acl.c:1302-1387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1302-L1387)

## Overview
Ensures that no privileges become "abandoned" by recursively revoking privileges that were granted through a broken chain of grant options.

## Definition
```c
static Acl *recursive_revoke(Acl *acl, Oid grantee, AclMode revoke_privs, Oid ownerId, DropBehavior behavior)
```

## Detailed Description
This function implements the cascading revocation logic that maintains the integrity of PostgreSQL's privilege system. When a user loses grant options, any privileges they previously granted to others become "abandoned" because the grant chain is broken. This function identifies such abandoned privileges and either revokes them automatically (CASCADE behavior) or reports an error (RESTRICT behavior). The function works by first checking if the grantee still has the relevant grant options through other grantors, then iteratively finding and removing all privileges that were granted by the affected grantee using the now-revoked grant options.

## Parameters / Member Variables
- `acl` (Acl *): The input ACL list to be processed (may be freed and replaced)
- `grantee` (Oid): The user from whom grant options have been revoked
- `revoke_privs` (AclMode): The specific grant options being revoked  
- `ownerId` (Oid): Object identifier of the object owner
- `behavior` (DropBehavior): Either RESTRICT (error on dependencies) or CASCADE (automatically revoke dependencies)

## Dependencies
- Functions called/Symbols referenced:
  - [check_acl](../c/check_acl.md): Validates ACL structure
  - [aclmask](../a/aclmask.md): Computes remaining grant options the grantee might have via other grantors
  - [aclupdate](../a/aclupdate.md): Recursively removes dependent privileges
  - ACL manipulation macros (ACL_NUM, ACL_DAT, ACLITEM_GET_PRIVS, etc.)
  - Error reporting functions (ereport, errcode, errmsg, errhint)
  - Memory management (pfree)
- Called from (representative examples):
  - [aclupdate](../a/aclupdate.md): During privilege revocation operations that remove grant options

## Notes and Other Information
- Static function, only accessible within the ACL module
- Object owners can never truly lose grant options, so processing is short-circuited for owners
- Uses a restart mechanism (goto restart) to handle the iterative removal process
- With RESTRICT behavior, throws ERROR with ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST if dependent privileges are found
- With CASCADE behavior, automatically removes all dependent privileges
- The input ACL object is freed and replaced if modifications are made
- Essential for maintaining referential integrity in PostgreSQL's privilege grant chains
- Prevents orphaned privileges that could persist after their grant chain is broken
- Located in src/backend/utils/adt/acl.c:1302-1387