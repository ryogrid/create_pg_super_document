# changeDependencyOnOwner

## Location
[src/backend/catalog/pg_shdepend.c:316-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L316-L369)

## Overview
Updates shared dependency records when an object's owner changes, handling both the new ownership dependency and cleanup of conflicting ACL dependencies.

## Definition


## Detailed Description
This function manages the complex process of changing object ownership in PostgreSQL's shared dependency system. It performs two key operations: 1) Updates the SHARED_DEPENDENCY_OWNER entry to point to the new owner, and 2) Removes any SHARED_DEPENDENCY_ACL entry for the new owner to prevent conflicts (since owners don't need explicit ACL entries for their own objects). This cleanup prevents issues that could arise from ownership transfer scenarios where the new owner previously had explicit privileges on the object.

## Parameters / Member Variables
- : OID of the catalog containing the object whose owner is changing
- : OID of the object whose owner is changing
- : OID of the new owner (from pg_authid catalog)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [shdepChangeDep](../s/shdepChangeDep.md)
  - [shdepDropDependency](../s/shdepDropDependency.md)
  - table_close
  - SHARED_DEPENDENCY_OWNER (dependency type)
  - SHARED_DEPENDENCY_ACL (dependency type)
- Called from (representative examples):
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md)
  - [AlterDatabaseOwner](../A/AlterDatabaseOwner.md)
  - [ATExecChangeOwner](../A/ATExecChangeOwner.md) (table ownership changes)
  - [AlterSchemaOwner_internal](../A/AlterSchemaOwner_internal.md)
  - [AlterSubscriptionOwner_internal](../A/AlterSubscriptionOwner_internal.md)
  - [AlterPublicationOwner_internal](../A/AlterPublicationOwner_internal.md)
  - [AlterTypeOwner_oid](../A/AlterTypeOwner_oid.md)

## Notes and Other Information
- No objsubid parameter needed since only whole objects have owners (not sub-objects)
- Prevents duplicate entries by removing ACL dependencies for the new owner
- Handles complex ownership transfer scenarios correctly
- Does not modify SHARED_DEPENDENCY_INITACL entries as they exist independently of ownership
- Opens pg_shdepend with RowExclusiveLock for safe concurrent access
- Part of PostgreSQL's ALTER OWNER command implementation
- Located in src/backend/catalog/pg_shdepend.c:316-369