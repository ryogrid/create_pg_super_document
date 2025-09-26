# InitTempTableNamespace

## Location
[src/backend/catalog/namespace.c:4390-4511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4390-L4511)

## Overview
Initializes the temporary table namespace on first use in a backend, creating both temp and toast namespaces with proper permissions and cleanup.

## Definition
```c
static void
InitTempTableNamespace(void)
```

## Detailed Description
InitTempTableNamespace performs the complex initialization of PostgreSQL's temporary table namespace system for a backend process. The function handles multiple critical aspects:

**Permission and State Validation:**
- Verifies the current user has ACL_CREATE_TEMP privileges on the database
- Prevents temp table creation during Hot Standby recovery mode
- Blocks temp table creation in parallel worker processes

**Namespace Creation and Management:**
- Creates a uniquely named temp namespace (`pg_temp_N`) based on the backend's MyProcNumber
- If the namespace already exists (from a crashed previous session), cleans out existing temporary relations
- Creates a corresponding toast namespace (`pg_toast_temp_N`) for TOAST tables
- Sets namespace ownership to the bootstrap superuser with restrictive permissions

**State Management:**
- Updates global variables (myTempNamespace, myTempToastNamespace) to track the namespaces
- Marks the process as owning the namespace via MyProc->tempNamespaceId
- Records the subtransaction ID for proper cleanup handling
- Invalidates search path caches since the namespace list has changed

## Parameters / Member Variables
This function takes no parameters and operates on global state variables and system catalogs.

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (constant)
  - [object_aclcheck](../o/object_aclcheck.md)
  - ACL_CREATE_TEMP
  - [get_database_name](../g/get_database_name.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - IsParallelWorker
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [NamespaceCreate](../N/NamespaceCreate.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [RemoveTempRelations](../R/RemoveTempRelations.md)
  - InvalidSubTransactionId
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
- Called from (representative examples):
  - [AccessTempTableNamespace](../A/AccessTempTableNamespace.md)

## Notes and Other Information
- This is a static function only accessible within namespace.c
- Uses BOOTSTRAP_SUPERUSERID as the owner to ensure security isolation between backends
- The namespace naming scheme (`pg_temp_N`) ensures uniqueness across concurrent backends
- [Command](../C/Command.md) counter increments ensure namespace visibility within the same transaction
- Critical for transaction cleanup via AtEOXact_Namespace through the recorded subtransaction ID
- Implements lazy initialization - namespaces are only created when actually needed
- Handles crash recovery by cleaning up leftover temporary relations from previous sessions