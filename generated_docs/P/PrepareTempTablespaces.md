# PrepareTempTablespaces

## Location
[src/backend/commands/tablespace.c:1331-1425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L1331-L1425)

## Overview
Parses the temp_tablespaces GUC variable and configures the storage subsystem to use the specified tablespaces for temporary files, performing validation and permission checks.

## Definition

```c
void
PrepareTempTablespaces(void)
```
## Detailed Description
This function ensures that temporary file storage is properly configured according to the temp_tablespaces GUC setting. It is designed to be called multiple times safely - it performs no work if tablespaces have already been set up in the current transaction.

The function performs several steps:
1. **Early exit checks**: Returns immediately if tablespaces are already configured or if not in a transaction
2. **String parsing**: Parses the temp_tablespaces GUC value as a comma-separated list of tablespace names
3. **Validation loop**: For each tablespace name, verifies existence and checks CREATE permissions
4. **Memory allocation**: Stores valid tablespace OIDs in TopTransactionContext for transaction-duration persistence
5. **Configuration**: Calls SetTempTablespaces to inform the storage system of available tablespaces

Special handling includes allowing empty strings and explicit database default tablespace references, both of which are converted to InvalidOid. Invalid or inaccessible tablespaces are silently skipped rather than causing errors.

## Parameters
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [TempTablespacesAreSet](../T/TempTablespacesAreSet.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - [get_tablespace_oid](../g/get_tablespace_oid.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [SetTempTablespaces](../S/SetTempTablespaces.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [GetDefaultTablespace](../G/GetDefaultTablespace.md) (src/backend/commands/tablespace.c:1150)
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md) (src/backend/executor/nodeHash.c:586)
  - [BufFileCreateTemp](../B/BufFileCreateTemp.md) (src/backend/storage/file/buffile.c:207)
  - [inittapestate](../i/inittapestate.md) (src/backend/utils/sort/tuplesort.c:1965)

## Notes and Other Information
- Called once per transaction, subsequent calls in the same transaction are no-ops
- Silently skips invalid tablespace names rather than throwing errors
- Uses TopTransactionContext for memory allocation to ensure persistence across the transaction
- Falls back gracefully when called outside a transaction (storage system uses database default)
- Converts explicit database default tablespace references to InvalidOid for consistency
- Essential for operations that create temporary files like sorting, hashing, and tuple storage

## Simplified Source

```c
void
PrepareTempTablespaces(void)
{
    // Skip if already configured or not in transaction
    if (TempTablespacesAreSet() || !IsTransactionState())
        return;

    // Parse the temp_tablespaces GUC setting
    char *rawname = pstrdup(temp_tablespaces);
    List *namelist;

    if (!SplitIdentifierString(rawname, ',', &namelist)) {
        // Handle syntax error - use no temp tablespaces
        SetTempTablespaces(NULL, 0);
        pfree(rawname);
        list_free(namelist);
        return;
    }

    // Allocate tablespace OID array in transaction context
    Oid *tblSpcs = (Oid *) MemoryContextAlloc(TopTransactionContext,
                                              list_length(namelist) * sizeof(Oid));
    int numSpcs = 0;

    // Validate each tablespace name and collect valid OIDs
    ListCell *l;
    foreach(l, namelist) {
        char *curname = (char *) lfirst(l);

        // Handle empty string (database default)
        if (curname[0] == '\0') {
            tblSpcs[numSpcs++] = InvalidOid;
            continue;
        }

        // Look up tablespace by name
        Oid curoid = get_tablespace_oid(curname, true);
        if (curoid == InvalidOid)
            continue;  // Skip invalid tablespace names

        // Handle explicit database default tablespace
        if (curoid == MyDatabaseTableSpace) {
            tblSpcs[numSpcs++] = InvalidOid;
            continue;
        }

        // Check CREATE permission on tablespace
        AclResult aclresult = object_aclcheck(TableSpaceRelationId, curoid,
                                              GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            continue;  // Skip tablespaces without permission

        tblSpcs[numSpcs++] = curoid;
    }

    // Configure the storage system with valid tablespaces
    SetTempTablespaces(tblSpcs, numSpcs);

    pfree(rawname);
    list_free(namelist);
}
```