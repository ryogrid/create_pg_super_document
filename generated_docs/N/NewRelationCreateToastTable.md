# NewRelationCreateToastTable

## Location
[src/backend/catalog/toasting.c:71-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/toasting.c#L71-L77)

## Overview
NewRelationCreateToastTable is a function that creates a TOAST table for a newly created relation with default locking behavior and simplified parameter interface.

## Definition

```c
void
NewRelationCreateToastTable(Oid relOid, Datum reloptions)
```
## Detailed Description
This function provides a simplified interface for creating TOAST tables for new relations. It automatically uses AccessExclusiveLock for the locking mode and assumes no previous TOAST table exists (InvalidOid for the old TOAST OID). This function is typically used during the creation of new tables where TOAST storage might be needed, such as during CREATE TABLE AS SELECT operations or other utility commands that create new relations.

The function is designed for scenarios where a new relation has been created and needs TOAST table setup with standard parameters. It delegates to CheckAndCreateToastTable with predefined values for common use cases, simplifying the interface for callers who don't need to specify custom locking or reference old TOAST tables.

## Parameters / Member Variables
- : The OID of the newly created relation for which to create a TOAST table
- : Datum containing reloptions for the TOAST table, or (Datum) 0 for default options

## Dependencies
- Functions called/Symbols referenced:
  - [CheckAndCreateToastTable](../C/CheckAndCreateToastTable.md)
  - AccessExclusiveLock
- Called from (representative examples):
  - [create_ctas_internal](../c/create_ctas_internal.md) (in src/backend/commands/createas.c:130)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (in src/backend/tcop/utility.c:1192)

## Notes and Other Information
- This is the simplest interface among the TOAST table creation functions, requiring only relation OID and reloptions
- Automatically uses AccessExclusiveLock, which is appropriate for new relation creation scenarios
- Sets  to false and  to InvalidOid when calling CheckAndCreateToastTable
- Commonly used in CREATE TABLE AS SELECT operations and other utility commands that create new relations
- The simplified parameter set makes it ideal for straightforward new table creation scenarios

## Simplified Source

```c
void NewRelationCreateToastTable(Oid relOid, Datum reloptions)
{
    // Simple wrapper that creates a TOAST table for a new relation
    // Uses default settings: AccessExclusiveLock, no old TOAST table
    CheckAndCreateToastTable(relOid, reloptions, AccessExclusiveLock, false, InvalidOid);
}
```