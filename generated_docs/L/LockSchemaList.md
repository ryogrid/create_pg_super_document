# LockSchemaList

## Location
[src/backend/commands/publicationcmds.c:1719-1746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1719-L1746)

## Overview
Locks schemas specified in a schema list with AccessShareLock to prevent concurrent schema deletion during publication operations.

## Definition

```c
static void
LockSchemaList(List *schemalist)
```
## Detailed Description
LockSchemaList is a static function that ensures schema stability during publication operations by acquiring appropriate locks. The function performs the following operations:

1. **Schema Locking**: Iterates through each schema OID in the provided list and acquires an AccessShareLock using LockDatabaseObject to prevent concurrent schema deletion
2. **Existence Validation**: After acquiring each lock, verifies that the schema still exists by checking the system catalog (NAMESPACEOID), as concurrent DDL operations might have removed the schema before the lock was acquired
3. **Error Handling**: Reports an appropriate error if a schema no longer exists after lock acquisition

The AccessShareLock level allows concurrent read operations while preventing destructive modifications like schema deletion.

## Parameters / Member Variables
- `*schemalist`: List of schema OIDs that need to be locked to ensure they remain stable during publication operations
## Dependencies
- Functions called/Symbols referenced:
  - [LockDatabaseObject](LockDatabaseObject.md) (acquires lock on database object)
  - SearchSysCacheExists1 (checks schema existence in system catalog)
  - CHECK_FOR_INTERRUPTS (allows query cancellation)
- Called from (representative examples):
  - [CreatePublication](../C/CreatePublication.md) (src/backend/commands/publicationcmds.c:849)
  - [AlterPublicationSchemas](../A/AlterPublicationSchemas.md) (src/backend/commands/publicationcmds.c:1266)
  - [AlterPublicationSchemas](../A/AlterPublicationSchemas.md) (src/backend/commands/publicationcmds.c:1315)

## Notes and Other Information
- Uses AccessShareLock which permits concurrent reads but prevents schema deletion
- Essential for maintaining referential integrity during publication schema operations
- Includes CHECK_FOR_INTERRUPTS() to allow cancellation during potentially long lock acquisition sequences
- Validates schema existence after lock acquisition to handle race conditions with concurrent DDL
- Lock acquisition follows the pattern of locking first, then validating existence to handle concurrent schema drops

## Simplified Source

```c
static void LockSchemaList(List *schemalist)
{
    ListCell *lc;

    foreach(lc, schemalist) {
        Oid schemaid = lfirst_oid(lc);

        // Allow query cancellation during potentially long lock operations
        CHECK_FOR_INTERRUPTS();

        LockDatabaseObject(NamespaceRelationId, schemaid, 0, AccessShareLock);

        // Verify schema still exists after acquiring lock
        if (!SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(schemaid))) {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_SCHEMA),
                     errmsg("schema with OID %u does not exist", schemaid)));
        }
    }
}
```