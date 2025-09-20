# GetNewRelFileNumber

## Location
[src/backend/catalog/catalog.c:530-615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L530-L615)

## Overview
GetNewRelFileNumber generates a new unique relfile number for a relation within the specified database and tablespace, with filesystem-level collision detection.

## Definition

```c
enumber assignments during a binary-upgrade run should be
	 * determined by commands in the dump script.
	 */
	Assert(!IsBinaryUpgrade);
```
## Detailed Description
This function generates a unique relfile number for database relations by combining OID generation with filesystem collision detection. It handles different relation persistence types (temporary, unlogged, permanent) with appropriate backend assignments. For temporary relations, it uses a specific backend number, while permanent and unlogged relations use an invalid backend number. The function optionally ensures the generated number is also unique as an OID in pg_class when that catalog is provided. It constructs the complete file path and uses filesystem access checks to verify uniqueness, preventing file conflicts at the storage layer.

## Parameters / Member Variables
- : OID of the tablespace where the relation will be stored (0 for default)
- : Open pg_class catalog relation if the relfile number will also serve as OID (NULL otherwise)
- : Character indicating relation persistence ('t' for temporary, 'u' for unlogged, 'p' for permanent)

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumberForTempRelations
  - [GetNewOidWithIndex](GetNewOidWithIndex.md) (when pg_class is provided)
  - [GetNewObjectId](GetNewObjectId.md) (when pg_class is NULL)
  - relpath
  - access (filesystem access check)
  - RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED, RELPERSISTENCE_PERMANENT
  - INVALID_PROC_NUMBER
  - MyDatabaseTableSpace, MyDatabaseId
  - GLOBALTABLESPACE_OID
  - MAIN_FORKNUM, F_OK
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (src/backend/catalog/heap.c:1249)
  - index_create (src/backend/catalog/index.c:965)
  - [ATExecSetTableSpace](../A/ATExecSetTableSpace.md) (src/backend/commands/tablecmds.c:15290)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md) (src/backend/utils/cache/relcache.c:3783)

## Notes and Other Information
- Not supported in bootstrap mode since all bootstrap relations have preassigned OIDs
- Includes assertion to prevent usage during pg_upgrade operations
- Uses filesystem-level collision detection via access() system call
- Handles different backend number assignments based on relation persistence
- For global tablespace relations, sets database OID to InvalidOid
- Implements a pragmatic approach to filesystem errors during collision detection
- The relpath construction logic matches RelationInitPhysicalAddr for consistency
- Continues generation loop until both OID uniqueness (if required) and filesystem uniqueness are achieved