# deleteDependencyRecordsFor

## Location
src/backend/catalog/pg_depend.c: 302 - 351

## Overview
Deletes all dependency records where the specified object is the depender, typically used when redefining existing objects to clean up old dependencies before recreating them.

## Definition


## Detailed Description
This function removes all dependency records from pg_depend where the specified object (identified by classId and objectId) is the dependent object. It is primarily used during object redefinition operations where the outgoing dependencies from an object need to be cleared before establishing new ones. The function provides an option to preserve extension membership dependencies, which is important for maintaining the relationship between objects and their containing extensions during redefinition. The function uses a system catalog scan to locate and delete matching records, returning the count of deleted entries.

## Parameters / Member Variables
- : OID of the system catalog containing the object (e.g., RelationRelationId for tables)
- : OID of the specific object whose dependency records should be deleted
- : Boolean flag to preserve DEPENDENCY_EXTENSION records (true = keep extension memberships)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - [SysScanDesc](../S/SysScanDesc.md)
  - Form_pg_depend
  - DEPENDENCY_EXTENSION
- Called from (representative examples):
  - [makeOperatorDependencies](../m/makeOperatorDependencies.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - [swap_relation_files](../s/swap_relation_files.md)
  - [CreateTransform](../C/CreateTransform.md)
  - [AlterPolicy](../A/AlterPolicy.md)
  - [CreateProceduralLanguage](../C/CreateProceduralLanguage.md)
  - [ATExecSetExpression](../A/ATExecSetExpression.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)

## Notes and Other Information
- Located in src/backend/catalog/pg_depend.c:302-351
- Returns the number of dependency records that were deleted
- Uses DependDependerIndexId index for efficient scanning of pg_depend catalog
- Acquires RowExclusiveLock on pg_depend relation for safe deletion
- When skipExtensionDeps=true, preserves extension membership to avoid complex recreation logic
- Part of object redefinition workflow where dependencies must be refreshed
- Does not affect incoming dependencies (where this object is referenced by others)
- Critical for maintaining consistency during ALTER operations and object replacements