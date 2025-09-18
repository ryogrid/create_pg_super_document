# sequenceIsOwned

## Location
[src/backend/catalog/pg_depend.c:829-877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L829-L877)

## Overview
Detects whether a sequence is marked as "owned" by a table column through AUTO or INTERNAL dependency relationships.

## Definition


## Detailed Description
The `sequenceIsOwned` function determines if a sequence has an ownership relationship with a table column by searching for specific dependency types in the `pg_depend` system catalog. Sequence ownership is established through either AUTO or INTERNAL dependencies from the sequence to a column, which indicates that the sequence's lifecycle is tied to that column.

The function performs a targeted scan of the `pg_depend` table, looking for entries where:
- The `classid` is RelationRelationId and `objid` matches the sequence OID (indicating the dependency originates from the sequence)
- The `refclassid` is RelationRelationId (indicating the dependency target is a table/relation)  
- The `deptype` matches the specified dependency type (typically DEPENDENCY_AUTO or DEPENDENCY_INTERNAL)

When such a dependency is found, the function extracts the owning table's OID (`refobjid`) and column number (`refobjsubid`) into the provided output parameters and returns true. If no ownership relationship is found, the function returns false.

## Parameters / Member Variables
- `seqId`: The OID of the sequence to check for ownership
- `deptype`: The type of dependency to look for (typically DEPENDENCY_AUTO or DEPENDENCY_INTERNAL)
- `tableId`: Output parameter - receives the OID of the owning table if found
- `colId`: Output parameter - receives the column number (attribute number) of the owning column if found

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](systable_beginscan.md)
  - [systable_getnext](systable_getnext.md)
  - Form_pg_depend
- Called from (representative examples):
  - [process_owned_by](../p/process_owned_by.md)
  - [ATExecChangeOwner](../A/ATExecChangeOwner.md)
  - [AlterTableNamespace](../A/AlterTableNamespace.md)
  - PERFORM_DELETION_CONCURRENT_LOCK

## Notes and Other Information
- Returns false if the sequence is not owned by any column
- If multiple ownership dependencies exist (which should not happen under normal circumstances), the function returns information for the first one found
- The `refobjsubid` field contains the column number, with positive values indicating specific columns
- Sequence ownership is typically established when creating sequences with SERIAL or IDENTITY columns
- This function is crucial for maintaining referential integrity when performing operations like changing table ownership or moving tables to different schemas
- Uses AccessShareLock when accessing the pg_depend catalog to ensure consistent reads