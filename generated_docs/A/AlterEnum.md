# AlterEnum

## Location
[src/backend/commands/typecmds.c:1271-1318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L1271-L1318)

## Overview
AlterEnum modifies existing enumeration types by either adding new enum values or renaming existing enum labels, providing the core functionality for the ALTER TYPE ... ADD VALUE and RENAME VALUE commands.

## Definition

```c
enum_type_oid;
```
## Detailed Description
AlterEnum implements PostgreSQL's ALTER TYPE enum modification commands, supporting two primary operations on enumeration types:

1. **Adding New Enum Values**: Adds new enum labels to an existing enum type with optional positioning relative to existing values (BEFORE/AFTER clauses)
2. **Renaming Existing Values**: Changes the textual label of an existing enum value while preserving its internal OID and sort position

The function performs essential validation including type lookup, enum type verification, and ownership checks before delegating to specialized functions for the actual modification work. It supports positioning new values relative to existing ones and includes an option to skip addition if the value already exists.

The function operates on the live enum type and immediately makes changes visible to the system. For adding values, the operation handles sort order management to maintain enum comparison semantics. The changes are reflected in both the pg_type catalog and the pg_enum catalog where enum values are stored.

## Parameters / Member Variables
- : AlterEnumStmt structure containing the modification details
  - : Qualified name list identifying the target enum type
  - : Existing enum label to rename (NULL for ADD VALUE operations)
  - : New enum label (for both ADD VALUE and RENAME VALUE)
  - : Reference enum value for positioning (ADD VALUE only)
  - : Whether to place new value after the neighbor (ADD VALUE only)
  - : Whether to skip operation if value already exists (ADD VALUE only)

## Dependencies
- Functions called/Symbols referenced:
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md): Converts name list to TypeName structure
  - [typenameTypeId](../t/typenameTypeId.md): Resolves type name to OID
  - [SearchSysCache1](../S/SearchSysCache1.md): Retrieves type tuple from system cache
  - [checkEnumOwner](../c/checkEnumOwner.md): Validates user permissions to modify the enum
  - [RenameEnumLabel](../R/RenameEnumLabel.md): Handles enum value renaming operations
  - [AddEnumLabel](AddEnumLabel.md): Handles enum value addition with positioning
  - InvokeObjectPostAlterHook: Triggers post-modification hooks
  - ObjectAddressSet: Sets up return address structure

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main DDL command processing

## Notes and Other Information
- The function acts as a dispatcher between ADD VALUE and RENAME VALUE operations based on the presence of oldVal
- Enum modifications are immediately visible system-wide (no transaction isolation for enum changes)
- Adding values requires careful sort order management to maintain comparison semantics
- Renaming preserves the internal OID and sort position of the enum value
- Permission checking ensures only enum owners can modify enum types
- The IF NOT EXISTS option for ADD VALUE allows idempotent operations
- Position specification (BEFORE/AFTER) enables precise control over enum value ordering
- Post-alter hooks enable extension and trigger integration with enum modifications
- All changes are immediately committed to the catalogs and visible to concurrent transactions

## Simplified Source

```c
ObjectAddress AlterEnum(AlterEnumStmt *stmt) {
    Oid enum_type_oid;
    TypeName *typename;
    HeapTuple tup;
    ObjectAddress address;

    // Convert type name list to TypeName and resolve to OID
    typename = makeTypeNameFromNameList(stmt->typeName);
    enum_type_oid = typenameTypeId(NULL, typename);

    // Get the type tuple and validate it exists
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(enum_type_oid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", enum_type_oid);

    // Check it's an enum and user has permission
    checkEnumOwner(tup);
    ReleaseSysCache(tup);

    // Dispatch based on operation type
    if (stmt->oldVal) {
        // Rename existing enum label
        RenameEnumLabel(enum_type_oid, stmt->oldVal, stmt->newVal);
    } else {
        // Add new enum label with optional positioning
        AddEnumLabel(enum_type_oid, stmt->newVal,
                     stmt->newValNeighbor, stmt->newValIsAfter,
                     stmt->skipIfNewValExists);
    }

    // Trigger post-alter hooks and return object address
    InvokeObjectPostAlterHook(TypeRelationId, enum_type_oid, 0);
    ObjectAddressSet(address, TypeRelationId, enum_type_oid);

    return address;
}
```