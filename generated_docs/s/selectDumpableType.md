# selectDumpableType

## Location
src/bin/pg_dump/pg_dump.c: 1909 - 1953

## Overview
Policy-setting function that determines whether a data type should be dumped, with special handling for table rowtypes, auto-generated array types, and multirange types to maintain proper dump ordering.

## Definition
```c
static void selectDumpableType(TypeInfo *tyinfo, Archive *fout)
```

## Detailed Description
This function implements dump decision logic for PostgreSQL data types with sophisticated handling for special type categories:

1. **Table rowtypes and related types**: Types with a valid typrelid that aren't standalone composite types are marked as DO_DUMMY_TYPE. Their dump flag is set to match their underlying table's dump setting, ensuring proper dependency handling for casts while preventing the types from appearing in the datatype section of the dump.

2. **Auto-generated types**: Array types (isArray) and multirange types (isMultirange) are also marked as DO_DUMMY_TYPE. This prevents them from being dumped as independent types since they're automatically created with their base types.

3. **Extension membership**: Like other database objects, extension membership overrides all other dump decisions via checkExtensionMembership().

4. **Namespace inheritance**: Regular types inherit their dump setting from their containing namespace's dump_contains flag.

The DO_DUMMY_TYPE classification is crucial for maintaining the correct dump order - it ensures that table-related types don't cause tables to be moved into the datatype section of the dump, while still allowing casts involving these types to be dumped correctly.

## Parameters / Member Variables
- `tyinfo`: Pointer to TypeInfo structure containing type information and dump flags to be set
- `fout`: Pointer to Archive structure passed to checkExtensionMembership for extension processing

## Dependencies
- Functions called/Symbols referenced:
  - findTableByOid (locate table associated with rowtype)
  - checkExtensionMembership (check if type belongs to extension)
  - DO_DUMMY_TYPE (special object type for dump ordering)
  - RELKIND_COMPOSITE_TYPE (relation kind constant)
  - DUMP_COMPONENT_NONE (dump component constant)
- Called from (representative examples):
  - getTypes

## Notes and Other Information
- This is a static function within pg_dump.c used during the dump planning phase
- The DO_DUMMY_TYPE mechanism is essential for correct dump ordering and dependency resolution
- Auto-generated types (arrays, multiranges) are handled differently from user-defined types
- The function ensures that casts involving special types can still be dumped correctly
- Extension membership takes precedence over all other dump decisions
- Table rowtypes follow their underlying table's dump status for consistency