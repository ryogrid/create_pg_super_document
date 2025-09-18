# CheckIndexCompatible

## Location
[src/backend/commands/indexcmds.c:177-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L177-L359)

## Overview
Determines whether an existing index definition is compatible with a prospective index definition, such that the existing index storage could become the storage of the new index, avoiding a rebuild.

## Definition
```c
bool CheckIndexCompatible(Oid oldId, const char *accessMethodName, const List *attributeList, const List *exclusionOpNames)
```

## Detailed Description
CheckIndexCompatible is tailored to the needs of ALTER TABLE ALTER TYPE operations, which recreate indexes that depend on a changing column from their pg_get_indexdef or pg_get_constraintdef definitions. The function performs compatibility checks by comparing operator classes, collations, and exclusion operators between the old and new index definitions.

Most column type changes that can skip a table rewrite do not invalidate indexes. The function acknowledges this when all operator classes, collations and exclusion operators match. It omits some sanity checks of DefineIndex and assumes that the old and new indexes have the same number of columns and that if one has an expression column or predicate, both do.

The function performs several key compatibility checks:
- Verifies access method compatibility
- Compares operator classes and collations
- Handles polymorphic operators by checking actual input types
- Validates exclusion constraint operators if present
- Checks opclass options for compatibility

Currently, the function does not implement tests to verify compatibility of expression columns or predicates, so it assumes any such index is incompatible.

## Parameters / Member Variables
- `oldId`: The OID of the existing index to check compatibility against
- `accessMethodName`: Name of the access method to use for the new index
- `attributeList`: A list of IndexElem specifying columns and expressions to index on
- `exclusionOpNames`: List of names of exclusion-constraint operators, or NIL if not an exclusion constraint

## Dependencies
- Functions called/Symbols referenced:
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md)
  - [makeIndexInfo](../m/makeIndexInfo.md)
  - [ComputeIndexAttrs](ComputeIndexAttrs.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - [get_opclass_input_type](../g/get_opclass_input_type.md)
  - IsPolymorphicType
  - [CompareOpclassOptions](CompareOpclassOptions.md)
  - [RelationGetExclusionInfo](../R/RelationGetExclusionInfo.md)
  - [op_input_types](../o/op_input_types.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [TryReuseIndex](../T/TryReuseIndex.md)

## Notes and Other Information
- Returns false immediately if the index has expressions, predicates, or is invalid
- For polymorphic operator class input types, column type changes break compatibility
- Changes in operator class options also break compatibility
- The function assumes compatibility issues are primarily related to operator classes, collations, and exclusion operators
- Used primarily in ALTER TABLE operations to determine if an index rebuild can be avoided
- Located in src/backend/commands/indexcmds.c:177-359