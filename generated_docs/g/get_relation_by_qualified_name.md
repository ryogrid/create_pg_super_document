# get_relation_by_qualified_name

## Location
src/backend/catalog/objectaddress.c: 1333 - 1414

## Overview
Locates a relation by qualified name and validates that the relation matches the expected object type (table, index, sequence, view, etc.).

## Definition


## Detailed Description
The  function is a static helper function that handles object address resolution specifically for relation-type objects in PostgreSQL. It takes a qualified name list and attempts to open the corresponding relation, then validates that the relation's kind matches the expected object type.

The function first attempts to open the relation using , which handles the name resolution and locking. If the relation cannot be found and  is true, it returns an invalid ObjectAddress. Otherwise, it performs type validation by checking the relation's  field against the expected type.

The function supports six different relation types: indexes (including partitioned indexes), sequences, tables (including partitioned tables), views, materialized views, and foreign tables. For each type, it validates that the actual relation kind matches what was requested, throwing appropriate error messages if there's a mismatch.

After successful validation, the function sets the objectId to the relation's OID and returns the opened relation through the  parameter. The caller is responsible for closing the relation when done.

## Parameters / Member Variables
- : The expected type of relation object (OBJECT_INDEX, OBJECT_TABLE, etc.)
- : List of name components forming the qualified relation name
- : Output parameter that receives the opened relation
- : The lock mode to apply when opening the relation
- : If true, return invalid ObjectAddress instead of throwing error when relation not found

## Dependencies
- Functions called/Symbols referenced:
  - [relation_openrv_extended](../r/relation_openrv_extended.md) (for opening relations with missing_ok support)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md) (for converting name list to RangeVar)
  - RelationGetRelationName (for error messages)
  - RelationGetRelid (for getting relation OID)
  - Various RELKIND_* constants for type validation
- Called from (representative examples):
  - [get_object_address](get_object_address.md)

## Notes and Other Information
This function is marked static and serves as a specialized helper for relation objects within the objectaddress.c module. It combines name resolution, locking, and type validation in a single operation. The type validation is strict - it will reject relations that don't exactly match the expected type, which helps prevent operations on wrong object types. The function handles both regular and partitioned variants of tables and indexes, recognizing that these are logically similar object types.