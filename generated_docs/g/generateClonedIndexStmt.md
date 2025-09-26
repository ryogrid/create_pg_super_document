# generateClonedIndexStmt

## Location
[src/backend/parser/parse_utilcmd.c:1514-1864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L1514-L1864)

## Overview
Generates an IndexStmt node by cloning the structure and properties of an existing index, adjusting attribute numbers according to a provided mapping for use in table creation scenarios.

## Definition
IndexStmt *generateClonedIndexStmt(RangeVar *heapRel, Relation source_idx, const AttrMap *attmap, Oid *constraintOid)

## Detailed Description
This function creates a complete IndexStmt that recreates an existing index on a different table. It extracts all properties from the source index including access method, uniqueness, primary key status, constraint information, column definitions, expressions, predicates, and options. The function handles both simple column references and complex expression indexes, adjusting all attribute numbers using the provided attribute map. It also processes constraint-related indexes (primary key, unique, exclusion) and copies their constraint properties. The resulting IndexStmt can be executed to create an equivalent index on the target table.

## Parameters / Member Variables
- `heapRel`: RangeVar specifying the target table for the new index (may be NULL if not needed)
- `source_idx`: Relation representing the existing index to clone
- `attmap`: AttrMap for translating attribute numbers from source to target table
- `constraintOid`: Output parameter to store the OID of any associated constraint (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [get_tablespace_name](get_tablespace_name.md)
  - [get_index_constraint](get_index_constraint.md)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [get_namespace_name](get_namespace_name.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - [map_variable_attnos](../m/map_variable_attnos.md)
  - [get_attname](get_attname.md)
  - [get_atttype](get_atttype.md)
  - [get_collation](get_collation.md)
  - [get_opclass](get_opclass.md)
  - [get_attoptions](get_attoptions.md)
  - [untransformRelOptions](../u/untransformRelOptions.md)
  - [exprType](../e/exprType.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - [DefineRelation](../D/DefineRelation.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)
  - [expandTableLikeClause](../e/expandTableLikeClause.md)

## Notes and Other Information
- Does not preserve the original index name, allowing DefineIndex to choose a new name
- Rejects whole-row table references in expressions and predicates to prevent future incompatibilities
- Handles both key columns (indnkeyatts) and included columns (indnatts) separately
- Processes exclusion constraints by extracting operator names from pg_constraint
- Supports partial indexes by translating predicate expressions
- Copies index options and column-specific options like collation and operator class
- Sets transformed=true to skip transformIndexStmt processing
- Maintains proper sort ordering and null handling options for ordered access methods