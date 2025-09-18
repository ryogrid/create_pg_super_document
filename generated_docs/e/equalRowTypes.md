# equalRowTypes

## Location
[src/backend/access/common/tupdesc.c:586-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L586-L621)

## Overview
equalRowTypes determines whether two TupleDesc structures represent compatible row types by comparing only the fields relevant to logical row structure, ignoring physical storage and table metadata details.

## Definition


## Detailed Description
This function performs a specialized comparison focused solely on row type compatibility rather than complete TupleDesc equality. It is designed for scenarios where two record types need to be verified as compatible for operations like function returns, record type comparisons, or type cache lookups.

The comparison specifically checks:
1. **Structural compatibility**: Same number of attributes and composite type ID (allowing both to be zero)
2. **Attribute compatibility**: For each corresponding attribute position, compares name, data type, type modifier, and collation
3. **Dropped column consistency**: Ensures both TupleDescs have the same dropped column status

Unlike , this function deliberately ignores many pg_attribute fields that define physical storage (like , , , ) or table metadata (like , , ). It also intentionally skips  comparison to allow type cache optimization.

## Parameters / Member Variables
- : First TupleDesc to compare for row type compatibility
- : Second TupleDesc to compare for row type compatibility

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic comparison operations and TupleDescAttr macro)
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md) (validating function return types)
  - [shared_record_table_compare](../s/shared_record_table_compare.md) (type cache comparisons)
  - [RevalidateCachedQuery](../R/RevalidateCachedQuery.md) (plan cache validation)
  - [record_type_typmod_compare](../r/record_type_typmod_compare.md) (type modifier comparisons)

## Notes and Other Information
- More permissive than  as it focuses only on logical row type compatibility
- Deliberately excludes  comparison to enable type cache optimizations
- Does not check array dimensions () as discussed but not implemented
- Handles record types derived from tables by checking dropped column consistency
- Used primarily for type compatibility checking rather than exact structural equality
- Essential for determining if record types can be used interchangeably in expressions and function calls