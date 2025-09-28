# equalRowTypes

## Location
[src/backend/access/common/tupdesc.c:586-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L586-L621)

## Overview
equalRowTypes determines whether two TupleDesc structures represent compatible row types by comparing only the fields relevant to logical row structure, ignoring physical storage and table metadata details.

## Definition

```c
structure in
 *		a previously allocated tuple descriptor.
 *
 * If attributeName is NULL, the attname field is set to an empty string
 * (this is for cases where we don't know or need a name for the field).
 * Also, some callers use this function to change the datatype-related fields
 * in an existing tupdesc;
```
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

## Simplified Source

```c
// Simplified version of equalRowTypes
bool equalRowTypes(TupleDesc tupdesc1, TupleDesc tupdesc2) {
    // Check basic structural compatibility
    if (tupdesc1->natts != tupdesc2->natts)
        return false;
    if (tupdesc1->tdtypeid != tupdesc2->tdtypeid)
        return false;

    // Compare each attribute for type compatibility
    for (int i = 0; i < tupdesc1->natts; i++) {
        Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
        Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

        // Check attribute name, type, typmod, and collation
        if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0)
            return false;
        if (attr1->atttypid != attr2->atttypid)
            return false;
        if (attr1->atttypmod != attr2->atttypmod)
            return false;
        if (attr1->attcollation != attr2->attcollation)
            return false;

        // Ensure consistency for dropped fields
        if (attr1->attisdropped != attr2->attisdropped)
            return false;
    }

    return true;
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Focused on the core comparison logic
- Maintained all essential compatibility checks
- Streamlined the attribute iteration and comparison