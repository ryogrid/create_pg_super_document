# equalTupleDescs

## Location
[src/backend/access/common/tupdesc.c:419-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L419-L585)

## Overview
equalTupleDescs performs comprehensive logical equality comparison between two TupleDesc structures, checking all attributes and constraints to determine if they represent equivalent tuple descriptors.

## Definition

```c
bool
equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2)
```
## Detailed Description
This function implements deep equality comparison for TupleDesc structures, going beyond simple pointer comparison to check logical equivalence. The comparison process includes:

1. **Basic structure comparison**: Verifies that both TupleDescs have the same number of attributes () and the same type identifier ()
2. **Attribute-by-attribute comparison**: For each attribute position, compares all relevant pg_attribute fields including name, type, length, dimensions, type modifier, storage properties, nullability, and inheritance information
3. **Constraint comparison**: If constraints exist, compares default values, missing values, and check constraints in detail

The function deliberately ignores certain fields that are not semantically relevant for equality, such as , , and . For dropped columns, it still performs complete comparison since  may be zero.

## Parameters / Member Variables
- `tupdesc1`: First TupleDesc to compare
- `tupdesc2`: Second TupleDesc to compare
## Dependencies
- Functions called/Symbols referenced:
  - [TupleConstr](../T/TupleConstr.md) (constraint structure access)
  - [AttrDefault](../A/AttrDefault.md) (default value structures)
  - [AttrMissing](../A/AttrMissing.md) (missing value structures)
  - [datumIsEqual](../d/datumIsEqual.md) (value comparison for missing values)
  - [ConstrCheck](../C/ConstrCheck.md) (check constraint structures)
- Called from (representative examples):
  - [RelationFindReplTupleSeq](../R/RelationFindReplTupleSeq.md) (in replication logic)
  - [RelationClearRelation](../R/RelationClearRelation.md) (in relation cache management)
  - ReleaseTupleDesc (for optimization in release logic)

## Notes and Other Information
- Returns  only if TupleDescs are logically equivalent in all checked aspects
- Ignores  and  at the TupleDesc level as they're not part of logical equality
- Skips  comparison as it may not be set consistently
- Assumes AttrDefault arrays are sorted by  and ConstrCheck arrays are sorted by name
- For missing values, performs deep comparison using  when values are present
- Comprehensive constraint comparison includes default values, missing values, and check constraints with their validity and inheritance flags

## Simplified Source

```c
bool equalTupleDescs(TupleDesc tupdesc1, TupleDesc tupdesc2) {
    // Quick checks: compare basic structure
    if (tupdesc1->natts != tupdesc2->natts ||
        tupdesc1->tdtypeid != tupdesc2->tdtypeid) {
        return false;
    }

    // Compare each attribute in detail
    for (int i = 0; i < tupdesc1->natts; i++) {
        Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
        Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

        // Compare all significant attribute fields
        if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) != 0 ||
            attr1->atttypid != attr2->atttypid ||
            attr1->attlen != attr2->attlen ||
            attr1->attndims != attr2->attndims ||
            attr1->atttypmod != attr2->atttypmod ||
            attr1->attbyval != attr2->attbyval ||
            attr1->attalign != attr2->attalign ||
            attr1->attstorage != attr2->attstorage ||
            attr1->attcompression != attr2->attcompression ||
            attr1->attnotnull != attr2->attnotnull ||
            attr1->atthasdef != attr2->atthasdef ||
            attr1->attidentity != attr2->attidentity ||
            attr1->attgenerated != attr2->attgenerated ||
            attr1->attisdropped != attr2->attisdropped ||
            attr1->attislocal != attr2->attislocal ||
            attr1->attinhcount != attr2->attinhcount ||
            attr1->attcollation != attr2->attcollation) {
            return false;
        }
    }

    // Compare constraints if present
    TupleConstr *constr1 = tupdesc1->constr;
    TupleConstr *constr2 = tupdesc2->constr;

    if (constr1 != NULL) {
        if (constr2 == NULL ||
            constr1->has_not_null != constr2->has_not_null ||
            constr1->has_generated_stored != constr2->has_generated_stored ||
            constr1->num_defval != constr2->num_defval ||
            constr1->num_check != constr2->num_check) {
            return false;
        }

        // Compare default values, missing values, and check constraints
        // (detailed comparisons for defval, missing, and check arrays)
        // ... constraint comparison logic ...
    } else if (constr2 != NULL) {
        return false;
    }

    return true;
}
```