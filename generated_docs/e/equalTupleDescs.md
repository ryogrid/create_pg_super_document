# equalTupleDescs

## Location
[src/backend/access/common/tupdesc.c:419-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L419-L585)

## Overview
equalTupleDescs performs comprehensive logical equality comparison between two TupleDesc structures, checking all attributes and constraints to determine if they represent equivalent tuple descriptors.

## Definition


## Detailed Description
This function implements deep equality comparison for TupleDesc structures, going beyond simple pointer comparison to check logical equivalence. The comparison process includes:

1. **Basic structure comparison**: Verifies that both TupleDescs have the same number of attributes () and the same type identifier ()
2. **Attribute-by-attribute comparison**: For each attribute position, compares all relevant pg_attribute fields including name, type, length, dimensions, type modifier, storage properties, nullability, and inheritance information
3. **Constraint comparison**: If constraints exist, compares default values, missing values, and check constraints in detail

The function deliberately ignores certain fields that are not semantically relevant for equality, such as , , and . For dropped columns, it still performs complete comparison since  may be zero.

## Parameters / Member Variables
- : First TupleDesc to compare
- : Second TupleDesc to compare

## Dependencies
- Functions called/Symbols referenced:
  - [TupleConstr](../T/TupleConstr.md) (constraint structure access)
  - [AttrDefault](../A/AttrDefault.md) (default value structures)
  - AttrMissing (missing value structures)
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