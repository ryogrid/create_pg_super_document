# rowtype_field_matches

## Location
src/backend/optimizer/util/clauses.c: 2186 - 2253

## Overview
Validates that a field in a row type still matches its expected data type characteristics, protecting against invalid optimizations after ALTER COLUMN TYPE operations.

## Definition
```c
static bool rowtype_field_matches(Oid rowtypeid, int fieldnum,
                                  Oid expectedtype, int32 expectedtypmod,
                                  Oid expectedcollation)
```

## Detailed Description
This static helper function is crucial for maintaining query correctness during constant expression evaluation. It verifies that a specific field within a row type still has the same data type characteristics as when an expression was originally parsed and planned.

The function serves as a safeguard against improper query optimization that could occur after DDL operations like ALTER COLUMN TYPE. Without this check, the optimizer might incorrectly simplify expressions based on outdated type information, leading to runtime errors or incorrect results.

The validation process involves:
1. Handling the special case of RECORDOID (which cannot be altered)
2. Looking up the current tuple descriptor for the row type
3. Validating the field number is within bounds
4. Checking that the field is not dropped
5. Comparing the current type, type modifier, and collation against expected values

## Parameters / Member Variables
- `rowtypeid`: OID of the row type containing the field to check
- `fieldnum`: 1-based field number within the row type
- `expectedtype`: Expected data type OID for the field
- `expectedtypmod`: Expected type modifier (for precision, scale, etc.)
- `expectedcollation`: Expected collation OID for the field

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_rowtype_tupdesc_domain](../l/lookup_rowtype_tupdesc_domain.md) (retrieves tuple descriptor, handling domains)
  - ReleaseTupleDesc (deallocates tuple descriptor resources)
- Called from (representative examples):
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md) (during expression simplification)

## Notes and Other Information
- Static function - internal to clauses.c module
- Essential for query correctness after schema changes
- Handles domains over composite types correctly
- RECORDOID types always pass validation as they cannot be altered
- Properly manages tuple descriptor memory by releasing resources
- Part of PostgreSQL's defense against optimization bugs after DDL operations
- Returns false for dropped columns to prevent access to invalid fields
- May need similar checks in other parts of the system (as noted in code comments)