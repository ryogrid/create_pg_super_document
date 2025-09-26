# get_const_collation

## Location
[src/backend/utils/adt/ruleutils.c:11265-11284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11265-L11284)

## Overview
A static helper function within the rule decompilation system that appends a COLLATE clause to the output buffer when a constant value has a non-default collation.

## Definition
```c
static void get_const_collation(Const *constval, deparse_context *context)
```

## Detailed Description
This function is a specialized helper used during SQL rule decompilation to handle collation information for constant values. It examines whether a constant (Const node) has an explicit collation that differs from the default collation for its data type. If so, it appends the appropriate COLLATE clause to the output buffer to ensure the decompiled rule accurately represents the original collation specification.

The function works by:
1. Checking if the constant has a valid collation OID
2. Retrieving the default collation for the constant's data type  
3. Comparing the constant's collation with the type's default collation
4. Appending a COLLATE clause only when they differ

This approach ensures that only non-default collations are explicitly shown in the decompiled output, making the result more readable while preserving semantic correctness.

## Parameters / Member Variables
- `constval`: Pointer to a Const node containing the constant value and its collation information
- `context`: Pointer to a deparse_context structure containing the output buffer and other decompilation state

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro to check if OID is valid)
  - get_typcollation (retrieves default collation for a data type)
  - appendStringInfo (appends formatted text to StringInfo buffer)
  - generate_collation_name (converts collation OID to collation name)
- Called from (representative examples):
  - get_const_expr (main constant expression decompilation function)

## Notes and Other Information
- This is a static function local to ruleutils.c, indicating it's an internal implementation detail
- Part of PostgreSQL's rule decompilation system which converts internal query representations back to SQL text
- The function only outputs COLLATE when necessary, avoiding redundant collation specifications
- Located in src/backend/utils/adt/ruleutils.c:11265-11284