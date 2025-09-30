# check_collation_set

## Location
[src/backend/utils/adt/varlena.c:1510-1538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1510-L1538)

## Overview
Validates that a collation OID is set and reports an error if it is invalid or not determined.

## Definition

```c
static void
check_collation_set(Oid collid)
```
## Detailed Description
This function validates that a collation identifier (OID) is valid and properly set. When called with an invalid OID (typically InvalidOid), it reports a specific error indicating that the collation could not be determined for string comparison operations. This typically occurs when the PostgreSQL parser encounters conflicting implicit collations and cannot resolve which one to use. The function serves as a centralized validation point for collation requirements in string comparison functions, ensuring that operations requiring collation have a valid collation context before proceeding.

## Parameters / Member Variables
- : The collation OID to validate

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro to check if OID is valid)
  - ereport (error reporting function)
  - ERROR, ERRCODE_INDETERMINATE_COLLATION (error constants)
- Called from (representative examples):
  - [bpchareq](../b/bpchareq.md), bpcharne (bpchar comparison functions)
  - [texteq](../t/texteq.md), textne (text comparison functions) 
  - [text_starts_with](../t/text_starts_with.md) (text prefix checking)
  - [varstr_cmp](../v/varstr_cmp.md) (variable string comparison)
  - [varstr_sortsupport](../v/varstr_sortsupport.md) (sort support for variable strings)

## Notes and Other Information
- This is a static function, accessible only within varchar.c
- The function does not return on invalid collation - it always throws an error
- Used primarily by string comparison and text manipulation functions that require collation context
- The error message specifically suggests using the COLLATE clause to resolve ambiguity
- Part of PostgreSQL's collation support system for internationalization and locale-specific string operations
- Helps ensure consistent collation handling across different string comparison operations in the system

## Simplified Source

```c
static void check_collation_set(Oid collid) {
    // Check if collation ID is valid
    if (!OidIsValid(collid)) {
        // Report error when collation cannot be determined
        ereport(ERROR,
                (errcode(ERRCODE_INDETERMINATE_COLLATION),
                 errmsg("could not determine which collation to use for string comparison"),
                 errhint("Use the COLLATE clause to set the collation explicitly.")));
    }
}
```