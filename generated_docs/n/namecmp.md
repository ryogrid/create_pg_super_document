# namecmp

## Location
src/backend/utils/adt/name.c: 135 - 147

## Overview
The  function performs comparison operations between two Name values, supporting both C collation (fast path) and locale-aware collation through PostgreSQL's collation infrastructure.

## Definition

```c
structure */
	return varstr_cmp(NameStr(*arg1), strlen(NameStr(*arg1)),
					  NameStr(*arg2), strlen(NameStr(*arg2)),
					  collid);
```
## Detailed Description
The  function is a static helper function that implements the core comparison logic for PostgreSQL's Name data type. It supports collation-aware comparison, providing different behavior based on the specified collation ID.

The function implements a two-path approach:
1. **Fast path for C collation**: When  equals , it uses  with  limit for maximum performance
2. **General collation path**: For other collations, it delegates to  which handles locale-aware string comparison according to the specified collation rules

The function uses  with  primarily for historical reasons, as the comment notes that  would work equally well since Name values are guaranteed to be null-terminated. Any data beyond the null terminator is not considered relevant for comparisons.

This function serves as the foundation for all Name comparison operations in PostgreSQL, including equality, inequality, and ordering operations.

## Parameters / Member Variables
- : First Name value to compare
- : Second Name value to compare  
- : Collation OID specifying the comparison rules to use

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C string comparison function (for C collation)
  - : PostgreSQL's variable-length string comparison function (for other collations)
  - : Standard C string length function
  - : Macro to access the string data within Name structures
  - : Maximum length constant for Name type
  - : Constant representing C/POSIX collation
  - : PostgreSQL Name data type
  - : PostgreSQL object identifier type

- Called from (representative examples):
  - : Name equality comparison
  - : Name inequality comparison  
  - : Name less-than comparison
  - : Name less-than-or-equal comparison
  - : Name greater-than comparison
  - : Name greater-than-or-equal comparison
  - : B-tree comparison function for Name type

## Notes and Other Information
- This is a static function, only accessible within the name.c source file
- Provides optimized fast path for C collation which is commonly used in system catalogs
- The use of  with  is mostly historical -  would work equally well
- Supports PostgreSQL's full collation infrastructure for internationalization
- Serves as the foundation for all Name comparison operators (=, <>, <, <=, >, >=)
- Essential for B-tree indexing and sorting operations on Name columns
- The function assumes both Name arguments are properly null-terminated
- Part of PostgreSQL's comprehensive type system supporting both performance and internationalization