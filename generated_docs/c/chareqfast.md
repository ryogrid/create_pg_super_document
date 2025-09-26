# chareqfast

## Location
src/backend/utils/cache/catcache.c: 191 - 196

## Overview
A fast equality comparison function for single-byte character (char) data types used as catalog cache keys in PostgreSQL.

## Definition


## Detailed Description
chareqfast is a performance-optimized equality comparison function specifically designed for single-byte character data types used as keys in PostgreSQL's catalog cache system. It provides a faster alternative to calling the standard SQL-callable equality functions by directly comparing the character values extracted from Datum parameters. This function is part of PostgreSQL's catalog cache optimization strategy, where frequently accessed data types get specialized comparison functions to avoid the overhead of DirectFunctionCallN() calls. The function simply extracts character values from both Datum parameters using DatumGetChar() and performs a direct comparison, making it significantly faster than the general-purpose equality functions.

## Parameters / Member Variables
- : The first Datum containing a character value to compare
- : The second Datum containing a character value to compare

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetChar (inline function that extracts char from Datum)
- Called from (representative examples):
  - GetCCHashEqFuncs (for BOOLOID at line 280)
  - GetCCHashEqFuncs (for CHAROID at line 285)

## Notes and Other Information
- This is a static function defined in src/backend/utils/cache/catcache.c (lines 191-196)
- Part of a suite of fast comparison functions for performance-critical catalog cache operations
- Used for both BOOLOID and CHAROID data types in the catalog cache key comparison system
- Avoids the overhead of standard SQL function calls by directly comparing extracted values
- Returns true if both character values are equal, false otherwise
- Critical for catalog cache performance as character comparisons are frequently performed during cache lookups
- Works in conjunction with charhashfast for complete key handling in hash-based catalog cache structures
- The optimization provides substantial performance improvements for catalog cache operations involving character data types