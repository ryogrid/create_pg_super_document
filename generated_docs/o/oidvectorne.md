# oidvectorne

## Location
[src/backend/utils/adt/oid.c:352-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L352-L359)

## Overview
PostgreSQL comparison function that determines if two oidvector values are not equal, returning true if they differ and false if they are identical.

## Definition


## Detailed Description
The  function implements the "not equal" operator (<>) for oidvector data types in PostgreSQL. It serves as a wrapper around the  comparison function, which performs a comprehensive comparison of two oidvector values considering both their dimensions and individual OID values. The function returns a boolean result indicating whether the two input oidvectors are different.

The comparison logic follows PostgreSQL's standard approach where vectors are first compared by length (dimension), and then element-by-element if lengths are equal. If any difference is found at any level, the vectors are considered not equal.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: First oidvector to compare
  - Argument 1: Second oidvector to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Core comparison function that returns integer comparison result
  - : Macro to extract int32 value from Datum
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's operator implementation system for oidvector types
- Returns boolean true (non-zero comparison result) when vectors differ, false when identical
- The underlying comparison in  prioritizes vector length differences before comparing individual elements
- Typically invoked through SQL expressions using the <> or != operators on oidvector columns
- Located in src/backend/utils/adt/oid.c:352-359