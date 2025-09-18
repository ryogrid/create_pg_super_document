# nameiclike

## Location
src/backend/utils/adt/like.c: 370 - 384

## Overview
A PostgreSQL function that performs case-insensitive LIKE pattern matching on Name data types using the ILIKE operator.

## Definition


## Detailed Description
The  function implements case-insensitive pattern matching for PostgreSQL's Name data type. It takes a Name value and a text pattern as input, converts the Name to text format, and then uses the generic case-insensitive text matching function  to perform the pattern matching operation. This function is the backend implementation for the ILIKE operator when applied to Name data types.

The function follows PostgreSQL's standard function call convention using  and returns a Datum containing a boolean result indicating whether the Name matches the pattern in a case-insensitive manner.

## Parameters / Member Variables
- : The Name value to be matched against the pattern
- : The text pattern to match against (supports SQL LIKE wildcards % and _)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract Name argument from function call
  -  - Extract text pattern argument 
  -  - Convert Datum to text pointer
  -  - Direct function call interface
  -  - Convert Name to text format
  -  - Convert Name to Datum
  -  - Perform case-insensitive pattern matching
  -  - Get collation information for the operation
  -  - Constant representing successful match
  -  - Return boolean result
- Called from: 
  - This function is called through PostgreSQL's function manager when the ILIKE operator is used with Name data types

## Notes and Other Information
- This function is part of PostgreSQL's LIKE/ILIKE operator implementation
- It specifically handles the case-insensitive variant (ILIKE) for Name data types
- The function converts Name to text before processing since the generic matching function works with text
- Located in src/backend/utils/adt/like.c:370-384
- Uses PostgreSQL's collation system for proper case-insensitive matching across different locales