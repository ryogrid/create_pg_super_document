# similar_escape_internal

## Location
src/backend/utils/adt/regexp.c: 767 - 1031

## Overview
Converts SQL "SIMILAR TO" regexp patterns to POSIX style for use with PostgreSQL's regexp engine, handling escape sequences and special character transformations.

## Definition


## Detailed Description
The  function serves as the core implementation for PostgreSQL's "SIMILAR TO" pattern matching functionality. It transforms SQL standard "SIMILAR TO" patterns into POSIX-compatible regular expressions that can be processed by PostgreSQL's regexp engine.

The function performs several key transformations:
- Wraps the pattern with  to ensure full string matching as required by SQL spec
- Handles escape-double-quote sequences for SUBSTRING pattern separation
- Converts SQL wildcards ( to ,  to )
- Processes character classes with proper bracket handling
- Manages escape sequences and special character escaping
- Supports multi-byte character encodings

For SUBSTRING operations, the function creates a three-part pattern structure with specific greedy/non-greedy quantifiers to ensure SQL-compliant behavior where the middle part (between escape-double-quotes) captures the largest possible match.

## Parameters / Member Variables
-  (text*): The input SQL "SIMILAR TO" pattern to be converted
-  (text*): The escape character specification (NULL for default '\', empty string for no escape)

## Dependencies
- Functions called/Symbols referenced:
  -  (calculates multi-byte string length)
  -  (macro to access variable-length data)
  -  (gets multi-byte character length)
  -  (macro to set variable-length data size)
- Called from (representative examples):
  -  (src/backend/utils/adt/regexp.c:1038)
  -  (src/backend/utils/adt/regexp.c:1053)
  -  (src/backend/utils/adt/regexp.c:1082)

## Notes and Other Information
- Static function serving as the common implementation for three SQL-exposed functions
- Handles complex pattern transformation including character class nesting and escape-double-quote separators
- Supports both single-byte and multi-byte character encodings with optimized fast/slow paths
- Implements SQL standard requirements for pattern anchoring and greedy matching behavior
- Enforces SQL spec limitation of at most two escape-double-quote separators
- Allocates result buffer with sufficient space (up to 3 bytes output per input byte)
- Critical component for PostgreSQL's SIMILAR TO and SUBSTRING pattern matching functionality