# similar_to_escape_2

## Location
src/backend/utils/adt/regexp.c: 1032 - 1047

## Overview
A PostgreSQL SQL function wrapper that converts a SIMILAR TO pattern with an escape character to POSIX-style regular expression format.

## Definition


## Detailed Description
This function implements the PostgreSQL SQL function  that takes two text arguments: a SIMILAR TO pattern and an escape character. It serves as a thin wrapper around , which performs the actual conversion from SQL SIMILAR TO syntax to POSIX regular expression syntax that can be used by PostgreSQL's regexp engine.

The function is part of PostgreSQL's SQL standard compliance for the SIMILAR TO operator, which provides pattern matching capabilities similar to LIKE but with more advanced regular expression features. The escape parameter allows users to specify a custom escape character for the pattern.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The SIMILAR TO pattern text to be converted
  -  (arg 1): The escape character text (single character or empty string)

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting text arguments)
  -  (performs the actual pattern conversion)
  -  (macro for returning text result)
- Called from:
  - SQL queries using  function

## Notes and Other Information
- This is the 2-argument version of the similar_to_escape function family
- The function directly delegates all processing to 
- The escape character can be NULL (uses default backslash), empty string (no escape), or a single character
- Multi-character escape strings are rejected with an error
- Located in 
- The converted pattern includes anchors (^ and $) and non-capturing groups to ensure proper SQL SIMILAR TO semantics