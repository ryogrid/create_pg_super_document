# CRSERR_TOO_MANY

## Location
src/backend/parser/parse_target.c: 1163 - 1292

## Overview
An enumeration constant used in ExpandColumnRefStar() to indicate an error condition when a column reference contains too many dotted name components.

## Definition


## Detailed Description
CRSERR_TOO_MANY is one of three error classification constants defined within the ExpandColumnRefStar() function to track different types of column reference resolution failures. This specific constant represents the error condition when a qualified column reference contains more than four dotted name components.

PostgreSQL supports qualified names in the following formats:
- 1 component:  (bare star)
- 2 components: 
- 3 components: 
- 4 components: 

When a column reference exceeds this maximum of 4 components, the parser sets crserr to CRSERR_TOO_MANY and later generates a syntax error with the message "improper qualified name (too many dotted names)".

This error classification is part of PostgreSQL's defensive parsing strategy to provide clear, specific error messages for different types of malformed qualified names rather than generic parsing failures.

## Parameters / Member Variables
This is an enumeration constant with no parameters or member variables.

## Dependencies
- Functions called/Symbols referenced:
  - Used within ExpandColumnRefStar function context
  - Referenced in error reporting via ereport()
  - Works with NameListToString() for error message formatting
- Called from (representative examples):
  - ExpandColumnRefStar (src/backend/parser/parse_target.c:1214) - assignment
  - ExpandColumnRefStar (src/backend/parser/parse_target.c:1263) - case handling

## Notes and Other Information
- This is a local enumeration defined within the ExpandColumnRefStar() function scope
- The enumeration is used in a switch statement to handle different error conditions systematically
- When CRSERR_TOO_MANY is triggered, it results in an ERRCODE_SYNTAX_ERROR
- The error message includes the actual malformed qualified name for better user feedback
- This is part of PostgreSQL's comprehensive error handling for SQL name resolution
- The constant represents a design decision to limit qualified name depth for clarity and implementation simplicity