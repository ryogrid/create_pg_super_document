# quote_qualified_identifier

## Location
src/backend/utils/adt/ruleutils.c: 12783 - 12802

## Overview
A utility function that constructs a possibly-qualified identifier string by combining a qualifier and an identifier, quoting each component as necessary for safe SQL usage.

## Definition


## Detailed Description
This function creates a qualified identifier in the format "qualifier.ident" or just "ident" if no qualifier is provided. It automatically applies proper SQL identifier quoting to both the qualifier and identifier components using the quote_identifier() function. The result is allocated using palloc and must be freed by the caller. This is commonly used throughout PostgreSQL for generating safe SQL identifiers that may contain special characters or reserved words.

## Parameters / Member Variables
- `qualifier`: Optional schema or namespace qualifier; if NULL, only the identifier is returned
- `ident`: The main identifier name that will always be included in the result

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo
  - appendStringInfo  
  - appendStringInfoString
  - quote_identifier
- Called from (representative examples):
  - getObjectDescription (multiple calls in objectaddress.c)
  - generate_relation_name
  - generate_qualified_relation_name
  - generate_function_name
  - format_type_extended
  - regprocout
  - regclassout

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Handles NULL qualifier gracefully by omitting the qualifier part entirely
- Essential for generating safe SQL identifiers in PostgreSQL's rule and utility systems
- Used extensively in object description and identity functions throughout the codebase
- Part of the ruleutils.c module which handles SQL generation and formatting utilities