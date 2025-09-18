# tsqueryin

## Location
src/backend/utils/adt/tsquery.c: 952 - 973

## Overview
The  function is a PostgreSQL I/O function that converts a text string into a TSQuery data type without applying any morphological processing or text search configuration.

## Definition


## Detailed Description
This function serves as the input function for the TSQuery data type in PostgreSQL's type system. It takes a C-string representation of a text search query and converts it directly into the internal TSQuery format. The key characteristic of this function is that it performs "raw" parsing without any morphological analysis - meaning it doesn't apply stemming, dictionary lookups, or other text processing that would normally be done by text search configurations.

The function uses  as its callback, which ensures that operands are added to the query exactly as they appear in the input string. This makes it suitable for cases where the query terms are already in their desired final form, or when morphological processing is not wanted.

The function follows PostgreSQL's standard I/O function conventions, taking arguments through the  macro and returning a  through .

## Parameters / Member Variables
- Function uses  calling convention:
  - : Input string containing the tsquery representation
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [parse_tsquery](../p/parse_tsquery.md)
  - [pushval_asis](../p/pushval_asis.md)
  - PG_RETURN_TSQUERY
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - PostgreSQL type system (no direct code references found, called via function manager)

## Notes and Other Information
- This is a PostgreSQL I/O function registered in the system catalogs for the TSQuery data type
- The comment "in without morphology" emphasizes that no text search configuration processing is applied
- Uses flags value of 0, meaning standard tsquery syntax without special parsing modes
- Passes NULL as the opaque parameter since no additional state is needed for the simple pushval_asis callback
- Error handling is managed through the escontext parameter for proper soft error support
- This function is typically called indirectly through PostgreSQL's type conversion system rather than being called directly by user code