# SPI_getvalue

## Location
src/backend/executor/spi.c: 1220 - 1251

## Overview
Extracts a specific attribute value from a tuple and converts it to its string representation using the appropriate output function.

## Definition


## Detailed Description
SPI_getvalue retrieves the value of a specific attribute from a tuple and converts it to a human-readable string representation. The function handles the complete process of extracting the raw Datum value, checking for NULL values, determining the appropriate data type, and calling the corresponding output function to convert the value to a string. This function is essential for displaying tuple data in a readable format or for converting values for external interfaces.

The function supports both regular attributes (positive attribute numbers) and system attributes (negative attribute numbers). It properly handles NULL values by returning NULL, and uses the PostgreSQL type system to ensure correct string conversion for all data types.

## Parameters / Member Variables
- : The HeapTuple containing the data to extract from
- : The TupleDesc that describes the structure and types of the tuple
- : The 1-based attribute number for regular attributes, or negative number for system attributes

## Dependencies
- Functions called/Symbols referenced:
  - heap_getattr (to extract the raw attribute value)
  - TupleDescAttr (macro for accessing tuple descriptor attributes)
  - SystemAttributeDefinition (for system attribute type information)
  - getTypeOutputInfo (to get the output function for the data type)
  - OidOutputFunctionCall (to convert the value to string using the appropriate output function)
- Called from (representative examples):
  - refresh_by_match_merge (materialized view operations)
  - make_ruledef (rule definition utilities)
  - make_viewdef (view definition utilities)

## Notes and Other Information
- Sets SPI_result to SPI_ERROR_NOATTRIBUTE if fnumber is invalid (0, > natts, or <= FirstLowInvalidHeapAttributeNumber)
- Returns NULL if the attribute value is NULL (no string conversion needed)
- Returns a newly allocated string that must be freed by the caller
- Uses the PostgreSQL type system to ensure proper string conversion for all data types
- Handles both built-in types (int4, text, etc.) and user-defined types
- The string representation follows PostgreSQL's standard output format for each data type
- System attributes (ctid, oid, etc.) are properly handled with their specific output functions
- Essential for debugging, logging, and interfacing with external systems that expect string data