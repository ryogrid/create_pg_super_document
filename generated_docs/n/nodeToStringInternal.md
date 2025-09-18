# nodeToStringInternal

## Location
src/backend/nodes/outfuncs.c: 770 - 790

## Overview
A static helper function that converts a PostgreSQL node structure to its ASCII string representation, with control over whether location fields are included in the output.

## Definition
static char *nodeToStringInternal(const void *obj, bool write_loc_fields)

## Detailed Description
nodeToStringInternal is the core implementation function for converting PostgreSQL parse tree nodes and other structures into their string representations. It serves as the common backend for both nodeToString and nodeToStringWithLocations functions. The function creates a StringInfo buffer, temporarily sets a global flag to control location field output, calls the main output function (outNode), and then restores the original location field setting. This approach allows for consistent string conversion while providing flexibility in whether debugging location information is included.

## Parameters / Member Variables
- obj: A pointer to the PostgreSQL node or structure to be converted to string format
- write_loc_fields: A boolean flag that determines whether location fields should be output with their actual values (true) or as -1 (false)

## Dependencies
- Functions called/Symbols referenced:
  - outNode
  - initStringInfo
  - StringInfoData (struct)
- Called from (representative examples):
  - nodeToString
  - nodeToStringWithLocations

## Notes and Other Information
- This is a static function, meaning it's only accessible within the outfuncs.c file
- The function manipulates the global write_location_fields variable to control output format
- Location fields are typically set to -1 in most use cases since the original query string is usually not available
- The actual location values can be useful for debugging purposes
- Memory for the returned string is allocated using PostgreSQL's palloc mechanism
- The function follows PostgreSQL's pattern of using StringInfo for efficient string building