# datum_to_json

## Location
src/backend/utils/adt/json.c: 754 - 769

## Overview
The datum_to_json function converts a PostgreSQL Datum value to JSON text format, serving as the core conversion engine for JSON output operations.

## Definition
```c
Datum datum_to_json(Datum val, JsonTypeCategory tcategory, Oid outfuncoid)
```

## Detailed Description
This function performs the actual conversion of a PostgreSQL Datum to JSON text representation. It acts as an intermediary that prepares a StringInfo buffer and delegates the detailed conversion work to datum_to_json_internal(). The function handles the final packaging of the JSON text into a PostgreSQL text datum that can be returned to the caller. It's designed to work with pre-categorized type information for efficiency.

## Parameters / Member Variables
- `val`: The Datum value to be converted to JSON
- `tcategory`: JsonTypeCategory indicating how the value should be treated during JSON conversion
- `outfuncoid`: OID of the output function to use for the conversion (from previous json_categorize_type call)

## Dependencies
- Functions called/Symbols referenced:
  - makeStringInfo (to create output buffer)
  - [datum_to_json_internal](datum_to_json_internal.md) (to perform the actual conversion logic)
  - cstring_to_text_with_len (to convert result to PostgreSQL text datum)
- Called from:
  - [to_json](../t/to_json.md) (main SQL function entry point)
  - [ExecEvalJsonConstructor](../E/ExecEvalJsonConstructor.md) (from executor for JSON constructors)

## Notes and Other Information
- Requires pre-categorized type information from json_categorize_type for optimal performance
- Uses StringInfo for efficient string building during conversion
- Located in src/backend/utils/adt/json.c:754-769
- Part of the internal JSON conversion infrastructure used by multiple PostgreSQL components
- The tcategory and outfuncoid parameters must come from a previous json_categorize_type call