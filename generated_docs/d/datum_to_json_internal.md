# datum_to_json_internal

## Location
[src/backend/utils/adt/json.c:177-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L177-L300)

## Overview
Converts PostgreSQL datums to their JSON text representation, handling type-specific formatting and escaping based on the data type category.

## Definition


## Detailed Description
The  function is the core conversion engine for transforming PostgreSQL data values into their JSON text representation. It handles the complex logic of determining how each PostgreSQL data type should be represented in JSON format, including proper quoting, escaping, and formatting rules. The function dispatches to type-specific conversion logic based on the provided type category and handles special cases for JSON object keys versus values.

This internal function is the foundation for PostgreSQL's JSON conversion infrastructure and is called by various higher-level JSON functions. It ensures that PostgreSQL data types are converted to JSON in a consistent and standards-compliant manner, handling edge cases like null values, special numeric values (NaN, Infinity), and different temporal types.

## Parameters / Member Variables
- : The PostgreSQL datum value to convert to JSON
- : Boolean flag indicating whether the value is NULL
- : StringInfo buffer where the JSON representation will be appended
- : JsonTypeCategory enum indicating how the type should be handled (array, composite, boolean, numeric, date, timestamp, etc.)
- : OID of the output function to use for type conversion (may be output function or cast function depending on category)
- : Boolean flag indicating whether this value is being used as a JSON object key (affects quoting behavior)

## Dependencies
- Functions called/Symbols referenced:
  - : Prevents stack overflow in recursive calls
  - : Efficiently appends binary data to StringInfo
  - : Converts PostgreSQL arrays to JSON arrays
  - : Converts composite types (records) to JSON objects
  - : Extracts boolean value from datum
  - : Calls type-specific output function
  - : Formats date/time values for JSON
  - : Extracts text value with potential detoasting
  - : Calls cast functions for type conversion
  - : Escapes string values for JSON format
  - , : Macros for variable-length data handling
- Called from (representative examples):
  - : Public interface for single datum conversion
  - : Array element conversion during array processing
  - : Field conversion during composite type processing
  - : Aggregate function processing
  - : Object aggregation processing

## Notes and Other Information
- Enforces JSON standards by rejecting arrays, composites, and JSON types as object keys
- Handles special numeric values (NaN, Infinity) by quoting them as strings
- Uses optimized numeric validation for performance when determining whether to quote numbers
- Supports all PostgreSQL temporal types with proper ISO 8601 formatting
- Implements recursive stack depth checking to prevent infinite recursion
- Handles both regular output functions and cast functions depending on type category
- JSON and JSONB values are passed through without additional escaping since they're already in JSON format
- Provides efficient binary string operations for better performance with large JSON documents