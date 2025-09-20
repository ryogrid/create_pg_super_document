# jsonb_from_text

## Location
[src/backend/utils/adt/jsonb.c:147-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L147-L158)

## Overview
The  function converts a PostgreSQL text value containing JSON data into internal JSONB format, with optional validation for unique object keys.

## Definition

```c
Datum
jsonb_from_text(text *js, bool unique_keys)
```
## Detailed Description
This function provides a convenient interface for converting PostgreSQL text values to JSONB format. Unlike the standard input functions that work with C strings, this function operates directly on PostgreSQL's text data type, handling the necessary data extraction and length calculations. It serves as a bridge between PostgreSQL's text type system and the core JSONB conversion functionality, delegating the actual parsing work to  while properly extracting the text content and length from the PostgreSQL text structure.

## Parameters / Member Variables
-  (text*): PostgreSQL text value containing the JSON string to be parsed and converted
-  (bool): Flag indicating whether to enforce unique keys within JSON objects during parsing

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs JSON parsing and JSONB conversion
  - : Macro to extract the actual data from a PostgreSQL variable-length type
  - : Macro to get the size of data excluding the header from a PostgreSQL variable-length type
- Called from (representative examples):
  - : Expression evaluation for JSON constructor operations
  - : Function for categorizing JSON types (referenced in header file)

## Notes and Other Information
- This function is primarily used internally within PostgreSQL for text-to-JSONB conversions
- The  parameter allows for strict JSON validation when set to true
- Uses NULL for the memory context parameter in , indicating default memory management
- More efficient than converting text to C string first, as it works directly with PostgreSQL's text format
- Located in 
- Essential for internal conversions between PostgreSQL's text and JSONB types
- The function handles PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) through the VARDATA_ANY and VARSIZE_ANY_EXHDR macros