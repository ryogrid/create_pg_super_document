# JsonbToCStringWorker

## Location
src/backend/utils/adt/jsonb.c: 491 - 614

## Overview
The core static function that performs the actual conversion of JSONB containers to string representation, with optional indentation support for both compact and pretty-printed output.

## Definition


## Detailed Description
This function is the heart of JSONB to string conversion in PostgreSQL. It uses a JsonbIterator to traverse the JSONB structure and builds the string representation token by token. The function handles all JSONB types including objects, arrays, and scalar values, with support for both compact and indented output formats. It manages proper JSON syntax including commas, brackets, braces, colons, and indentation. The function includes special handling for raw scalar values (top-level scalars wrapped in arrays) and maintains proper nesting levels for indentation. It uses an iterator-based approach that allows for efficient memory usage and streaming output generation.

## Parameters / Member Variables
- : Optional StringInfo buffer for output; if NULL, a new buffer is created
- : Pointer to the JsonbContainer structure containing the JSONB data to convert
- : Estimated length for buffer pre-allocation (uses 64 as default if negative)
- : Boolean flag controlling whether to generate pretty-printed output with indentation

## Dependencies
- Functions called/Symbols referenced:
  - makeStringInfo, enlargeStringInfo (for buffer management)
  - [JsonbIteratorInit](JsonbIteratorInit.md), JsonbIteratorNext (for JSONB traversal)
  - [jsonb_put_escaped_value](../j/jsonb_put_escaped_value.md) (for scalar value conversion)
  - add_indent (for indentation formatting)
  - appendBinaryStringInfo, appendStringInfoCharMacro (for string building)
  - JsonbIterator, JsonbIteratorToken, JsonbValue (JSONB iterator types)
  - WJB_* constants (iterator token types: WJB_DONE, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT, WJB_KEY, WJB_ELEM, WJB_VALUE, WJB_END_ARRAY, WJB_END_OBJECT)
- Called from (representative examples):
  - [JsonbToCString](JsonbToCString.md) (with indent=false)
  - [JsonbToCStringIndent](JsonbToCStringIndent.md) (with indent=true)

## Notes and Other Information
- This is a static function used internally by the JSONB string conversion functions
- Uses a state machine approach with redo_switch logic to handle complex iterator sequences
- Manages multiple state variables: first (comma handling), level (nesting depth), use_indent (indentation control), raw_scalar (top-level scalar detection), last_was_key (formatting context)
- The ispaces variable controls spacing: 1 space after comma when indenting, 2 when not
- Special handling for raw scalars (top-level scalars) where array brackets are omitted
- Includes assertions to ensure proper nesting (level == 0 at end)
- Efficiently handles both object key-value pairs and array elements
- The function is designed to be memory-efficient and can work with very large JSONB structures
- Returns the data pointer from the StringInfo buffer, making the caller responsible for memory management when out was NULL