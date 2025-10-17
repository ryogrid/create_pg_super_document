# textoverlay_no_len

## Location
[src/backend/utils/adt/varlena.c:1104-1115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1104-L1115)

## Overview
A PostgreSQL function that implements the SQL OVERLAY() operation without an explicit length parameter, automatically using the length of the replacement text.

## Definition

```c
Datum
textoverlay_no_len(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a variant of the SQL OVERLAY() operation where the length of the substring to replace is not explicitly specified. Instead, it automatically calculates the length by measuring the replacement text using the  function. This creates a more convenient interface for cases where you want to replace a substring with text of a different length, automatically adjusting the replacement length to match the new text.

The function extracts the original text, replacement text, and start position from the PostgreSQL function arguments, then determines the replacement length by calling  on the replacement text. Finally, it delegates to the internal  function to perform the actual overlay operation.

## Parameters / Member Variables
-  (t1): The original text string to be modified
-  (t2): The replacement text to insert
-  (sp): The substring start position (1-based)
- : Automatically calculated length based on replacement text length

## Dependencies
- Functions called/Symbols referenced:
  - [text_length](text_length.md)
  - [text_overlay](text_overlay.md)
  - PG_RETURN_TEXT_P
  - PG_GETARG_TEXT_PP
  - PG_GETARG_INT32
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL OVERLAY function variant)

## Notes and Other Information
- Provides a convenient variant of OVERLAY() that auto-sizes the replacement length
- Uses PostgreSQL's standard function calling conventions with PG_FUNCTION_ARGS
- The replacement length is determined by calling  on the replacement text
- Part of PostgreSQL's variable-length character data handling utilities
- Located in src/backend/utils/adt/varlena.c with other text manipulation functions
- Complements the standard  function by providing automatic length calculation

## Simplified Source
```c
Datum textoverlay_no_len(PG_FUNCTION_ARGS)
{
    // Extract arguments: original text, replacement text, start position
    text *original_text = PG_GETARG_TEXT_PP(0);
    text *replacement_text = PG_GETARG_TEXT_PP(1);
    int start_position = PG_GETARG_INT32(2);

    // Auto-calculate replacement length from replacement text
    int replacement_length = text_length(PointerGetDatum(replacement_text));

    // Delegate to text_overlay with calculated length
    return PG_RETURN_TEXT_P(text_overlay(original_text, replacement_text,
                                        start_position, replacement_length));
}
```