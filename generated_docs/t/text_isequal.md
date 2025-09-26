# text_isequal

## Location
src/backend/utils/adt/varlena.c: 4500 - 4513

## Overview
A static convenience function that provides a simple boolean interface for comparing two text values for equality using a specified collation.

## Definition

```c
struct_empty_array(TEXTOID));
```
## Detailed Description
This function serves as a lightweight wrapper around PostgreSQL's texteq function, providing a more convenient interface for internal code that needs to compare text values. It leverages PostgreSQL's built-in text equality operator while handling the function call infrastructure automatically.

The function uses DirectFunctionCall2Coll to invoke the texteq operator with the specified collation, ensuring that text comparison follows the appropriate linguistic rules for the given locale. This is particularly important for case-insensitive comparisons and proper handling of accent marks and other locale-specific character variations.

## Parameters / Member Variables
- : First text value to compare
- : Second text value to compare  
- : OID of the collation to use for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - texteq (PostgreSQL's text equality operator function)
  - DirectFunctionCall2Coll (direct function call with collation support)
  - DatumGetBool (extract boolean value from Datum)
  - PointerGetDatum (convert pointer to Datum)
- Called from (representative examples):
  - split_text_accum_result

## Notes and Other Information
- This is a static function internal to varlena.c, designed to simplify text equality testing within the module
- The function properly handles collation-aware text comparison, which is essential for internationalization
- It abstracts away the complexity of PostgreSQL's function call interface for internal use
- The use of DirectFunctionCall2Coll ensures optimal performance by bypassing some function call overhead
- Located in src/backend/utils/adt/varlena.c:4500-4513