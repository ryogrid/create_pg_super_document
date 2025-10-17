# anychar_typmodin

## Location
[src/backend/utils/adt/varchar.c:33-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L33-L71)

## Overview
A static utility function that processes type modifier input for character types (both BPCHAR and VARCHAR), validating the length parameter and converting it to the internal typmod format.

## Definition

```c
static int32
anychar_typmodin(ArrayType *ta, const char *typename)
```
## Detailed Description
This function serves as common code for both bpchartypmodin and varchartypmodin functions. It processes the type modifier array provided during type declaration (e.g., CHAR(10) or VARCHAR(255)) and performs validation on the length parameter. The function extracts the length value from the input array, validates it against PostgreSQL's constraints, and converts it to the internal typmod representation by adding VARHDRSZ (variable header size) to the user-specified length. This design maintains backward compatibility with existing client-side code that expects this specific typmod encoding.

## Parameters / Member Variables
- `ta`: ArrayType pointer containing the type modifier values from the SQL type declaration
- `typename`: String name of the type being processed (used for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md)
  - MaxAttrSize
  - ereport (for error reporting)
  - VARHDRSZ (variable header size constant)
- Called from (representative examples):
  - [bpchartypmodin](../b/bpchartypmodin.md)
  - [varchartypmodin](../v/varchartypmodin.md)

## Notes and Other Information
- The function enforces that exactly one type modifier must be provided
- Length must be at least 1 and cannot exceed MaxAttrSize
- The typmod encoding adds VARHDRSZ to the user-specified length for historical compatibility reasons
- Error messages use the provided typename parameter to give context-specific feedback
- This is a static function, meaning it's only accessible within the varchar.c source file

## Simplified Source

```c
static int32 anychar_typmodin(ArrayType *ta, const char *typename) {
    int32 *length_array;
    int num_modifiers;

    // Extract integer type modifiers from input array
    length_array = ArrayGetIntegerTypmods(ta, &num_modifiers);

    // Validate exactly one modifier provided
    if (num_modifiers != 1) {
        ereport(ERROR, "invalid type modifier");
    }

    // Validate length is within acceptable range
    int32 specified_length = *length_array;
    if (specified_length < 1) {
        ereport(ERROR, "length must be at least 1");
    }
    if (specified_length > MaxAttrSize) {
        ereport(ERROR, "length cannot exceed maximum");
    }

    // Convert to internal typmod format (length + header size)
    return VARHDRSZ + specified_length;
}
```