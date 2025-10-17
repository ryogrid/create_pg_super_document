# name_bpchar

## Location
[src/backend/utils/adt/varchar.c:407-416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L407-L416)

## Overview
Converts a NameData type to a bpchar (blank-padded character) type by leveraging text conversion functions.

## Definition

```c
Datum
name_bpchar(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs type conversion from PostgreSQL's internal  type to the  type. It uses the text conversion infrastructure to accomplish this conversion, which is appropriate given that BpChar and text types are equivalent in their internal representation. The function extracts the string representation from the NameData input and converts it to a BpChar result using the existing  utility function.

## Parameters / Member Variables
- Takes input through  macro which provides:
  - : A  (NameData) type representing the source name to be converted

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts Name argument from function call context
  - : Converts C string to text/bpchar representation
  - : Returns BpChar pointer as Datum
  - : Macro to extract C string from NameData
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in 
- This function assumes that BpChar and text types are equivalent, which is a key design principle in PostgreSQL's type system
- The conversion is straightforward as it relies on the existing text handling infrastructure
- Part of PostgreSQL's type casting system for name-related operations

## Simplified Source

```c
Datum name_bpchar(PG_FUNCTION_ARGS) {
    Name s = PG_GETARG_NAME(0);

    // Convert name to bpchar using text conversion infrastructure
    BpChar *result = (BpChar *) cstring_to_text(NameStr(*s));

    PG_RETURN_BPCHAR_P(result);
}
```