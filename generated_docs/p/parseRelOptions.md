# parseRelOptions

## Location
[src/backend/access/common/reloptions.c:1508-1549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1508-L1549)

## Overview
Static function that interprets relation options from a text array format, building a structured array of relopt_value elements for a specific relation option kind.

## Definition
```c
static relopt_value *parseRelOptions(Datum options, bool validate, relopt_kind kind, int *numrelopts)
```

## Detailed Description
This function serves as the main entry point for parsing relation options from their text-array representation as constructed by transformRelOptions(). It first determines which relation options are applicable for the specified kind (table, index, etc.), allocates an appropriate relopt_value array, and initializes each option with its definition and default state. If options are provided, it delegates the actual parsing to parseRelOptionsInternal(). The function returns a complete array containing both set and unset options, allowing callers to easily access default values for unspecified options.

## Parameters / Member Variables
- `options`: Datum containing relation options in text-array format
- `validate`: Boolean flag to enable validation and error reporting for invalid options
- `kind`: relopt_kind specifying the family of options to process (e.g., table, index)
- `numrelopts`: Output parameter returning the number of elements in the returned array

## Dependencies
- Functions called/Symbols referenced:
  - [initialize_reloptions](../i/initialize_reloptions.md)
  - [palloc](palloc.md)
  - PointerIsValid
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [parseRelOptionsInternal](parseRelOptionsInternal.md)
- Called from (representative examples):
  - [build_reloptions](../b/build_reloptions.md)

## Notes and Other Information
- Returns NULL and sets numrelopts to 0 if no options of the given kind exist
- The returned array includes both set (isset=true) and unset (isset=false) options
- Memory management note: string values are allocated separately and must be freed by caller
- Performs lazy initialization of the reloptions system via initialize_reloptions()
- Uses global relOpts array to determine available options for each kind