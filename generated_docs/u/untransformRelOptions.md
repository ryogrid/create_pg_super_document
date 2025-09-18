# untransformRelOptions

## Location
[src/backend/access/common/reloptions.c:1340-1387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1340-L1387)

## Overview
Converts text-array format reloptions back into a List of DefElem nodes, serving as the inverse operation of transformRelOptions().

## Definition


## Detailed Description
This function performs the reverse transformation of , taking the internal text-array representation of relation options and converting them back into a list of DefElem nodes that can be processed by other parts of the system. It parses each 'name=value' formatted string in the array, splitting on the '=' character to separate option names from their values. Options without values (bare names) are handled as having NULL values. This function is commonly used when PostgreSQL needs to examine or manipulate existing relation options.

## Parameters / Member Variables
- : Datum containing text array of reloptions in 'name=value' format (may be NULL/invalid)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - DatumGetArrayTypeP
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - [makeString](../m/makeString.md)
  - makeDefElem
- Called from (representative examples):
  - [transformGenericOptions](../t/transformGenericOptions.md) (foreign data wrapper handling)
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md) (ALTER TABLE operations)
  - [GetForeignDataWrapperExtended](../G/GetForeignDataWrapperExtended.md) (foreign wrapper introspection)
  - [pg_options_to_table](../p/pg_options_to_table.md) (option display functions)

## Notes and Other Information
- Returns NIL (empty list) if input options is NULL or invalid
- Handles both 'name=value' and bare 'name' formats (bare names get NULL values)
- Each parsed option becomes a DefElem with location set to -1
- Used extensively in foreign data wrapper code and relation option introspection
- The parsing splits strings on first '=' character, so values can contain '=' if needed
- Function is defined in src/backend/access/common/reloptions.c:1340-1387