# untransformRelOptions

## Location
[src/backend/access/common/reloptions.c:1340-1387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1340-L1387)

## Overview
Converts text-array format reloptions back into a List of DefElem nodes, serving as the inverse operation of transformRelOptions().

## Definition

```c
struct_array_builtin(array, TEXTOID, &optiondatums, NULL, &noptions);
```
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
  - [makeDefElem](../m/makeDefElem.md)
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

## Simplified Source

```c
List *untransformRelOptions(Datum options) {
    List *result = NIL;

    // Return empty list if no options provided
    if (!PointerIsValid(DatumGetPointer(options)))
        return result;

    // Extract text array from datum
    ArrayType *array = DatumGetArrayTypeP(options);
    Datum *optiondatums;
    int noptions;

    // Deconstruct array to get individual option strings
    deconstruct_array_builtin(array, TEXTOID, &optiondatums, NULL, &noptions);

    // Parse each "name=value" string into DefElem
    for (int i = 0; i < noptions; i++) {
        char *option_string = TextDatumGetCString(optiondatums[i]);
        char *equals_pos = strchr(option_string, '=');
        Node *value = NULL;

        // Split on '=' to separate name from value
        if (equals_pos) {
            *equals_pos++ = '\0';  // Terminate name, advance to value
            value = (Node *) makeString(equals_pos);
        }

        // Add DefElem to result list
        result = lappend(result, makeDefElem(option_string, value, -1));
    }

    return result;
}
```