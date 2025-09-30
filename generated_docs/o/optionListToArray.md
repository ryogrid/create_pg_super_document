# optionListToArray

## Location
[src/backend/commands/foreigncmds.c:66-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L66-L120)

## Overview
Converts a list of DefElem structures into a text array format that is used for storing options in PostgreSQL system catalogs such as pg_foreign_data_wrapper, pg_foreign_server, pg_user_mapping, and pg_foreign_table.

## Definition

```c
static Datum
optionListToArray(List *options)
```
## Detailed Description
This static function transforms a linked list of DefElem structures (representing option name-value pairs) into a PostgreSQL text array datum. Each option is formatted as "name=value" and stored as a text element in the array. The function performs validation to ensure that option names do not contain "=" characters, which would make the format ambiguous. If the input list is empty, the function returns PointerGetDatum(NULL). The resulting array is typically stored directly in database system catalogs without further processing, so validation should be performed before calling this function.

## Parameters / Member Variables
- : A List of DefElem structures containing option name-value pairs to be converted into array format

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayBuildState](../A/ArrayBuildState.md) (type used for building arrays)
  - [DefElem](../D/DefElem.md) (structure representing option definitions)  
  - [defGetString](../d/defGetString.md) (extracts string value from DefElem)
  - SET_VARSIZE (sets the size of a variable-length type)
  - VARDATA (gets pointer to data portion of variable-length type)
  - [accumArrayResult](../a/accumArrayResult.md) (accumulates elements into array result)
  - [makeArrayResult](../m/makeArrayResult.md) (finalizes and returns the array result)
- Called from (representative examples):
  - [transformGenericOptions](../t/transformGenericOptions.md) (src/backend/commands/foreigncmds.c:190)

## Notes and Other Information
- The function validates that option names do not contain "=" characters to prevent ambiguous parsing of the "name=value" format
- Memory allocation is handled through PostgreSQL's memory context system
- The array elements are stored in text format with each element being "name=value"
- This is an internal static function used exclusively within the foreign data wrapper command processing module
- The function properly handles empty option lists by returning NULL datum

## Simplified Source

```c
static Datum
optionListToArray(List *options)
{
    ArrayBuildState *astate = NULL;
    ListCell *cell;

    // Convert list of DefElem structures to text array format
    // Used for storing FDW options in system catalogs
    foreach(cell, options)
    {
        DefElem *def = lfirst(cell);
        const char *name = def->defname;
        const char *value = defGetString(def);
        Size len;
        text *t;

        // Validate option name doesn't contain "=" to avoid ambiguous parsing
        if (strchr(name, '=') != NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid option name \"%s\": must not contain \"=\"",
                            name)));

        // Create text datum in "name=value" format
        len = VARHDRSZ + strlen(name) + 1 + strlen(value);
        t = palloc(len + 1); // +1 for sprintf's trailing null
        SET_VARSIZE(t, len);
        sprintf(VARDATA(t), "%s=%s", name, value);

        // Accumulate into array result
        astate = accumArrayResult(astate, PointerGetDatum(t),
                                false, TEXTOID,
                                CurrentMemoryContext);
    }

    // Return completed array or NULL if empty
    if (astate)
        return makeArrayResult(astate, CurrentMemoryContext);

    return PointerGetDatum(NULL);
}
```