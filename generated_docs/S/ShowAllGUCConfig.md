# ShowAllGUCConfig

## Location
[src/backend/utils/misc/guc_funcs.c:456-541](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L456-L541)

## Overview
ShowAllGUCConfig implements the "SHOW ALL" command by retrieving all visible GUC parameters, formatting them into a three-column result set (name, setting, description), and sending the results to the specified destination receiver.

## Definition
static void ShowAllGUCConfig(DestReceiver *dest)

## Detailed Description
This static function handles the comprehensive display of all GUC configuration parameters. The process involves several steps:

1. **Parameter Collection**: Uses get_guc_variables() to retrieve a sorted array of all GUC variables from the global hash table
2. **Schema Setup**: Creates a three-column tuple descriptor with columns:
   - "name": The parameter name (TEXT)
   - "setting": The current value (TEXT)  
   - "description": The parameter description (TEXT)
3. **Output Initialization**: Sets up tuple output state for sending results to the destination
4. **Filtering and Processing**: For each GUC variable:
   - Skips parameters marked with GUC_NO_SHOW_ALL flag
   - Filters out parameters not visible to current user via ConfigOptionIsVisible()
   - Converts parameter name to text datum
   - Calls ShowGUCOption() to get formatted value string
   - Uses parameter short description if available
5. **Result Transmission**: Sends each parameter as a three-column tuple using do_tup_output()
6. **Memory Management**: Properly frees allocated strings and text datums after each row
7. **Cleanup**: Calls end_tup_output() to finalize the result set

The function handles NULL values appropriately by setting isnull flags when settings or descriptions are unavailable.

## Parameters / Member Variables
- dest: The destination receiver where the result tuples should be sent

## Dependencies
- Functions called/Symbols referenced:
  - [get_guc_variables](../g/get_guc_variables.md) (to retrieve sorted array of all GUC variables)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (to create the three-column result schema)
  - [TupleDescInitBuiltinEntry](../T/TupleDescInitBuiltinEntry.md) (to initialize column definitions efficiently)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md) (to set up tuple output infrastructure)
  - [ConfigOptionIsVisible](../C/ConfigOptionIsVisible.md) (to check parameter visibility permissions)
  - [ShowGUCOption](ShowGUCOption.md) (to format parameter values with proper units)
  - [cstring_to_text](../c/cstring_to_text.md) (to convert C strings to PostgreSQL text datums)
  - [do_tup_output](../d/do_tup_output.md) (to send individual result tuples)
  - [end_tup_output](../e/end_tup_output.md) (to clean up output state)
  - GUC_NO_SHOW_ALL (flag constant to identify hidden parameters)
- Called from (representative examples):
  - [GetPGVariable](../G/GetPGVariable.md) (when name equals "all")

## Notes and Other Information
- This is a static function, only accessible within guc_funcs.c
- Parameters are displayed in alphabetical order thanks to get_guc_variables() sorting
- Respects both GUC_NO_SHOW_ALL flags and user permission checks for security
- Handles all GUC parameter types (bool, int, real, string, enum) through ShowGUCOption()
- Manages memory carefully to prevent leaks during the potentially large result set iteration
- The three-column format matches what GetPGVariableResultDesc() creates for "SHOW ALL"
- Located in src/backend/utils/misc/guc_funcs.c:456-541

## Simplified Source

```c
// Simplified version of ShowAllGUCConfig
static void
ShowAllGUCConfig(DestReceiver *dest)
{
    struct config_generic **guc_vars;
    int num_vars;
    TupOutputState *tstate;
    TupleDesc tupdesc;
    Datum values[3];
    bool isnull[3] = {false, false, false};

    // Get all GUC variables in sorted order
    guc_vars = get_guc_variables(&num_vars);

    // Create tuple descriptor for three TEXT columns: name, setting, description
    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitBuiltinEntry(tupdesc, 1, "name", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, "setting", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 3, "description", TEXTOID, -1, 0);

    // Initialize output state
    tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

    // Process each GUC variable
    for (int i = 0; i < num_vars; i++)
    {
        struct config_generic *conf = guc_vars[i];
        char *setting;

        // Skip hidden or non-visible parameters
        if (conf->flags & GUC_NO_SHOW_ALL)
            continue;
        if (!ConfigOptionIsVisible(conf))
            continue;

        // Build result row
        values[0] = PointerGetDatum(cstring_to_text(conf->name));

        setting = ShowGUCOption(conf, true);
        if (setting) {
            values[1] = PointerGetDatum(cstring_to_text(setting));
            isnull[1] = false;
        } else {
            values[1] = PointerGetDatum(NULL);
            isnull[1] = true;
        }

        if (conf->short_desc) {
            values[2] = PointerGetDatum(cstring_to_text(conf->short_desc));
            isnull[2] = false;
        } else {
            values[2] = PointerGetDatum(NULL);
            isnull[2] = true;
        }

        // Send row to output
        do_tup_output(tstate, values, isnull);

        // Clean up memory for this row
        pfree(DatumGetPointer(values[0]));
        if (setting) {
            pfree(setting);
            pfree(DatumGetPointer(values[1]));
        }
        if (conf->short_desc)
            pfree(DatumGetPointer(values[2]));
    }

    end_tup_output(tstate);
}
```

Key simplifications made:
- Added comments explaining the main phases of operation
- Simplified tuple descriptor creation comments
- Made the filtering logic more explicit with clear conditions
- Preserved all memory management and error handling
- Maintained the exact output format and processing logic