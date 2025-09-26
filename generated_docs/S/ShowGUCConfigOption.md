# ShowGUCConfigOption

## Location
src/backend/utils/misc/guc_funcs.c: 428 - 455

## Overview
ShowGUCConfigOption displays the value of a single GUC (Grand Unified Configuration) parameter by creating a result tuple and sending it to the specified destination receiver.

## Definition
static void ShowGUCConfigOption(const char *name, DestReceiver *dest)

## Detailed Description
This static function handles the display of individual GUC configuration parameters. It performs the following operations:

1. **Value Retrieval**: Calls GetConfigOptionByName() to get both the current value and canonical name of the requested parameter
2. **Schema Creation**: Creates a single-column tuple descriptor using the canonical parameter name as the column name
3. **Output Setup**: Initializes a tuple output state using begin_tup_output_tupdesc() for sending results to the destination
4. **Data Transmission**: Uses do_text_output_oneline() macro to convert the value to a text datum and send it as a single-row result
5. **Cleanup**: Calls end_tup_output() to clean up the output state

The function uses TupleDescInitBuiltinEntry() instead of TupleDescInitEntry() for efficiency, as it can initialize TEXT columns without catalog access.

## Parameters / Member Variables
- name: The name of the GUC parameter to display (will be resolved to canonical form)
- dest: The destination receiver where the result tuple should be sent

## Dependencies
- Functions called/Symbols referenced:
  - GetConfigOptionByName (to retrieve parameter value and canonical name)
  - CreateTemplateTupleDesc (to create the result tuple descriptor)
  - TupleDescInitBuiltinEntry (to initialize the column definition efficiently)
  - begin_tup_output_tupdesc (to set up tuple output infrastructure)
  - do_text_output_oneline (macro to send a single text value as a tuple)
  - end_tup_output (to clean up output state)
  - DestReceiver (type for output destination)
  - TupOutputState (type for output state management)
- Called from (representative examples):
  - GetPGVariable (when showing a specific parameter)

## Notes and Other Information
- This is a static function, only accessible within guc_funcs.c
- The function automatically handles parameter name canonicalization and access permission checking through GetConfigOptionByName()
- Uses the efficient builtin entry initialization to avoid catalog lookups for the TEXT type
- The do_text_output_oneline macro handles the conversion from C string to PostgreSQL text datum
- Complements ShowAllGUCConfig() for the single-parameter case
- Located in src/backend/utils/misc/guc_funcs.c:428-455