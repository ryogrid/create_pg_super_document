# reapply_stacked_values

## Location
src/backend/utils/misc/guc.c: 5048 - 5139

## Overview
Recursive function that reapplies stacked GUC values in the correct order when replacing a placeholder with a real custom variable definition.

## Definition
```c
static void
reapply_stacked_values(struct config_generic *variable,
                       struct config_string *pHolder,
                       GucStack *stack,
                       const char *curvalue,
                       GucContext curscontext, 
                       GucSource cursource,
                       Oid cursrole)
```

## Detailed Description
This recursive function is used during custom variable definition to restore the proper stack of values that were stored in a placeholder. When a GUC variable is referenced (SET) before being defined, a placeholder is created to hold the values. Once the real variable is defined, this function recursively walks through the stack from bottom to top, reapplying each value with the appropriate GUC action (SAVE, SET, LOCAL, SET_LOCAL). The recursion ensures values are applied in the same order as they were originally stacked, preserving the semantic meaning of nested contexts and transactions.

## Parameters / Member Variables
- `variable`: The newly defined config_generic structure that will receive the stacked values
- `pHolder`: The placeholder config_string structure containing the stored values
- `stack`: Current stack entry being processed (NULL when at the end)
- `curvalue`: The string value to be applied at this stack level
- `curscontext`: The GucContext for this value
- `cursource`: The GucSource for this value  
- `cursrole`: The role OID that set this value

## Dependencies
- Functions called/Symbols referenced:
  - set_config_option_ext (multiple calls with different actions)
  - slist_delete
  - reapply_stacked_values (recursive self-call)
  - GUC_ACTION_SAVE, GUC_ACTION_SET, GUC_ACTION_LOCAL (action constants)
  - GUC_SAVE, GUC_SET, GUC_LOCAL, GUC_SET_LOCAL (state constants)
  - PGC_S_SESSION (context constant)
- Called from (representative examples):
  - define_custom_variable
  - reapply_stacked_values (recursive)

## Notes and Other Information
- This is a static function internal to guc.c and not exposed publicly
- Uses recursion to process stack entries from bottom to top, ensuring proper order
- Handles all GUC stack states: SAVE, SET, LOCAL, and SET_LOCAL
- The GUC_SET_LOCAL case is complex, requiring two separate set_config_option_ext calls
- Uses WARNING level errors to avoid breaking module loading if values are invalid
- Adjusts nest levels of successfully created stack entries to match the original
- At the stack bottom, handles session values that differ from reset values
- May leak some stack entries in edge cases, but this is acceptable given the rarity of the scenario
- Maintains proper transaction and session semantics during value migration