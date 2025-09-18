# check_random_seed

## Location
src/backend/commands/variable.c: 648 - 659

## Overview
This function validates and prepares random seed value changes, ensuring that only interactive SET SEED commands can affect the random number generator state.

## Definition


## Detailed Description
 is a GUC check hook function that validates attempts to set the random seed via  commands. The function implements special handling for the random seed to prevent undesirable behavior such as configuration file reloads affecting the random sequence or transaction rollbacks attempting to re-execute the seed operation.

The function allocates extra storage to track whether the seed assignment should be performed, based on the source of the configuration change. Only interactive  commands (those with source >= ) are allowed to actually modify the random sequence. This prevents configuration file reloads, server restarts, and other non-interactive sources from unexpectedly changing the random seed.

The extra storage mechanism ensures that transaction rollbacks don't attempt to re-execute the random seed assignment, maintaining the integrity of the random sequence.

## Parameters / Member Variables
- : Pointer to the new double value for the random seed
- : Pointer to extra data storage, used to store a flag indicating whether the assignment should proceed
- : The source of the configuration change (GucSource enum)

## Dependencies
- Functions called/Symbols referenced:
  - guc_malloc
  - PGC_S_INTERACTIVE (constant)
  - GucSource (enum type)
- Called from (representative examples):
  - GUC system via function pointer in guc_hooks.h

## Notes and Other Information
- This is a GUC check hook function for the  configuration parameter
- Uses memory allocation via  to store control information in the  parameter
- The function always returns  for validation but uses the  storage to control actual assignment
- Prevents configuration file reloads and non-interactive sources from affecting the random sequence
- The check function works in conjunction with an assign hook that uses the  data to determine whether to actually set the seed
- Memory allocation failure is handled by returning , preventing the seed change