# assign_random_seed

## Location
src/backend/commands/variable.c: 660 - 668

## Overview
This function performs the actual assignment of a new random seed value to PostgreSQL's random number generator, working in conjunction with the check_random_seed function to ensure controlled execution.

## Definition


## Detailed Description
 is a GUC assign hook function that performs the actual random seed assignment when the  configuration parameter is changed. This function works as part of a two-phase system with  to ensure that random seed changes only occur under appropriate circumstances.

The function uses the  parameter (set up by ) to determine whether the seed assignment should actually be performed. This mechanism prevents unwanted seed changes from configuration file reloads, transaction rollbacks, and other non-interactive sources. Once the seed is set, the function resets the flag in  to zero, ensuring that the seed assignment happens at most once per GUC variable setting.

The actual seed setting is performed by calling the  function through PostgreSQL's function call interface.

## Parameters / Member Variables
- : The new double value for the random seed to be applied
- : Pointer to extra data containing a flag that indicates whether the assignment should proceed (set by check_random_seed)

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1 (macro/function for calling PostgreSQL functions)
  - [setseed](../s/setseed.md) (PostgreSQL function that actually sets the random seed)
  - [Float8GetDatum](../F/Float8GetDatum.md) (function to convert double to PostgreSQL Datum type)
- Called from (representative examples):
  - GUC system via function pointer in guc_hooks.h

## Notes and Other Information
- This is a GUC assign hook function for the  configuration parameter
- Works in tandem with  to implement controlled random seed assignment
- The  parameter contains an integer flag that controls whether the assignment proceeds
- Uses PostgreSQL's internal function calling mechanism to invoke the  function
- The flag reset to 0 ensures idempotency - the seed is set at most once per GUC change
- This design prevents transaction rollbacks from re-executing the seed assignment
- Returns void as assign hook functions don't return values