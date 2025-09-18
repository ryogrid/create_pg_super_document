# pgstat_reset_entries_of_kind

## Location
src/backend/utils/activity/pgstat_shmem.c: 1065 - 1070

## Overview
This function resets all statistics entries in the shared hashtable that match a specific statistics kind.

## Definition


## Detailed Description
The function provides a convenient interface for resetting all statistics entries of a particular type or kind. It acts as a wrapper around , using the  callback function to identify and reset only the statistics entries whose kind matches the specified target kind.

The function converts the kind parameter to a  and passes it along with the  predicate function to the more general  function, which handles the actual scanning and resetting logic.

## Parameters / Member Variables
- : The specific statistics kind () to match and reset. Only entries with this exact kind will be reset.
- : TimestampTz value used as the timestamp for the reset operation, typically the current time.

## Dependencies
- Functions called/Symbols referenced:
  - : General function that resets matching entries based on a predicate
  - : Predicate function that checks if an entry's kind matches the target kind
  - : Converts the int32 kind value to Datum format
  - : Enumeration type representing different statistics kinds
- Called from (representative examples):
  - : Higher-level function for resetting statistics of a specific kind

## Notes and Other Information
- This function provides a more specific interface compared to the general 
- Uses the callback pattern internally by delegating to  with 
- Part of PostgreSQL's layered statistics reset infrastructure
- Efficiently filters entries by statistics kind without having to examine entry contents
- The function is synchronous and will complete the reset of all matching entries before returning