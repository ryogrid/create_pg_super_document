# pg_stat_reset_subscription_stats

## Location
src/backend/utils/adt/pgstatfuncs.c: 1806 - 1829

## Overview
This function resets subscription statistics in PostgreSQL, allowing for either clearing statistics for a specific subscription or all subscription statistics at once.

## Definition


## Detailed Description
The  function provides a mechanism to reset statistical data collected for logical replication subscriptions. It operates in two modes:

1. **Reset all subscription stats**: When called with a NULL argument, it clears statistics for all subscriptions by calling 
2. **Reset specific subscription stats**: When provided with a valid subscription OID, it resets statistics only for that particular subscription using 

The function includes validation to ensure that when a specific subscription OID is provided, it is a valid OID value. If an invalid OID is passed, it raises an error with code .

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - When argument 0 is NULL: Resets all subscription statistics  
  - When argument 0 is a valid OID: Resets statistics for the specified subscription only

## Dependencies
- Functions called/Symbols referenced:
  -  - Resets statistics for all objects of a specific kind
  -  - Resets statistics for a specific object
  -  - Constant identifying subscription statistics type
  -  - PostgreSQL macro for returning void from SQL functions

- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- This function is exposed as a SQL function and can be called from SQL queries to reset subscription statistics
- Part of PostgreSQL's statistics collection and management system for logical replication
- Located in 
- Provides administrative functionality for monitoring and maintaining subscription performance metrics
- The function validates input parameters and provides appropriate error messages for invalid subscription OIDs