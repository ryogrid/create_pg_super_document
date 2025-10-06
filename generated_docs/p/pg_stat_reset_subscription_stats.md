# pg_stat_reset_subscription_stats

## Location
[src/backend/utils/adt/pgstatfuncs.c:1806-1829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1806-L1829)

## Overview
This function resets subscription statistics in PostgreSQL, allowing for either clearing statistics for a specific subscription or all subscription statistics at once.

## Definition

```c
Datum
pg_stat_reset_subscription_stats(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum
pg_stat_reset_subscription_stats(PG_FUNCTION_ARGS)
{
    Oid subscription_id;

    if (PG_ARGISNULL(0))
    {
        // Clear all subscription statistics when no argument provided
        pgstat_reset_of_kind(PGSTAT_KIND_SUBSCRIPTION);
    }
    else
    {
        subscription_id = PG_GETARG_OID(0);

        // Validate the subscription OID
        if (!OidIsValid(subscription_id))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid subscription OID %u", subscription_id)));

        // Reset statistics for the specific subscription
        pgstat_reset(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subscription_id);
    }

    PG_RETURN_VOID();
}
```