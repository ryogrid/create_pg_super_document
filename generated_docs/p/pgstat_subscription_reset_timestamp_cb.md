# pgstat_subscription_reset_timestamp_cb

## Location
[src/backend/utils/activity/pgstat_subscription.c:111-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_subscription.c#L111-L114)

## Overview
A callback function that updates the statistics reset timestamp for subscription statistics when reset operations are performed.

## Definition

```c
void
pgstat_subscription_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
```
## Detailed Description
This function serves as a callback within PostgreSQL's statistics system infrastructure, specifically for handling timestamp updates when subscription statistics are reset. It is called internally by the statistics framework when a reset operation occurs on subscription statistics. The function takes a generic shared statistics header and casts it to the subscription-specific structure to set the reset timestamp.

The callback mechanism allows the statistics system to perform type-specific operations during reset procedures while maintaining a generic interface. This function ensures that when subscription statistics are reset (typically via administrative commands), the timestamp of when the reset occurred is properly recorded for monitoring and troubleshooting purposes.

## Parameters / Member Variables
- : Generic pointer to the shared statistics structure that will be cast to subscription-specific type
- : The timestamp to set as the statistics reset time

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Common
  - [PgStatShared_Subscription](../P/PgStatShared_Subscription.md)
- Called from (representative examples):
  - SH_DECLARE (part of statistics system infrastructure)

## Notes and Other Information
This function is part of PostgreSQL's statistics callback infrastructure and is registered with the statistics system for subscription-type statistics. It is not called directly by user code but rather invoked automatically by the statistics framework during reset operations. The reset timestamp is useful for determining when statistics were last cleared, which can be important for interpreting the meaning and age of accumulated statistics data.