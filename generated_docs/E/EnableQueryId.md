# EnableQueryId

## Location
[src/backend/nodes/queryjumblefuncs.c:150-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/queryjumblefuncs.c#L150-L160)

## Overview
EnableQueryId is a function that allows third-party plugins to request that PostgreSQL enable query identifier computation.

## Definition


## Detailed Description
EnableQueryId provides a programmatic interface for third-party plugins to signal that they require query identifier computation to be enabled. The function checks the current compute_query_id configuration setting and only enables query ID generation if it's not explicitly set to OFF. This design allows plugins to request query ID computation while still respecting administrator configuration preferences.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - COMPUTE_QUERY_ID_OFF (constant for checking if query ID computation is disabled)
  - compute_query_id (global configuration variable)
  - query_id_enabled (global flag that gets set to enable query ID computation)
- Called from (representative examples):
  - COMPUTE_QUERY_ID_REGRESS (testing macro in queryjumble.h)

## Notes and Other Information
- Designed specifically for third-party plugin integration
- Respects the compute_query_id configuration setting - will not enable if set to OFF
- Sets the internal query_id_enabled flag to true when conditions are met
- Part of PostgreSQL's extensibility framework for query monitoring and analysis
- Used in conjunction with other query jumbling functions like JumbleQuery
- Allows plugins like pg_stat_statements to request query ID generation without bypassing administrative controls