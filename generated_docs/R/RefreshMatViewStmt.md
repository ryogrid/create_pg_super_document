# RefreshMatViewStmt

## Location
[src/include/nodes/parsenodes.h:3902-3908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3902-L3908)

## Overview
RefreshMatViewStmt represents the parsed structure of a REFRESH MATERIALIZED VIEW statement, which is used to update the contents of a materialized view with fresh data from its underlying query.

## Definition

```c
typedef struct RefreshMatViewStmt
{
	NodeTag		type;
	bool		concurrent;		/* allow concurrent access? */
	bool		skipData;		/* true for WITH NO DATA */
	RangeVar   *relation;		/* relation to insert into */
} RefreshMatViewStmt;
```
## Detailed Description
RefreshMatViewStmt encapsulates the REFRESH MATERIALIZED VIEW command, which rebuilds the stored data in a materialized view. The statement provides two important modes of operation: concurrent refresh (CONCURRENTLY) which allows read access during the refresh process, and the option to clear the view's data (WITH NO DATA) instead of refreshing it. The relation field identifies the specific materialized view to be refreshed.

## Parameters / Member Variables
- : NodeTag identifying this as a RefreshMatViewStmt node
- : Boolean flag indicating if CONCURRENTLY option was specified, allowing read access during refresh
- : Boolean flag indicating if WITH NO DATA was specified, which clears the view rather than refreshing it
- : RangeVar pointer identifying the materialized view to be refreshed

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (for materialized view identification)
- Called from (representative examples):
  - ExecRefreshMatView
  - ProcessUtilitySlow

## Notes and Other Information
- Part of PostgreSQL's materialized view system introduced to provide cached query results
- The concurrent option (CONCURRENTLY) requires a unique index on the materialized view and uses a more complex refresh algorithm
- WITH NO DATA option effectively truncates the materialized view, making it unreadable until the next refresh
- Concurrent refresh provides better availability but has stricter requirements and potentially higher overhead
- The refresh process involves re-executing the materialized view's defining query and replacing the stored data