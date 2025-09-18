# ri_restrict

## Location
src/backend/utils/adt/ri_triggers.c: 624 - 742

## Overview
A core internal function that implements the common logic for both RESTRICT and NO ACTION referential integrity constraints for both DELETE and UPDATE operations on referenced tables.

## Definition
```c
static Datum ri_restrict(TriggerData *trigdata, bool is_no_action)
```

## Detailed Description
This function contains the shared implementation logic for four different foreign key constraint trigger functions: ON DELETE RESTRICT, ON DELETE NO ACTION, ON UPDATE RESTRICT, and ON UPDATE NO ACTION. It performs the actual constraint checking by querying the foreign key table to determine if any rows would become orphaned by the proposed operation.

The function builds and executes a SELECT query against the foreign key table to check for existing references to the key being modified. If references are found, it raises an error to prevent the constraint violation. The function handles the subtle difference between NO ACTION and RESTRICT constraints: in NO ACTION mode, it first checks if another primary key row with the same values already exists, which would make the constraint violation moot.

The query built is of the form: `SELECT 1 FROM [ONLY] <fktable> x WHERE  = fkatt1 [AND ...] FOR KEY SHARE OF x`, using the primary key values as parameters.

## Parameters / Member Variables
- `trigdata`: Trigger execution data containing context information including the trigger definition, target relation, and old tuple values
- `is_no_action`: Boolean flag distinguishing NO ACTION behavior from RESTRICT (true for NO ACTION, false for RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - `[ri_FetchConstraintInfo](ri_FetchConstraintInfo.md)`: Retrieves constraint metadata
  - `table_open`: Opens the foreign key relation
  - `[ri_Check_Pk_Match](ri_Check_Pk_Match.md)`: Checks for matching primary key (NO ACTION only)
  - `[ri_BuildQueryKey](ri_BuildQueryKey.md)`: Builds query cache key
  - `[ri_FetchPreparedPlan](ri_FetchPreparedPlan.md)`: Retrieves cached query plan
  - `[ri_GenerateQual](ri_GenerateQual.md)`: Generates WHERE clause conditions
  - `[ri_PlanCheck](ri_PlanCheck.md)`: Prepares and caches the query plan
  - `[ri_PerformCheck](ri_PerformCheck.md)`: Executes the constraint check query
  - SPI functions: `SPI_connect`, `SPI_finish`
  - Various utility functions for name quoting and type handling

- Called from (representative examples):
  - `[RI_FKey_noaction_del](../R/RI_FKey_noaction_del.md)`: NO ACTION DELETE constraints  
  - `[RI_FKey_restrict_del](../R/RI_FKey_restrict_del.md)`: RESTRICT DELETE constraints
  - `[RI_FKey_noaction_upd](../R/RI_FKey_noaction_upd.md)`: NO ACTION UPDATE constraints
  - `[RI_FKey_restrict_upd](../R/RI_FKey_restrict_upd.md)`: RESTRICT UPDATE constraints
  - `[ri_set](ri_set.md)`: SET NULL/DEFAULT constraint handling

## Notes and Other Information
- This is a static function, not directly accessible outside ri_triggers.c
- Uses SPI (Server Programming Interface) to execute SQL queries within trigger context
- Implements query plan caching for performance optimization
- Handles partitioned tables by omitting ONLY keyword when appropriate
- Takes RowShareLock on the foreign key relation for consistency
- Part of PostgreSQL's comprehensive referential integrity system
- Located in `src/backend/utils/adt/ri_triggers.c` at lines 624-742
- The function distinguishes between NO ACTION and RESTRICT primarily for the pk_match check optimization