# ri_Check_Pk_Match

## Location
src/backend/utils/adt/ri_triggers.c: 461 - 550

## Overview
Internal static function that checks if another primary key row exists with the same key values as a modified or deleted tuple, used to determine if foreign key constraint violations would occur.

## Definition


## Detailed Description
This function performs a critical check in PostgreSQL's referential integrity system by searching the primary key table to determine if there's another row that matches the key values from a tuple that's being modified or deleted. This check is essential for NO ACTION and RESTRICT foreign key constraints to determine whether the operation should be allowed or blocked.

The function dynamically builds and executes a SELECT query against the primary key table using the values from the old tuple. It uses prepared statements for performance optimization and employs proper locking (FOR KEY SHARE) to ensure consistency. The function assumes the caller has already verified that the old tuple contains no NULL key values, as a match would be impossible with NULLs.

Key aspects of the implementation:
- Builds SQL query: "SELECT 1 FROM [ONLY] <pktable> x WHERE pkatt1 =  [AND ...] FOR KEY SHARE OF x"
- Uses SPI (Server Programming Interface) for query execution
- Supports both regular tables and partitioned tables
- Uses prepared statement caching for performance

## Parameters / Member Variables
- : Relation pointer to the primary key table being checked
- : Relation pointer to the foreign key table (used for plan caching)
- : TupleTableSlot containing the tuple values to match against
- : RI_ConstraintInfo structure containing constraint metadata including key column mappings

## Dependencies
- Functions called/Symbols referenced:
  - ri_NullCheck
  - SPI_connect/SPI_finish
  - ri_BuildQueryKey
  - ri_FetchPreparedPlan
  - ri_PlanCheck
  - ri_PerformCheck
  - quoteRelationName
  - quoteOneName
  - RIAttType
  - RIAttName
  - ri_GenerateQual
  - RI_PLAN_CHECK_LOOKUPPK_FROM_PK (constant)
  - Various SPI constants and types
- Called from (representative examples):
  - ri_restrict (src/backend/utils/adt/ri_triggers.c:653)

## Notes and Other Information
- This is a static (internal) function, not exposed outside ri_triggers.c
- Function assumes input tuple has no NULL values in key columns (verified by Assert)
- Uses FOR KEY SHARE locking to prevent concurrent modifications during the check
- Returns boolean: true if a matching primary key row is found, false otherwise
- Critical component in implementing NO ACTION and RESTRICT foreign key constraints
- Located in src/backend/utils/adt/ri_triggers.c:461-550
- Handles both regular and partitioned primary key tables appropriately
- Part of PostgreSQL's comprehensive referential integrity enforcement system