# ri_PlanCheck

## Location
src/backend/utils/adt/ri_triggers.c: 2269 - 2311

## Overview
Prepares and caches execution plans for referential integrity constraint checking queries with proper security context switching.

## Definition


## Detailed Description
This function creates and caches prepared SQL execution plans for referential integrity operations. It handles security context management by:

1. **Query Target Determination**: Uses the query key to determine whether the query runs against the primary key (PK) or foreign key (FK) table
2. **Security Context Switch**: Temporarily switches to the table owner's user ID to ensure proper permissions
3. **Plan Preparation**: Uses SPI_prepare to create the execution plan with specified parameters
4. **Plan Caching**: Saves the plan using SPI_keepplan and caches it via ri_HashPreparedPlan
5. **Security Restoration**: Restores the original user ID and security context

The function ensures that RI checks are performed with appropriate privileges while maintaining security boundaries.

## Parameters / Member Variables
- : SQL query string to prepare
- : Number of arguments the query expects
- : Array of argument type OIDs
- : Query key for caching and identification
- : Foreign key table relation
- : Primary key table relation

## Dependencies
- Functions called/Symbols referenced:
  - GetUserIdAndSecContext
  - SetUserIdAndSecContext
  - RelationGetForm
  - SPI_prepare
  - SPI_result_code_string
  - SPI_keepplan
  - ri_HashPreparedPlan
  - elog
- Called from (representative examples):
  - ri_Check_Pk_Match
  - ri_restrict
  - RI_FKey_cascade_del
  - RI_FKey_cascade_upd
  - ri_set

## Notes and Other Information
- Implements security model where RI checks run with table owner privileges
- Uses PostgreSQL's Server Programming Interface (SPI) for query preparation
- Applies security flags SECURITY_LOCAL_USERID_CHANGE and SECURITY_NOFORCE_RLS
- Query type determination based on RI_PLAN_LAST_ON_PK threshold separates PK vs FK operations
- Plan caching mechanism improves performance by avoiding repeated preparation of identical queries
- Essential component of PostgreSQL's referential integrity enforcement system
- Located in src/backend/utils/adt/ri_triggers.c:2269-2311