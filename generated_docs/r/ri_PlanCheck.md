# ri_PlanCheck

## Location
[src/backend/utils/adt/ri_triggers.c:2269-2311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2269-L2311)

## Overview
Prepares and caches execution plans for referential integrity constraint checking queries with proper security context switching.

## Definition

```c
static SPIPlanPtr
ri_PlanCheck(const char *querystr, int nargs, Oid *argtypes,
			 RI_QueryKey *qkey, Relation fk_rel, Relation pk_rel)
```
## Detailed Description
This function creates and caches prepared SQL execution plans for referential integrity operations. It handles security context management by:

1. **Query Target Determination**: Uses the query key to determine whether the query runs against the primary key (PK) or foreign key (FK) table
2. **Security Context Switch**: Temporarily switches to the table owner's user ID to ensure proper permissions
3. **Plan Preparation**: Uses SPI_prepare to create the execution plan with specified parameters
4. **Plan Caching**: Saves the plan using SPI_keepplan and caches it via ri_HashPreparedPlan
5. **Security Restoration**: Restores the original user ID and security context

The function ensures that RI checks are performed with appropriate privileges while maintaining security boundaries.

## Parameters / Member Variables
- `*querystr`: SQL query string to prepare
- `nargs`: Number of arguments the query expects
- `*argtypes`: Array of argument type OIDs
- `*qkey`: Query key for caching and identification
- `fk_rel`: Foreign key table relation
- `pk_rel`: Primary key table relation
## Dependencies
- Functions called/Symbols referenced:
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - RelationGetForm
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
  - [SPI_keepplan](../S/SPI_keepplan.md)
  - [ri_HashPreparedPlan](ri_HashPreparedPlan.md)
  - elog
- Called from (representative examples):
  - [ri_Check_Pk_Match](ri_Check_Pk_Match.md)
  - [ri_restrict](ri_restrict.md)
  - [RI_FKey_cascade_del](../R/RI_FKey_cascade_del.md)
  - [RI_FKey_cascade_upd](../R/RI_FKey_cascade_upd.md)
  - [ri_set](ri_set.md)

## Notes and Other Information
- Implements security model where RI checks run with table owner privileges
- Uses PostgreSQL's Server Programming Interface (SPI) for query preparation
- Applies security flags SECURITY_LOCAL_USERID_CHANGE and SECURITY_NOFORCE_RLS
- [Query](../Q/Query.md) type determination based on RI_PLAN_LAST_ON_PK threshold separates PK vs FK operations
- [Plan](../P/Plan.md) caching mechanism improves performance by avoiding repeated preparation of identical queries
- Essential component of PostgreSQL's referential integrity enforcement system
- Located in src/backend/utils/adt/ri_triggers.c:2269-2311