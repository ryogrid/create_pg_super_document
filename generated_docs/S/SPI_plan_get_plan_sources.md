# SPI_plan_get_plan_sources

## Location
[src/backend/executor/spi.c:2057-2075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2057-L2075)

## Overview
Returns the underlying list of CachedPlanSources from a SPI plan, providing direct access to the plan's internal cached plan structure.

## Definition
```c
List *SPI_plan_get_plan_sources(SPIPlanPtr plan)
```

## Detailed Description
SPI_plan_get_plan_sources provides direct access to the list of CachedPlanSources that comprise a SPI plan. This function is primarily designed for internal PostgreSQL use, particularly by PL/pgSQL, to access the underlying plan cache structure without requiring the procedural language to directly access the SPIPlan structure internals.

The function performs minimal validation, only checking the plan's magic number to ensure structural integrity. Importantly, it does not verify whether the returned CachedPlanSources are up-to-date or valid, leaving that responsibility to the caller. This design choice prioritizes performance over safety, making the function suitable for internal use where the caller understands the implications.

## Parameters / Member Variables
- `plan`: A pointer to the SPI plan (SPIPlanPtr) from which to extract the cached plan sources. Must be a valid SPI plan with the correct magic number.

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_PLAN_MAGIC (for integrity verification)
  - [SPIPlanPtr](SPIPlanPtr.md) (plan structure type)
  - CachedPlan (referenced in context)
- Called from (representative examples):
  - Primarily used by PL/pgSQL (not directly visible in references)

## Notes and Other Information
- CAUTION: No validation is performed on whether the CachedPlanSources are current or valid
- Exported specifically for PL/pgSQL's use to avoid direct access to SPIPlan internals
- Not documented in the main SPI documentation (spi.sgml) to discourage widespread use
- Returns the raw plancache_list from the plan structure
- Should be used only when the caller understands the plan cache lifecycle
- Part of the internal SPI interface rather than the public API