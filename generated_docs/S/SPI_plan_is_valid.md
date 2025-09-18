# SPI_plan_is_valid

## Location
src/backend/executor/spi.c: 1948 - 1971

## Overview
Tests whether a SPI plan is currently valid and not marked as being in need of revalidation.

## Definition


## Detailed Description
SPI_plan_is_valid checks the validity of a Server Programming Interface (SPI) plan by iterating through all cached plan sources associated with the plan and verifying each one using CachedPlanIsValid. The function ensures that the plan is not marked for revalidation, which can happen when underlying database objects (tables, functions, etc.) are modified. This function is essential for determining whether a previously prepared SPI plan can still be executed without recompilation.

The function performs a magic number check to ensure the plan structure integrity before proceeding with validation checks. It returns false immediately if any of the cached plan sources in the plan are invalid, ensuring that the entire plan is considered invalid if any component requires revalidation.

## Parameters / Member Variables
- `plan`: A pointer to the SPI plan (SPIPlanPtr) to be validated. Must be a valid SPI plan with the correct magic number.

## Dependencies
- Functions called/Symbols referenced:
  - [CachedPlanIsValid](../C/CachedPlanIsValid.md)
  - _SPI_PLAN_MAGIC (for integrity check)
  - [SPIPlanPtr](SPIPlanPtr.md) (plan structure type)
  - CachedPlanSource (individual plan source type)
- Called from (representative examples):
  - [ri_FetchPreparedPlan](../r/ri_FetchPreparedPlan.md)

## Notes and Other Information
- The function includes an Assert to verify the plan's magic number, ensuring the plan structure is valid
- This function should be used in conjunction with understanding of CachedPlanIsValid behavior
- Returns true only if ALL cached plan sources in the plan are valid
- Used primarily in referential integrity triggers and other prepared statement scenarios
- Part of the SPI (Server Programming Interface) which allows C functions to execute SQL commands