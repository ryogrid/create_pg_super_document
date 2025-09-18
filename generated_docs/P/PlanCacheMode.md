# PlanCacheMode

## Location
src/include/utils/plancache.h: 35 - 39

## Overview
PlanCacheMode is an enumeration that defines the available modes for controlling PostgreSQL's plan cache behavior, specifically determining when to use custom or generic plans for prepared statements.

## Definition


## Detailed Description
PlanCacheMode is used in conjunction with the  GUC (Grand Unified Configuration) parameter to control how PostgreSQL chooses between custom and generic plans for prepared statements. This enum provides the possible values for the configuration parameter that determines the planner's behavior:

- **PLAN_CACHE_MODE_AUTO**: The default mode where PostgreSQL automatically decides between custom and generic plans based on cost analysis and execution statistics
- **PLAN_CACHE_MODE_FORCE_GENERIC_PLAN**: Forces the use of generic plans that are parameter-independent and can be reused across executions, saving planning time but potentially using less optimal plans
- **PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN**: Forces the creation of custom plans for each execution using specific parameter values, which may be more optimal but requires more planning time

The plan cache system is fundamental to PostgreSQL's performance optimization for prepared statements, allowing the database to reuse execution plans when appropriate while maintaining the flexibility to generate optimal plans when parameter values significantly affect query performance.

## Parameters / Member Variables
- : Automatic mode - allows PostgreSQL to choose between custom and generic plans based on cost analysis
- : Forces use of generic, parameter-independent plans that can be reused across executions
- : Forces creation of custom plans specific to each set of parameter values

## Dependencies
- Functions called/Symbols referenced:
  - Used by the  GUC parameter system
  - Referenced in  function logic
- Called from (representative examples):
  - GUC configuration system in 
  - Plan cache decision logic in 

## Notes and Other Information
- This enum is defined in 
- The corresponding GUC parameter  is declared as 
- The default value is 
- This setting affects performance characteristics: generic plans save planning time but may be less optimal, while custom plans are potentially more optimal but require more planning overhead
- The setting is considered when a cached plan is executed, not when it is initially prepared
- Can be configured at user session level (PGC_USERSET scope)
- Extensively used in regression tests to verify plan caching behavior under different scenarios