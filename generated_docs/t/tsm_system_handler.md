# tsm_system_handler

## Location
src/backend/access/tablesample/system.c: 67 - 87

## Overview
Creates and initializes a TsmRoutine descriptor for the SYSTEM table sampling method, setting up the function pointers and parameters for system sampling operations.

## Definition


## Detailed Description
The tsm_system_handler function is the entry point handler for PostgreSQL's SYSTEM table sampling method. It creates a TsmRoutine structure and populates it with the appropriate function pointers and configuration settings specific to the SYSTEM sampling algorithm. The SYSTEM method performs block-level sampling by randomly selecting blocks from the table and then scanning all tuples within those selected blocks. This function sets up the sampling infrastructure by defining which functions handle different phases of the sampling process and configuring the method's repeatability characteristics.

## Parameters / Member Variables
- This function uses PG_FUNCTION_ARGS macro (no explicit parameters)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates TsmRoutine node)
  - list_make1_oid (creates parameter type list)
  - system_samplescangetsamplesize (sample size calculation function)
  - system_initsamplescan (scan initialization function)
  - system_beginsamplescan (scan begin function)
  - system_nextsampleblock (block selection function)
  - system_nextsampletuple (tuple selection function)
- Called from (representative examples):
  - PostgreSQL function manager (via SQL function calls)

## Notes and Other Information
- The SYSTEM method accepts one FLOAT4 parameter representing the sampling percentage
- The method is configured as repeatable across both queries and scans
- EndSampleScan is set to NULL, indicating no special cleanup is needed
- This handler is typically registered in the system catalogs and called when TABLESAMPLE SYSTEM() is used in SQL queries