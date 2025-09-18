# pg_get_statisticsobjdef_string

## Location
src/backend/utils/adt/ruleutils.c: 1607 - 1616

## Overview
Returns the definition string for a statistics object in a format suitable for ALTER TABLE operations, including a tablespace clause.

## Definition


## Detailed Description
This function is an internal version of the statistics object definition retrieval functionality, specifically designed for use by ALTER TABLE commands. It generates a complete definition string for a statistics object that includes tablespace information, which is necessary when recreating statistics objects during table alterations. The function returns a palloc'd C string without pretty-printing formatting.

## Parameters / Member Variables
- : The OID of the statistics object for which to generate the definition string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_statisticsobj_worker](pg_get_statisticsobj_worker.md)
- Called from (representative examples):
  - [RememberStatisticsForRebuilding](../R/RememberStatisticsForRebuilding.md) (in src/backend/commands/tablecmds.c)

## Notes and Other Information
- This is an internal function specifically designed for ALTER TABLE operations
- Unlike user-facing functions, this version includes tablespace clause information
- Returns a palloc'd string that must be freed by the caller
- Does not perform pretty-printing, returning raw definition text
- The function delegates all actual work to pg_get_statisticsobj_worker with specific parameters (false, false)