# tsm_bernoulli_handler

## Location
[src/backend/access/tablesample/bernoulli.c:65-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/tablesample/bernoulli.c#L65-L85)

## Overview
This function creates a TsmRoutine descriptor for the BERNOULLI tablesample method, which implements statistical sampling based on the Bernoulli distribution.

## Definition
```c
Datum tsm_bernoulli_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tsm_bernoulli_handler` function serves as the entry point for the BERNOULLI tablesample method. It creates and initializes a TsmRoutine structure that defines the behavior of the Bernoulli sampling algorithm. The function sets up all the necessary callback functions and parameters needed for PostgreSQL's tablesample infrastructure to perform Bernoulli sampling on table data. The Bernoulli method samples each tuple independently with a given probability, providing a statistically sound way to obtain random samples from large tables.

## Parameters / Member Variables
- Returns: Datum containing pointer to initialized TsmRoutine structure

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates TsmRoutine node)
  - list_make1_oid (creates parameter type list)
  - [bernoulli_samplescangetsamplesize](../b/bernoulli_samplescangetsamplesize.md) (sample size calculation callback)
  - [bernoulli_initsamplescan](../b/bernoulli_initsamplescan.md) (scan initialization callback)
  - [bernoulli_beginsamplescan](../b/bernoulli_beginsamplescan.md) (scan begin callback)
  - [bernoulli_nextsampletuple](../b/bernoulli_nextsampletuple.md) (tuple sampling callback)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- The function sets parameterTypes to FLOAT4OID, indicating it accepts a float4 parameter (the sampling percentage)
- Both repeatable_across_queries and repeatable_across_scans are set to true, ensuring consistent sampling behavior
- NextSampleBlock is set to NULL, indicating this method works at the tuple level rather than block level
- EndSampleScan is set to NULL, indicating no cleanup is needed when scanning ends
- This is a PostgreSQL extension function that integrates with the tablesample infrastructure