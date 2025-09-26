# test_predtest

## Location
[src/test/modules/test_predtest/test_predtest.c:32-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_predtest/test_predtest.c#L32-L246)

## Overview
A PostgreSQL testing function that validates the correctness of predicate testing logic by comparing theoretical proof results with empirical query execution results.

## Definition

```c
Datum
test_predtest(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a comprehensive testing utility designed to validate PostgreSQL's predicate testing functionality in . It takes a SQL query string containing two boolean expressions and performs both empirical testing (by executing the query) and theoretical analysis (using predicate proof functions) to verify the correctness of implication and refutation logic.

The function operates in two phases:
1. **Empirical Testing**: Executes the provided query and analyzes the result set to determine if various logical relationships (strong/weak implication and refutation) hold based on actual data
2. **Theoretical Analysis**: Extracts the boolean expressions from the query plan and applies PostgreSQL's predicate proof functions to determine what the system believes about these relationships

The function then compares the empirical and theoretical results, issuing warnings when discrepancies are found, which would indicate bugs in the predicate testing logic.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : A text argument containing the SQL query string with two boolean expressions to test

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](text_to_cstring.md)
  - [SPI_connect](../S/SPI_connect.md), SPI_prepare, SPI_execute_plan, SPI_finish
  - [SPI_getbinval](../S/SPI_getbinval.md), SPI_plan_get_cached_plan
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [predicate_refuted_by](../p/predicate_refuted_by.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md), TupleDescInitEntry, BlessTupleDesc
  - [heap_form_tuple](../h/heap_form_tuple.md), HeapTupleGetDatum
  - PG_RETURN_DATUM
- Called from (representative examples):
  - No direct callers found (likely invoked via SQL function calls)

## Notes and Other Information
- Located in test module: 
- Requires input query to return exactly two boolean columns for comparison
- Tests four types of logical relationships:
  - **Strong implication**: truth of clause2 implies truth of clause1
  - **Weak implication**: non-falsity of clause2 implies non-falsity of clause1  
  - **Strong refutation**: truth of clause2 implies falsity of clause1
  - **Weak refutation**: truth of clause2 implies non-truth of clause1
- Returns a record with 8 boolean fields showing both proof results and empirical test results
- Issues warnings when theoretical proofs contradict empirical evidence, indicating potential bugs
- Part of PostgreSQL's test infrastructure for validating query optimization logic
- Uses SPI (Server Programming Interface) for query execution and plan analysis