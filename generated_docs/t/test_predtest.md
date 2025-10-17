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

## Simplified Source

```c
Datum test_predtest(PG_FUNCTION_ARGS) {
    text *txt = PG_GETARG_TEXT_PP(0);
    char *query_string = text_to_cstring(txt);

    // Connect to SPI for query execution
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Prepare and execute the test query
    SPIPlanPtr spiplan = SPI_prepare(query_string, 0, NULL);
    if (spiplan == NULL)
        elog(ERROR, "SPI_prepare failed for \"%s\"", query_string);

    int spirc = SPI_execute_plan(spiplan, NULL, NULL, true, 0);
    if (spirc != SPI_OK_SELECT)
        elog(ERROR, "failed to execute \"%s\"", query_string);

    // Validate result structure (must have 2 boolean columns)
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    if (tupdesc->natts != 2 ||
        TupleDescAttr(tupdesc, 0)->atttypid != BOOLOID ||
        TupleDescAttr(tupdesc, 1)->atttypid != BOOLOID)
        elog(ERROR, "test_predtest query must yield two boolean columns");

    // Analyze empirical results for logical relationships
    bool s_i_holds = true, w_i_holds = true, s_r_holds = true, w_r_holds = true;

    for (int i = 0; i < SPI_processed; i++) {
        HeapTuple tup = SPI_tuptable->vals[i];
        bool isnull;

        // Extract column values as 3-way logic (true/false/null)
        Datum dat1 = SPI_getbinval(tup, tupdesc, 1, &isnull);
        char c1 = isnull ? 'n' : (DatumGetBool(dat1) ? 't' : 'f');

        Datum dat2 = SPI_getbinval(tup, tupdesc, 2, &isnull);
        char c2 = isnull ? 'n' : (DatumGetBool(dat2) ? 't' : 'f');

        // Check logical relationship violations
        if (c2 == 't' && c1 != 't') s_i_holds = false;     // strong implication
        if (c2 != 'f' && c1 == 'f') w_i_holds = false;     // weak implication
        if (c2 == 't' && c1 != 'f') s_r_holds = false;     // strong refutation
        if (c2 == 't' && c1 == 't') w_r_holds = false;     // weak refutation
    }

    // Extract expressions from query plan for theoretical testing
    CachedPlan *cplan = SPI_plan_get_cached_plan(spiplan);
    PlannedStmt *stmt = linitial_node(PlannedStmt, cplan->stmt_list);
    Plan *plan = stmt->planTree;

    Expr *clause1 = linitial_node(TargetEntry, plan->targetlist)->expr;
    Expr *clause2 = lsecond_node(TargetEntry, plan->targetlist)->expr;

    // Preprocess expressions and run theoretical proofs
    clause1 = (Expr *) make_ands_implicit(clause1);
    clause2 = (Expr *) make_ands_implicit(clause2);

    bool strong_implied_by = predicate_implied_by((List *) clause1, (List *) clause2, false);
    bool weak_implied_by = predicate_implied_by((List *) clause1, (List *) clause2, true);
    bool strong_refuted_by = predicate_refuted_by((List *) clause1, (List *) clause2, false);
    bool weak_refuted_by = predicate_refuted_by((List *) clause1, (List *) clause2, true);

    // Compare theoretical proofs with empirical results
    if (strong_implied_by && !s_i_holds)
        elog(WARNING, "strong_implied_by result is incorrect");
    if (weak_implied_by && !w_i_holds)
        elog(WARNING, "weak_implied_by result is incorrect");
    if (strong_refuted_by && !s_r_holds)
        elog(WARNING, "strong_refuted_by result is incorrect");
    if (weak_refuted_by && !w_r_holds)
        elog(WARNING, "weak_refuted_by result is incorrect");

    SPI_finish();

    // Build result tuple with all 8 boolean values
    tupdesc = CreateTemplateTupleDesc(8);
    TupleDescInitEntry(tupdesc, 1, "strong_implied_by", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "weak_implied_by", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "strong_refuted_by", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "weak_refuted_by", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "s_i_holds", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 6, "w_i_holds", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 7, "s_r_holds", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, 8, "w_r_holds", BOOLOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    Datum values[8] = {
        BoolGetDatum(strong_implied_by), BoolGetDatum(weak_implied_by),
        BoolGetDatum(strong_refuted_by), BoolGetDatum(weak_refuted_by),
        BoolGetDatum(s_i_holds), BoolGetDatum(w_i_holds),
        BoolGetDatum(s_r_holds), BoolGetDatum(w_r_holds)
    };
    bool nulls[8] = {0};

    return PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
```