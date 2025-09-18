# test_transaction

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1773-1920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1773-L1920)

## Overview
Tests PostgreSQL pipeline behavior during transaction errors, specifically how pipeline-aborted state affects subsequent commands and transaction recovery mechanisms.

## Definition
```c
static void test_transaction(PGconn *conn)
```

## Detailed Description
This test function validates pipeline behavior when transaction errors occur and demonstrates the pipeline-aborted state recovery mechanisms. The test simulates a realistic error scenario and validates proper pipeline state transitions:

1. **Setup**: Creates a test table and enters pipeline mode, then prepares a ROLLBACK statement
2. **Error Generation**: Executes `BEGIN` followed by `SELECT 0/0` (division by zero error) to trigger a transaction error
3. **Pipeline-Aborted State Testing**: 
   - Attempts to execute a prepared ROLLBACK (fails due to pipeline-aborted state)
   - Attempts to INSERT (fails due to pipeline-aborted state)
   - Issues a pipeline sync to clear the pipeline-aborted state
4. **Transaction-Aborted State Testing**:
   - Attempts another INSERT (fails due to transaction-aborted state)
   - Issues another pipeline sync
5. **Recovery**: 
   - Executes prepared ROLLBACK (succeeds after sync)
   - Executes final INSERT (succeeds after transaction recovery)
6. **Validation**: Verifies that only the final INSERT succeeded by checking table contents

The test demonstrates the distinction between pipeline-aborted state (recoverable with sync) and transaction-aborted state (requires explicit transaction termination).

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) used for pipeline operations and transaction testing

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) - Execute immediate SQL commands
  - PQenterPipelineMode - Enter pipeline mode
  - [PQsendPrepare](../P/PQsendPrepare.md) - Prepare a statement in pipeline
  - [PQsendQueryParams](../P/PQsendQueryParams.md) - Send parameterized queries
  - [PQsendQueryPrepared](../P/PQsendQueryPrepared.md) - Execute prepared statements
  - PQpipelineSync - Send pipeline synchronization
  - [PQgetResult](../P/PQgetResult.md) - Retrieve query results
  - PQexitPipelineMode - Exit pipeline mode
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQresStatus](../P/PQresStatus.md) - Get status string representation
  - [PQntuples](../P/PQntuples.md) - Get number of tuples in result
  - [PQgetvalue](../P/PQgetvalue.md) - Get specific field value
  - [PQclear](../P/PQclear.md) - Free result memory
  - [PQerrorMessage](../P/PQerrorMessage.md) - Get error message
  - PGRES_COMMAND_OK - [Command](../C/Command.md) executed successfully
  - PGRES_TUPLES_OK - Normal tuples result status
  - PGRES_FATAL_ERROR - Fatal error status
  - PGRES_PIPELINE_ABORTED - Pipeline aborted status
  - PGRES_PIPELINE_SYNC - Pipeline sync result status
  - ExecStatusType - [Result](../R/Result.md) status enumeration type
- Called from (representative examples):
  - [main](../m/main.md) - Main test driver function

## Notes and Other Information
- This is a comprehensive test for PostgreSQL pipeline error handling and state management
- Tests the distinction between pipeline-aborted and transaction-aborted states
- Creates and uses a temporary test table `pq_pipeline_tst` for validation
- Uses prepared statements to test statement execution in different pipeline states
- Demonstrates that pipeline sync can clear pipeline-aborted state but not transaction-aborted state
- The test expects specific command failure patterns and validates recovery mechanisms
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- Validates that only commands executed after proper error recovery succeed
- The final validation ensures that exactly one row with value "3" exists in the test table
- Shows how transaction errors affect the entire pipeline until properly resolved
- Demonstrates the proper sequence for recovering from both pipeline and transaction errors