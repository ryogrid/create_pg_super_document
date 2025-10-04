# pg_get_wal_summarizer_state

## Location
[src/backend/backup/walsummaryfuncs.c:177-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummaryfuncs.c#L177-L208)

## Overview
Returns information about the current state of the WAL summarizer process as a composite tuple containing timeline ID, LSN positions, and process ID.

## Definition

```c
Datum
pg_get_wal_summarizer_state(PG_FUNCTION_ARGS)
```
## Detailed Description
This SQL-callable function provides a snapshot of the WAL summarizer's operational state by retrieving key metrics from the shared memory control structure. It returns a composite row type containing four columns that describe the current summarization progress and process status.

The function calls  to safely retrieve the state information from shared memory while holding the WALSummarizerLock in shared mode. The returned data includes the timeline being summarized, the LSN up to which summarization has been completed, the LSN currently being processed, and the process ID of the summarizer if it's running.

If the summarizer process is not active (indicated by a negative PID), the corresponding field in the result tuple is set to NULL.

## Parameters / Member Variables
This function takes no parameters ( is the standard PostgreSQL function interface).

**Return Values (Composite Tuple):**
-  (int8): Timeline ID being summarized
-  (pg_lsn): LSN up to which WAL summarization has been completed  
-  (pg_lsn): LSN currently being processed by the summarizer
-  (int4): Process ID of the WAL summarizer process (NULL if not running)

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves WAL summarizer state from shared memory
  - : Validates that return type is composite
  - : Converts int64 timeline ID to Datum
  - : Converts XLogRecPtr LSN values to Datum
  - : Converts int32 PID to Datum
  - : Creates heap tuple from values array
  - : Converts heap tuple to Datum for return
  - : Constant defining number of return attributes (4)

- Called from (representative examples):
  - SQL queries via system catalog functions
  - Database monitoring and administrative tools

## Notes and Other Information
- This function is part of PostgreSQL's WAL summary functionality introduced for incremental backups
- The function is declared in the system catalog as returning a composite type with 4 attributes
- Access to WAL summarizer state is protected by WALSummarizerLock to ensure consistent reads
- The function handles the case where the summarizer process has not been initialized or has exited
- Located in src/backend/backup/walsummaryfuncs.c

## Simplified Source
```c
Datum pg_get_wal_summarizer_state(PG_FUNCTION_ARGS) {
    Datum values[NUM_STATE_ATTS];
    bool nulls[NUM_STATE_ATTS];
    TimeLineID summarized_tli;
    XLogRecPtr summarized_lsn;
    XLogRecPtr pending_lsn;
    int summarizer_pid;
    TupleDesc tupdesc;

    // Get current WAL summarizer state from shared memory
    GetWalSummarizerState(&summarized_tli, &summarized_lsn, &pending_lsn, &summarizer_pid);

    // Validate return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    memset(nulls, 0, sizeof(nulls));

    // Build result tuple
    values[0] = Int64GetDatum((int64) summarized_tli);
    values[1] = LSNGetDatum(summarized_lsn);
    values[2] = LSNGetDatum(pending_lsn);

    // Handle case where summarizer is not running
    if (summarizer_pid < 0) {
        nulls[3] = true;
    } else {
        values[3] = Int32GetDatum(summarizer_pid);
    }

    HeapTuple htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
``` 