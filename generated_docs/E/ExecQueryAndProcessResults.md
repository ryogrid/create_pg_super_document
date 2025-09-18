# ExecQueryAndProcessResults

## Location
src/bin/psql/common.c: 1446 - 1832

## Overview
ExecQueryAndProcessResults is a comprehensive utility function that sends queries to PostgreSQL and handles all result processing, including COPY operations, chunked results, and various output modes.

## Definition
static int ExecQueryAndProcessResults(const char *query,
                                    double *elapsed_msec, bool *svpt_gone_p,
                                    bool is_watch, int min_rows,
                                    const printQueryOpt *opt, FILE *printQueryFout)

## Detailed Description
ExecQueryAndProcessResults serves as the core query execution engine for both SendQuery() and PSQLexecWatch(). It provides sophisticated handling of various PostgreSQL result types and psql output modes:

**Key Responsibilities:**
1. **Query Transmission**: Uses PQsendQuery() or PQsendQueryParams() for asynchronous query execution
2. **Chunked Results**: Implements FETCH_COUNT functionality using PQsetChunkedRowsMode() for large result sets
3. **COPY Operations**: Handles COPY IN/OUT operations with appropriate stream routing
4. **Result Processing**: Manages different result types (tuples, commands, copy, chunked data)
5. **Error Handling**: Comprehensive error detection and connection state management
6. **Output Routing**: Directs output to appropriate streams (pager, files, stdout) based on context
7. **Savepoint Tracking**: Monitors commands that would invalidate temporary savepoints
8. **Timing Measurement**: Records elapsed time for performance analysis

**Special Handling:**
- **Chunked Mode**: When FETCH_COUNT > 0, results are fetched and displayed incrementally
- **Watch Mode**: Special behavior for \watch command with min_rows support
- **COPY Streams**: Intelligent routing of COPY output to appropriate destinations
- **Variable Setting**: Updates psql variables (ERROR, SQLSTATE, ROW_COUNT) based on results

## Parameters / Member Variables
- `query`: The SQL query string to execute
- `elapsed_msec`: Output parameter for execution timing
- `svpt_gone_p`: Tracks whether temporary savepoints have been invalidated
- `is_watch`: Indicates execution from \watch command
- `min_rows`: Minimum rows required for \watch (0 if not applicable)
- `opt`: Print options for result formatting (can be NULL)
- `printQueryFout`: File stream for status output

## Dependencies
- Functions called/Symbols referenced:
  - PQsendQuery, PQsendQueryParams
  - PQgetResult, PQresultStatus
  - PQsetChunkedRowsMode
  - AcceptResult, HandleCopyResult
  - PrintQueryResult, printQuery
  - SetupGOutput, CloseGOutput
  - PageOutput, ClosePager
  - SetResultVariables, ClearOrSaveResult
  - CheckConnection, ClearOrSaveAllResults
- Called from (representative examples):
  - SendQuery (for regular query execution)
  - PSQLexecWatch (for \watch command)

## Notes and Other Information
- Returns 1 for complete success, 0 for interrupt, -1 for errors
- Function is static, only accessible within common.c
- Handles complex result processing including multiple result sets from compound queries
- Implements sophisticated cancellation handling with proper cleanup
- Supports all major psql output modes (\g, \gexec, \gset, \crosstab, etc.)
- Uses asynchronous libpq interface for better responsiveness
- Chunked mode is disabled for certain operations that need complete result sets
- Properly manages pager usage for large result sets going to stdout