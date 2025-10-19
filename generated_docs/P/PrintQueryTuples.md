# PrintQueryTuples

## Location
[src/bin/psql/common.c:738-761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L738-L761)

## Overview
PrintQueryTuples is a static helper function that formats and prints query result tuples using configurable output options and destination streams.

## Definition

```c
static bool
PrintQueryTuples(const PGresult *result, const printQueryOpt *opt,
				 FILE *printQueryFout)
```
## Detailed Description
PrintQueryTuples handles the formatting and output of query result data, assuming the PGresult contains valid tuple data. The function provides flexible configuration through its parameters:

- Uses provided printQueryOpt for formatting, falling back to pset.popt if NULL
- Directs output to specified FILE stream, defaulting to pset.queryFout if NULL
- Delegates actual formatting to the printQuery() function with appropriate parameters
- Includes error handling for output stream issues
- Ensures output is flushed for immediate display
- Returns success/failure status to allow caller error handling

The function serves as a standardized interface for result tuple printing throughout psql, providing consistent formatting and error handling while allowing customization of both format options and output destination.

## Parameters / Member Variables
- `*result`: Pointer to PGresult containing the query result data to be printed
- `*opt`: Pointer to printQueryOpt structure for formatting options (NULL uses default pset.popt)
- `*printQueryFout`: FILE pointer for output destination (NULL uses default pset.queryFout)
## Dependencies
- Functions called/Symbols referenced:
  - [printQueryOpt](../p/printQueryOpt.md) (structure type for formatting configuration)
  - [printQuery](../p/printQuery.md) (core function that performs the actual result formatting and output)

- Called from (representative examples):
  - [PrintQueryResult](PrintQueryResult.md) (higher-level result printing coordination)

## Notes and Other Information
- This is a static function, only accessible within src/bin/psql/common.c
- Returns true on successful output, false if I/O errors occur
- Uses ferror() to detect output stream errors after printing
- Provides dual-stream output capability (both to main output and logfile via printQuery)
- Part of psql's layered result processing architecture
- Ensures immediate output visibility through fflush() call
- Integrates with psql's configurable output formatting system

## Simplified Source

```c
static bool PrintQueryTuples(const PGresult *result, const printQueryOpt *opt,
                            FILE *printQueryFout) {
    bool ok = true;

    // Use provided output stream or default
    FILE *fout = printQueryFout ? printQueryFout : pset.queryFout;

    // Print query results with options (use defaults if none provided)
    printQuery(result, opt ? opt : &pset.popt, fout, false, pset.logfile);

    // Ensure output is written immediately
    fflush(fout);

    // Check for output errors
    if (ferror(fout)) {
        pg_log_error("could not print result table: %m");
        ok = false;
    }

    return ok;
}
```