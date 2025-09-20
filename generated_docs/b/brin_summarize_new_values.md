# brin_summarize_new_values

## Location
[src/backend/access/brin/brin.c:1356-1370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1356-L1370)

## Overview
A SQL-callable function that scans through a BRIN index and summarizes all block ranges that are not currently summarized.

## Definition

```c
Datum
brin_summarize_new_values(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a wrapper that calls  with a special argument to process all block ranges in the index. It is designed to be called from SQL to update index summaries for ranges that have been modified since the last summarization. The function uses  (which is ) to indicate that all ranges should be processed, not just a specific range.

## Parameters / Member Variables
- : The first argument (accessed via ) representing the BRIN index relation to be summarized

## Dependencies
- Functions called/Symbols referenced:
  - : Used to invoke brin_summarize_range
  - : The actual implementation function that performs the summarization
  - : Converts the block range specification to a Datum
  - : Constant indicating all block ranges should be processed
- Called from (representative examples):
  - SQL interface (as this is a SQL-callable function)

## Notes and Other Information
- This is a thin wrapper function that delegates the actual work to 
- The function is designed to be exposed to SQL users for maintenance operations
- Uses the special value  to indicate that all ranges should be summarized rather than a specific range
- Returns a Datum as required by PostgreSQL's function call interface