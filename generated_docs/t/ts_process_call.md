# ts_process_call

## Location
src/backend/utils/adt/tsvector_op.c: 2535 - 2574

## Overview
Processes subsequent calls in a set-returning function that iterates over TSVectorStat entries, formatting and returning one row of statistics data per call.

## Definition


## Detailed Description
This function handles the processing of individual calls within a set-returning function (SRF) that iterates through TSVectorStat tree entries. It retrieves the next entry from the tree traversal using walkStatEntryTree, formats the entry data into a tuple with three columns (lexeme, ndoc, nentry), and returns it as a Datum. The function constructs C-string representations of the lexeme text and numeric statistics, builds a heap tuple from these values, and marks the processed entry as visited by setting its ndoc to 0.

The function works as part of PostgreSQL's SRF framework, being called repeatedly until all entries in the statistics tree have been processed and returned.

## Parameters / Member Variables
- `funcctx`: Function call context containing the TSVectorStat data in user_fctx and tuple metadata

## Dependencies
- Functions called/Symbols referenced:
  - [walkStatEntryTree](../w/walkStatEntryTree.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - sprintf
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ts_stat1](ts_stat1.md)
  - [ts_stat2](ts_stat2.md)

## Notes and Other Information
- Returns a tuple with three text columns: lexeme (word), ndoc (document count), nentry (occurrence count)
- Uses sprintf to convert numeric statistics to string representation for tuple construction
- Allocates memory for the lexeme string and properly null-terminates it
- Marks processed entries as visited by setting ndoc to 0 to avoid reprocessing
- Returns (Datum) 0 when no more entries are available, signaling end of result set
- Memory management includes freeing the allocated lexeme string after tuple construction
- Part of PostgreSQL's text search functionality for analyzing TSVector statistics
- The returned tuple format matches the expected output schema for ts_stat functions