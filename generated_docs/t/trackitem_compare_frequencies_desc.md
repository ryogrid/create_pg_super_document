# trackitem_compare_frequencies_desc

## Location
src/backend/tsearch/ts_typanalyze.c: 518 - 529

## Overview
A static comparator function used for sorting TrackItem arrays by frequency in descending order for text search statistics analysis.

## Definition
static int trackitem_compare_frequencies_desc(const void *e1, const void *e2, void *arg)

## Detailed Description
This function implements a comparator for sorting TrackItem structures based on their frequency values in descending order. It is designed to work with PostgreSQL's qsort_arg function and similar sorting utilities. The function takes two void pointers that are expected to be pointers to TrackItem pointers, dereferences them to access the frequency fields, and returns the difference to establish the sorting order.

## Parameters / Member Variables
- e1: Pointer to the first TrackItem pointer to compare
- e2: Pointer to the second TrackItem pointer to compare  
- arg: Additional argument for the comparator (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - TrackItem (struct type)
- Called from (representative examples):
  - compute_tsvector_stats
  - compute_array_stats (in array_typanalyze.c)

## Notes and Other Information
- Returns positive value if e2 frequency > e1 frequency (descending order)
- Returns negative value if e2 frequency < e1 frequency
- Returns 0 if frequencies are equal
- Used in the Lossy Counting algorithm for text search statistics
- Part of PostgreSQL's ANALYZE functionality for tsvector columns
- Located in src/backend/tsearch/ts_typanalyze.c:518-529