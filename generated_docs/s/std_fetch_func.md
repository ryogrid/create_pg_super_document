# std_fetch_func

## Location
src/backend/commands/analyze.c: 1752 - 1767

## Overview
A standard fetch function that provides a uniform interface for compute_stats subroutines to extract attribute values from sampled tuples.

## Definition


## Detailed Description
The std_fetch_func function serves as a standardized interface between statistical computation routines and the actual sample data storage. It abstracts the process of extracting a specific attribute value from a given row in the sample data, hiding the details of tuple structure and attribute access from the statistics calculation code.

This function acts as an abstraction layer, allowing compute_stats subroutines to focus on statistical calculations rather than the mechanics of tuple attribute retrieval. It uses the heap_getattr function to extract the requested attribute value from the specified tuple, handling null value detection appropriately.

The function is designed to be passed as a function pointer to various statistics computation routines that need to access sample data values in a uniform way.

## Parameters / Member Variables
- : Pointer to VacAttrStats structure containing sample data and metadata
- : Index of the row (tuple) to fetch the attribute value from
- : Output parameter set to true if the attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getattr](../h/heap_getattr.md)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (assigned as function pointer for statistics computation)

## Notes and Other Information
- Provides abstraction between statistics computation routines and sample data access
- Uses the tupattnum field from VacAttrStats to identify which attribute to extract
- Accesses the rows array and tupDesc from the VacAttrStats structure
- Returns the attribute value as a Datum, with null indication via the isNull parameter
- Typically used as a function pointer argument to compute_stats and related routines
- Part of the pluggable statistics computation framework in PostgreSQL's ANALYZE system