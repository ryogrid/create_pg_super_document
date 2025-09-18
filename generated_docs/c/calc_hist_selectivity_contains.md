# calc_hist_selectivity_contains

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 1252 - 1336

## Overview
Calculates selectivity of the "var @> const" operator, estimating the fraction of multiranges that contain the constant lower and upper bounds using histograms.

## Definition


## Detailed Description
This function estimates what fraction of multiranges in the database contain (i.e., completely encompass) a given constant range. Like its counterpart calc_hist_selectivity_contained, it uses histograms of range lower bounds and lengths, assuming independence between these properties.

The algorithm works by:
1. Finding the bin containing the lower bound of the query range in the lower bound histogram
2. Walking backwards through bins with lower bounds <= query lower bound
3. For each bin, calculating what fraction of ranges would be long enough to extend past the query upper bound
4. Summing these fractions weighted by bin populations

This is essentially the complement operation to containment - instead of asking "how many ranges fit inside this constant range?", it asks "how many ranges does this constant range fit inside?"

## Parameters / Member Variables
- : Type cache entry containing range type information and comparison functions
- : Lower bound of the constant range for contains testing
- : Upper bound of the constant range for contains testing
- : Array of histogram values for range lower bounds  
- : Number of values in the lower bound histogram
- : Array of histogram values for range lengths
- : Number of values in the length histogram

## Dependencies
- Functions called/Symbols referenced:
  - rbound_bsearch
  - get_position
  - get_distance
  - calc_length_hist_frac
  - RangeBound
- Called from (representative examples):
  - calc_hist_selectivity

## Notes and Other Information
- Implements selectivity estimation for the contains operator (@>) on range types
- Uses complement of calc_length_hist_frac (1.0 - result) to find ranges long enough to contain the query
- Critical for optimizing queries that filter by range containment relationships
- Handles boundary conditions and edge cases similar to calc_hist_selectivity_contained
- Essential component of PostgreSQL's cost-based optimizer for range queries