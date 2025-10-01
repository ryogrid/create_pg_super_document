# ind_fetch_func

## Location
[src/backend/commands/analyze.c:1768-1797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1768-L1797)

## Overview
A static function that serves as a data fetch callback for analyzing index expressions, retrieving Datum values from pre-computed arrays without constructing full index tuples.

## Definition

```c
typedef struct
{
	int			count;			/* # of duplicates */
	int			first;			/* values[] index of first occurrence */
} ScalarMCVItem;
```
## Detailed Description
The  function is a specialized fetch function used during index analysis operations. It provides an efficient way to access expression values for statistical analysis without the overhead of constructing complete index tuples. The function operates on pre-computed arrays of Datum values and null indicators that are stored in the  structure. This approach optimizes memory usage and performance during the ANALYZE operation on index expressions.

## Parameters
- `stats`: A pointer to VacAttrStatsP structure containing the expression values and null indicators arrays
- `rownum`: The row number (index) to fetch data from in the arrays
- `isNull`: Output parameter that will be set to indicate whether the fetched value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [VacAttrStatsP](../V/VacAttrStatsP.md) (structure type)
- Called from (representative examples):
  - [compute_index_stats](../c/compute_index_stats.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the analyze.c file
- The function assumes that exprvals and exprnulls arrays are already properly offset for the target column
- Uses rowstride from the stats structure to calculate the correct array index
- Designed for efficiency during index expression analysis by avoiding tuple construction overhead
- Part of PostgreSQL's ANALYZE command implementation for gathering statistics on index expressions

## Simplified Source

```c
static Datum ind_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
    // Calculate array index using rowstride
    int i = rownum * stats->rowstride;

    // Set null indicator and return value from pre-computed arrays
    *isNull = stats->exprnulls[i];
    return stats->exprvals[i];
}
```