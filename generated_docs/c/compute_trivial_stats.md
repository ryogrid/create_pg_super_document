# compute_trivial_stats

## Location
[src/backend/commands/analyze.c:1923-2012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1923-L2012)

## Overview
A basic statistical analysis function that computes fundamental column statistics (null fraction and average width) when hash equality operators are not available for the data type.

## Definition
```c
static void compute_trivial_stats(VacAttrStatsP stats,
                                  AnalyzeAttrFetchFunc fetchfunc,
                                  int samplerows,
                                  double totalrows)
```

## Detailed Description
The `compute_trivial_stats` function performs minimal statistical analysis when more sophisticated analysis methods cannot be applied due to lack of suitable operators for the data type. It iterates through the sample data to count null and non-null values, and calculates average datum width for variable-length types. For variable-length types, it handles both varlena types (using VARSIZE_ANY) and cstring types (using strlen). The function sets basic statistics including null fraction, average width, and marks distinct values as unknown. It includes vacuum delay points to prevent blocking during long operations.

## Parameters / Member Variables
- `stats`: Pointer to VacAttrStatsP structure to store computed statistics
- `fetchfunc`: Function pointer to retrieve datum values from the sample
- `samplerows`: Number of rows in the sample to analyze
- `totalrows`: Total number of rows in the table (not directly used in computation)

## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - VARSIZE_ANY
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - strlen
- Called from (representative examples):
  - [std_typanalyze](../s/std_typanalyze.md)

## Notes and Other Information
- This is a static function used as a fallback when equality operators are not available
- Handles three data width scenarios: fixed-width types, varlena types, and cstring types
- For varlena types, uses the toasted width rather than detoasted width for efficiency
- Sets stadistinct to 0.0 ("unknown") since distinct value analysis requires equality operators
- Marks stats as valid only if at least some data is found (either null or non-null)
- If only nulls are found, assumes the entire column is null
- Includes vacuum delay points to allow interruption during long analysis operations
- Used when data types lack hash equality operators needed for more detailed statistical analysis