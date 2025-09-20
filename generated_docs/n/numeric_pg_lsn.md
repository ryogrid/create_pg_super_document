# numeric_pg_lsn

## Location
[src/backend/utils/adt/numeric.c:4766-4809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4766-L4809)

## Overview
Converts a PostgreSQL numeric value to a pg_lsn (Log Sequence Number) value, performing range validation and rejecting special numeric values.

## Definition

```c
typedef struct NumericAggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	MemoryContext agg_context;	/* context we're calculating in */
	int64		N;				/* count of processed numbers */
	NumericSumAccum sumX;		/* sum of processed numbers */
	NumericSumAccum sumX2;		/* sum of squares of processed numbers */
	int			maxScale;		/* maximum scale seen so far */
	int64		maxScaleCount;	/* number of values seen with maximum scale */
	/* These counts are *not* included in N!  Use NA_TOTAL_COUNT() as needed */
	int64		NaNcount;		/* count of NaN values */
	int64		pInfcount;		/* count of +Inf values */
	int64		nInfcount;		/* count of -Inf values */
} NumericAggState;
```
## Detailed Description
This function performs type conversion from PostgreSQL's arbitrary precision numeric type to a pg_lsn (XLogRecPtr) value. The pg_lsn type represents a position in the PostgreSQL write-ahead log and must be a valid unsigned 64-bit integer. The function first checks for special numeric values (NaN, infinity) and explicitly rejects them with appropriate error messages since they cannot be meaningfully converted to log positions. For regular numeric values, it converts the numeric to variable format and then attempts to convert to an unsigned 64-bit integer, ensuring the value fits within the valid range for pg_lsn values.

## Parameters / Member Variables
- Input parameter accessed via : The numeric value to be converted to pg_lsn

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if numeric has special value (NaN, infinity)
  -  - Check specifically for NaN
  -  - Convert numeric to NumericVar format
  -  - Convert NumericVar to 64-bit unsigned integer
  -  - Report errors with specific error codes
  -  - Return pg_lsn value
- Called from (representative examples):
  -  - pg_lsn plus integer operation
  -  - pg_lsn minus integer operation

## Notes and Other Information
- Explicitly rejects NaN and infinity values with 
- Validates that the numeric value fits in unsigned 64-bit integer range
- Reports  for out-of-range values
- Uses PostgreSQL's variable-format numeric representation as intermediate step
- Located in src/backend/utils/adt/numeric.c:4766-4809
- The pg_lsn type represents positions in PostgreSQL's write-ahead log system