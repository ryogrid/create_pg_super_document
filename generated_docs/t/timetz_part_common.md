# timetz_part_common

## Location
[src/backend/utils/adt/date.c:2927-3043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2927-L3043)

## Overview
A common implementation function that extracts specified time components (hour, minute, second, timezone, etc.) from a time with timezone (TimeTzADT) value, with support for both numeric and floating-point return types.

## Definition
```c
static Datum timetz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
```

## Detailed Description
This static function provides the core implementation for extracting various time components from TimeTzADT values. It handles a wide range of extraction units including timezone information (DTK_TZ, DTK_TZ_MINUTE, DTK_TZ_HOUR), time components (DTK_HOUR, DTK_MINUTE, DTK_SECOND, DTK_MICROSEC, DTK_MILLISEC), and special values like epoch time. The function parses the unit specification string, converts the TimeTzADT to a broken-down time structure, and extracts the requested component. It supports returning results as either numeric values (when retnumeric is true) or floating-point values, with special handling for fractional seconds and milliseconds. The function includes comprehensive error checking for unsupported units and properly handles timezone-specific extractions.

## Parameters / Member Variables
- Input parameter 0 (via PG_GETARG_TEXT_PP(0)): A text value specifying the unit to extract (e.g., 'hour', 'minute', 'timezone')
- Input parameter 1 (via PG_GETARG_TIMETZADT_P(1)): A TimeTzADT pointer representing the time with timezone value
- : Boolean flag determining return type (numeric if true, float8 if false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Macro to extract text argument
  - PG_GETARG_TIMETZADT_P: Macro to extract TimeTzADT argument
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md): Function to normalize unit strings
  - [DecodeUnits](../D/DecodeUnits.md)/DecodeSpecial: Functions to parse time unit specifications
  - [timetz2tm](timetz2tm.md): Function to convert TimeTzADT to broken-down time structure
  - [int64_div_fast_to_numeric](../i/int64_div_fast_to_numeric.md)/int64_to_numeric: Numeric conversion functions
  - PG_RETURN_NUMERIC/PG_RETURN_FLOAT8: Macros to return results
  - ereport/errcode/errmsg: Error reporting functions
- Constants used:
  - DTK_TZ, DTK_TZ_MINUTE, DTK_TZ_HOUR: Timezone extraction constants
  - DTK_MICROSEC, DTK_MILLISEC, DTK_SECOND, DTK_MINUTE, DTK_HOUR: Time unit constants
  - DTK_EPOCH: Epoch time constant
  - SECS_PER_MINUTE, MINS_PER_HOUR, SECS_PER_HOUR: Time conversion constants
  - INT64CONST: 64-bit integer constant macro
- Types used:
  - TimeTzADT: Time with timezone data type
  - [pg_tm](../p/pg_tm.md): Broken-down time structure
  - fsec_t: Fractional seconds type
- Called from (representative examples):
  - [timetz_part](timetz_part.md): Public function for EXTRACT() with float8 return
  - [extract_timetz](../e/extract_timetz.md): Public function for EXTRACT() with numeric return

## Notes and Other Information
- This is a static (internal) function shared by timetz_part and extract_timetz
- Supports extraction of timezone information with proper sign handling (negative for western timezones)
- Handles fractional seconds with microsecond precision
- Returns NULL for unsupported date-related units (day, month, year, etc.) since TimeTzADT contains no date information
- Epoch extraction returns seconds since Unix epoch adjusted for timezone
- The retnumeric parameter allows the same logic to serve both numeric and floating-point EXTRACT functions
- Located in src/backend/utils/adt/date.c with other date/time utility functions
- Comprehensive error handling with appropriate error codes for invalid or unsupported units
- Special handling for millisecond and second extraction to maintain precision when returning numeric values

## Simplified Source

```c
static Datum
timetz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
{
	text *units = PG_GETARG_TEXT_PP(0);
	TimeTzADT *time = PG_GETARG_TIMETZADT_P(1);
	int64 intresult;
	int type, val;
	char *lowunits;

	// Parse the unit specification
	lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units), false);
	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS) {
		// Extract time components
		int tz;
		fsec_t fsec;
		struct pg_tm tt, *tm = &tt;

		timetz2tm(time, tm, &fsec, &tz);

		switch (val) {
			case DTK_TZ:
				intresult = -tz;  // Timezone offset in seconds
				break;
			case DTK_TZ_MINUTE:
				intresult = (-tz / SECS_PER_MINUTE) % MINS_PER_HOUR;
				break;
			case DTK_TZ_HOUR:
				intresult = -tz / SECS_PER_HOUR;
				break;
			case DTK_MICROSEC:
				intresult = tm->tm_sec * INT64CONST(1000000) + fsec;
				break;
			case DTK_MILLISEC:
				if (retnumeric)
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 3));
				else
					PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + fsec / 1000.0);
				break;
			case DTK_SECOND:
				if (retnumeric)
					PG_RETURN_NUMERIC(int64_div_fast_to_numeric(tm->tm_sec * INT64CONST(1000000) + fsec, 6));
				else
					PG_RETURN_FLOAT8(tm->tm_sec + fsec / 1000000.0);
				break;
			case DTK_MINUTE:
				intresult = tm->tm_min;
				break;
			case DTK_HOUR:
				intresult = tm->tm_hour;
				break;
			default:
				// Unsupported units for TIMETZ
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unit \"%s\" not supported for type %s", lowunits, format_type_be(TIMETZOID))));
				intresult = 0;
		}
	} else if (type == RESERV && val == DTK_EPOCH) {
		// Epoch extraction
		if (retnumeric)
			PG_RETURN_NUMERIC(int64_div_fast_to_numeric(time->time + time->zone * INT64CONST(1000000), 6));
		else
			PG_RETURN_FLOAT8(time->time / 1000000.0 + time->zone);
	} else {
		// Invalid unit
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("unit \"%s\" not recognized for type %s", lowunits, format_type_be(TIMETZOID))));
		intresult = 0;
	}

	// Return result as numeric or float8
	if (retnumeric)
		PG_RETURN_NUMERIC(int64_to_numeric(intresult));
	else
		PG_RETURN_FLOAT8(intresult);
}
```