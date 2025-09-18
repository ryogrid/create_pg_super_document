# dtcvasc

## Location
[src/interfaces/ecpg/compatlib/informix.c:618-643](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L618-L643)

## Overview
The dtcvasc function converts an ASCII string representation of a date/time into a timestamp value, providing Informix ESQL/C compatibility for PostgreSQL ECPG applications.

## Definition
```c
int dtcvasc(char *str, timestamp * ts)
```

## Detailed Description
This function parses a string containing a date/time representation and converts it to PostgreSQL's internal timestamp format. It serves as a compatibility wrapper for Informix applications migrating to PostgreSQL. The function uses PostgreSQL's PGTYPEStimestamp_from_asc() internally to perform the actual parsing and conversion.

The function performs validation to ensure the entire input string is consumed during parsing. If extra characters remain after a valid timestamp is parsed, it returns an error code indicating the presence of extra characters. The function aims to provide Informix-compatible error handling, though some Informix error codes are noted as missing in the implementation.

## Parameters / Member Variables
- `str`: Input string containing the ASCII representation of a date/time to be converted
- `ts`: Pointer to a timestamp variable where the converted timestamp will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPEStimestamp_from_asc](../P/PGTYPEStimestamp_from_asc.md)
  - ECPG_INFORMIX_EXTRA_CHARS
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:618-643
- Returns 0 on success, or an error code on failure
- Returns ECPG_INFORMIX_EXTRA_CHARS if extra characters exist at the end of the input string
- The implementation includes TODO comments noting that complete Informix error code mapping is not yet implemented
- Part of the Informix compatibility layer in PostgreSQL ECPG
- Uses errno to capture errors from the underlying PostgreSQL timestamp parsing function