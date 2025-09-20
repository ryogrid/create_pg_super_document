# _Zstd_CCtx_setParam_or_die

## Location
[src/bin/pg_dump/compress_zstd.c:60-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L60-L72)

## Overview
A static utility function that safely sets compression parameters for a ZSTD compression context, terminating the program with a fatal error if the parameter setting fails.

## Definition

```c
static void
_Zstd_CCtx_setParam_or_die(ZSTD_CStream *cstream,
						   ZSTD_cParameter param, int value, char *paramname)
```
## Detailed Description
This function provides a wrapper around the ZSTD library's  function with error handling. It attempts to set a compression parameter on the given ZSTD compression stream and calls  to terminate the program if the operation fails. The function is designed to be used during initialization of ZSTD compression contexts where parameter setting failures are considered unrecoverable errors.

## Parameters / Member Variables
- : Pointer to the ZSTD compression stream context
- : The ZSTD compression parameter type to set
- : The integer value to assign to the parameter
- : Human-readable name of the parameter for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_CCtx_setParameter (from ZSTD library)
  - ZSTD_isError (from ZSTD library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
  - ZSTD_getErrorName (from ZSTD library)
- Called from (representative examples):
  - [_ZstdCStreamParams](_ZstdCStreamParams.md)

## Notes and Other Information
- This is a static function internal to the compress_zstd.c module
- Uses PostgreSQL's  function which terminates the program on error
- Provides descriptive error messages including the parameter name and ZSTD error description
- Part of PostgreSQL's pg_dump utility's ZSTD compression support