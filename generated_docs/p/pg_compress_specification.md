# pg_compress_specification

## Location
[src/include/common/compression.h:32-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/compression.h#L32-L40)

## Overview
A structure that defines compression parameters and options for PostgreSQL's compression subsystem, encompassing algorithm selection, compression level, worker configuration, and parsing state.

## Definition

```c
typedef struct pg_compress_specification
{
	pg_compress_algorithm algorithm;
	unsigned	options;		/* OR of PG_COMPRESSION_OPTION constants */
	int			level;
	int			workers;
	bool		long_distance;
	char	   *parse_error;	/* NULL if parsing was OK, else message */
} pg_compress_specification;
```
## Detailed Description
The pg_compress_specification structure serves as a comprehensive configuration container for compression operations throughout PostgreSQL. It encapsulates all necessary parameters for configuring compression algorithms including the specific algorithm to use, compression level settings, parallel worker configuration, and specialized options like long-distance matching for certain algorithms. The structure also includes error handling through a parse_error field that captures any issues encountered during specification parsing.

This structure is widely used across PostgreSQL's backup and restore subsystem, including pg_dump, pg_basebackup, and various streaming backup components. It provides a unified interface for compression configuration that can be serialized to disk and used consistently across different PostgreSQL utilities.

## Parameters / Member Variables
- `algorithm`: The compression algorithm to use (from pg_compress_algorithm enum)
- `options`: Bitfield of PG_COMPRESSION_OPTION constants controlling specific features
- `level`: Compression level setting (algorithm-specific meaning)
- `workers`: Number of worker threads to use for parallel compression
- `long_distance`: Boolean flag enabling long-distance matching optimization
- `*parse_error`: Error message string if specification parsing failed, NULL otherwise
## Dependencies
- Functions called/Symbols referenced:
  - [pg_compress_algorithm](pg_compress_algorithm.md)
  - PG_COMPRESSION_OPTION_WORKERS
  - PG_COMPRESSION_OPTION_LONG_DISTANCE

- Called from (representative examples):
  - [parse_compress_specification](parse_compress_specification.md)
  - [validate_compress_specification](../v/validate_compress_specification.md)
  - [bbsink_gzip_new](../b/bbsink_gzip_new.md)
  - [bbsink_lz4_new](../b/bbsink_lz4_new.md)
  - [bbsink_zstd_new](../b/bbsink_zstd_new.md)
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [AllocateCompressor](../A/AllocateCompressor.md)

## Notes and Other Information
This structure is designed to be persistent and may be stored to disk (as noted in the source comments). Any changes to the structure layout or field ordering must maintain backwards compatibility. The options field uses bitwise OR operations to combine multiple PG_COMPRESSION_OPTION constants. The parse_error field provides detailed error reporting for invalid compression specifications, enabling better user feedback during configuration parsing.