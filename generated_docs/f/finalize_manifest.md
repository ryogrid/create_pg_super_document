# finalize_manifest

## Location
[src/bin/pg_combinebackup/write_manifest.c:142-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/write_manifest.c#L142-L194)

## Overview
Completes the backup manifest by adding WAL range information, computing the manifest checksum, and closing the JSON structure.

## Definition

```c
void
finalize_manifest(manifest_writer *mwriter,
				  manifest_wal_range *first_wal_range)
```
## Detailed Description
This function completes the backup manifest generation by finalizing the JSON structure and adding remaining metadata. It terminates the files array, adds a WAL-Ranges section containing LSN range information for each timeline, computes and embeds a SHA256 checksum of the manifest content, and closes the JSON structure. The function ensures the manifest is properly formatted and includes all necessary metadata for backup validation.

The finalization process includes:
- Closing the Files array section of the JSON
- Adding WAL-Ranges array with timeline and LSN information
- Flushing any remaining buffered data before checksum calculation
- Computing SHA256 checksum of the manifest content up to that point
- Adding the computed checksum as the final field in the JSON
- Closing the JSON structure and file

## Parameters / Member Variables
- : Manifest writer structure containing the accumulated manifest data
- : Head of linked list containing WAL range information for backup

## Dependencies
- Functions called/Symbols referenced:
  - manifest_writer (structure type)
  - manifest_wal_range (structure type)
  - PG_SHA256_DIGEST_LENGTH (checksum length constant)
  - flush_manifest (buffer flushing)
  - enlargeStringInfo (buffer management)
  - PG_SHA256_DIGEST_STRING_LENGTH (string length constant)
  - pg_checksum_final (checksum finalization)
  - hex_encode (binary to hex conversion)
  - close (file closure)
- Called from (representative examples):
  - main (in src/bin/pg_combinebackup/pg_combinebackup.c:412)

## Notes and Other Information
- The manifest checksum is computed over all content up to the checksum field itself
- WAL ranges are output with Timeline, Start-LSN, and End-LSN fields in hex format
- The still_checksumming flag is set to false after the final flush to exclude the checksum field itself from checksum calculation
- LSN values are formatted using LSN_FORMAT_ARGS macro for consistent hex representation
- The function performs a final flush and file close operation
- The resulting manifest follows PostgreSQL backup manifest specification format
- Assert is used to verify the SHA256 digest length matches expected value