# JsonManifestWALRangeField

## Location
src/common/parse_manifest.c: 62 - 91

## Overview
JsonManifestWALRangeField is an enumerated type that identifies the different field types within a WAL range object when parsing JSON-format backup manifests in PostgreSQL.

## Definition
```c
typedef enum
{
    JMWRF_TIMELINE,
    JMWRF_START_LSN,
    JMWRF_END_LSN,
} JsonManifestWALRangeField;
```

## Detailed Description
This enumeration defines the possible fields that can appear in a WAL (Write-Ahead Log) range entry within a PostgreSQL backup manifest. The backup manifest is a JSON document that describes the contents of a backup, including files and WAL ranges. During JSON parsing of the manifest, this enum is used to track which specific field within a WAL range object is currently being processed.

The enum serves as a state indicator in the JSON parsing state machine, allowing the parser to correctly associate parsed values with their corresponding WAL range attributes. Each enum value corresponds to a specific JSON field name that appears in the manifest's WAL range objects.

## Parameters / Member Variables
- `JMWRF_TIMELINE`: Identifies the "Timeline" field, which specifies the timeline ID for the WAL range
- `JMWRF_START_LSN`: Identifies the "Start-LSN" field, which specifies the starting Log Sequence Number of the WAL range
- `JMWRF_END_LSN`: Identifies the "End-LSN" field, which specifies the ending Log Sequence Number of the WAL range

## Dependencies
- Functions called/Symbols referenced: None (this is an enum definition)
- Used by:
  - JsonManifestParseState struct (as the `wal_range_field` member)
  - json_manifest_object_field_start function (for field identification)
  - json_manifest_scalar function (for value assignment)

## Usage Context
The enum is used within the JSON manifest parsing logic in the following way:

1. **Field Identification**: When parsing a WAL range object, the parser encounters field names like "Timeline", "Start-LSN", or "End-LSN". The corresponding enum value is assigned to `parse->wal_range_field` to indicate which field is being processed.

2. **Value Assignment**: When the actual field value is parsed, the enum value is used in a switch statement to determine which parsing state variable (`timeline`, `start_lsn`, or `end_lsn`) should receive the parsed value.

## Notes and Other Information
- This enum is part of PostgreSQL's backup manifest parsing infrastructure, specifically located in `src/common/parse_manifest.c:57-62`
- The enum values directly correspond to JSON field names in backup manifests, with the mapping:
  - `JMWRF_TIMELINE` → "Timeline"
  - `JMWRF_START_LSN` → "Start-LSN"
  - `JMWRF_END_LSN` → "End-LSN"
- The enum is used internally during JSON parsing and is not exposed as part of the public API
- WAL ranges in backup manifests represent continuous segments of the write-ahead log that are associated with the backup
- This enum follows PostgreSQL's naming convention where JSON manifest field enums are prefixed with "JMWRF_" (JSON Manifest WAL Range Field)