# processEncodingEntry

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2821 - 2849

## Overview
Processes an ENCODING TOC entry by parsing the client encoding setting and configuring the archive's character encoding accordingly.

## Definition
```c
static void processEncodingEntry(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
This function handles the special processing required for ENCODING entries in the Table of Contents. It parses SQL statements of the form "SET client_encoding = 'encoding_name';" to extract the encoding name, validates it using PostgreSQL's encoding system, and sets both the archive's public encoding field and the format-specific encoding. This ensures that character data is properly handled during restore operations, maintaining encoding consistency between the original database and the restored database.

## Parameters / Member Variables
- `AH`: Archive handle that will have its encoding configuration updated
- `te`: Table of Contents entry containing the encoding definition in its defn field

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
  - [pg_strdup](pg_strdup.md) (string duplication)
  - pg_char_to_encoding (encoding name to ID conversion)
  - [setFmtEncoding](../s/setFmtEncoding.md) (format-specific encoding configuration)
  - [pg_fatal](pg_fatal.md) (error handling)
- Called from (representative examples):
  - [ReadToc](../R/ReadToc.md)

## Notes and Other Information
- This is a static function, only accessible within pg_backup_archiver.c
- Expects the TOC entry definition to be in the exact format: "SET client_encoding = 'encoding_name';"
- Uses string parsing to extract the encoding name between single quotes
- Validates the encoding name using PostgreSQL's built-in encoding system
- Terminates the entire operation with pg_fatal if the encoding is invalid or the format is malformed
- Sets both AH->public.encoding (for public API) and calls setFmtEncoding for format-specific handling
- This immediate processing during TOC reading ensures encoding is set before any data restoration begins
- The function is critical for maintaining data integrity across different character encodings
- Part of a set of special entry processors (along with processStdStringsEntry and processSearchPathEntry)