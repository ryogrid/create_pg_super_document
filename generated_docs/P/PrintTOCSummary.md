# PrintTOCSummary

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1281 - 1392

## Overview
PrintTOCSummary generates a comprehensive summary report of the archive's table of contents, including metadata about the dump session, archive format, and a detailed listing of all objects with their dependencies.

## Definition


## Detailed Description
PrintTOCSummary creates a human-readable summary of a PostgreSQL dump archive's contents and metadata. The function serves multiple purposes: it provides archive diagnostics information, lists all objects that would be restored based on current filter settings, and displays dependency relationships between objects when verbose mode is enabled.

The function begins by outputting archive metadata including creation timestamp, database name, total TOC entries, compression settings, format type, version information, and platform-specific details like integer and offset sizes. It then iterates through all TOC entries, applying the same selection logic used during actual restore operations to determine which objects would be processed.

For each selected object, the function outputs a formatted line containing the dump ID, catalog identifiers, object description, schema, name, and owner. In verbose mode, it additionally displays dependency information showing which other objects each entry depends upon. The output format is designed to be both human-readable and potentially machine-parseable.

The function handles output redirection, allowing the summary to be written to a specified file rather than standard output. It also enforces strict name checking if requested, ensuring that all specified object names actually exist in the archive.

## Parameters / Member Variables
- : Archive pointer representing the dump archive to summarize

## Dependencies
- Functions called/Symbols referenced:
  - [SaveOutput](../S/SaveOutput.md)/RestoreOutput (output redirection management)
  - [SetOutput](../S/SetOutput.md) (file output configuration)
  - strftime (timestamp formatting)
  - [ahprintf](../a/ahprintf.md) (archive-specific formatted output)
  - [sanitize_line](../s/sanitize_line.md) (name sanitization for output)
  - get_compress_algorithm_name (compression algorithm display)
  - [_tocEntryRequired](../t/_tocEntryRequired.md) (object selection logic)
  - [StrictNamesCheck](../S/StrictNamesCheck.md) (name validation)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_restore)

## Notes and Other Information
- The summary output includes both archive metadata and object listings in a comment-style format
- Object selection uses the same logic as actual restore operations, ensuring accuracy of what would be restored
- Dependency information is only displayed in verbose mode to avoid overwhelming output
- Names are sanitized to handle special characters and prevent formatting issues
- The function supports both console and file output through the archive's output system
- Timestamp formatting follows the PGDUMP_STRFTIME_FMT standard for consistency
- The TOC summary is always generated uncompressed regardless of archive compression settings
- [Archive](../A/Archive.md) format detection covers all supported formats: CUSTOM, DIRECTORY, TAR, and UNKNOWN fallback