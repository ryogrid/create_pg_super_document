# PrintTOCSummary

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1281-1392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1281-L1392)

## Overview
PrintTOCSummary generates a comprehensive summary report of the archive's table of contents, including metadata about the dump session, archive format, and a detailed listing of all objects with their dependencies.

## Definition

```c
void
PrintTOCSummary(Archive *AHX)
```
## Detailed Description
PrintTOCSummary creates a human-readable summary of a PostgreSQL dump archive's contents and metadata. The function serves multiple purposes: it provides archive diagnostics information, lists all objects that would be restored based on current filter settings, and displays dependency relationships between objects when verbose mode is enabled.

The function begins by outputting archive metadata including creation timestamp, database name, total TOC entries, compression settings, format type, version information, and platform-specific details like integer and offset sizes. It then iterates through all TOC entries, applying the same selection logic used during actual restore operations to determine which objects would be processed.

For each selected object, the function outputs a formatted line containing the dump ID, catalog identifiers, object description, schema, name, and owner. In verbose mode, it additionally displays dependency information showing which other objects each entry depends upon. The output format is designed to be both human-readable and potentially machine-parseable.

The function handles output redirection, allowing the summary to be written to a specified file rather than standard output. It also enforces strict name checking if requested, ensuring that all specified object names actually exist in the archive.

## Parameters / Member Variables
- `*AHX`: Archive pointer representing the dump archive to summarize
## Dependencies
- Functions called/Symbols referenced:
  - [SaveOutput](../S/SaveOutput.md)/RestoreOutput (output redirection management)
  - [SetOutput](../S/SetOutput.md) (file output configuration)
  - strftime (timestamp formatting)
  - [ahprintf](../a/ahprintf.md) (archive-specific formatted output)
  - [sanitize_line](../s/sanitize_line.md) (name sanitization for output)
  - [get_compress_algorithm_name](../g/get_compress_algorithm_name.md) (compression algorithm display)
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

## Simplified Source

```c
void
PrintTOCSummary(Archive *AHX)
{
    ArchiveHandle *AH = (ArchiveHandle *) AHX;
    RestoreOptions *ropt = AH->public.ropt;
    TocEntry *te;
    teSection curSection;
    CompressFileHandle *sav;
    const char *fmtName;
    char stamp_str[64];

    // Set up output (uncompressed TOC)
    pg_compress_specification out_compression_spec = {0};
    out_compression_spec.algorithm = PG_COMPRESSION_NONE;

    sav = SaveOutput(AH);
    if (ropt->filename)
        SetOutput(AH, ropt->filename, out_compression_spec);

    // Format timestamp
    if (strftime(stamp_str, sizeof(stamp_str), PGDUMP_STRFTIME_FMT,
                 localtime(&AH->createDate)) == 0)
        strcpy(stamp_str, "[unknown]");

    // Print archive header information
    ahprintf(AH, ";\n; Archive created at %s\n", stamp_str);
    ahprintf(AH, ";     dbname: %s\n;     TOC Entries: %d\n;     Compression: %s\n",
             sanitize_line(AH->archdbname, false),
             AH->tocCount,
             get_compress_algorithm_name(AH->compression_spec.algorithm));

    // Determine format name
    switch (AH->format) {
        case archCustom:   fmtName = "CUSTOM"; break;
        case archDirectory: fmtName = "DIRECTORY"; break;
        case archTar:      fmtName = "TAR"; break;
        default:           fmtName = "UNKNOWN";
    }

    // Print version and format details
    ahprintf(AH, ";     Dump Version: %d.%d-%d\n",
             ARCHIVE_MAJOR(AH->version), ARCHIVE_MINOR(AH->version), ARCHIVE_REV(AH->version));
    ahprintf(AH, ";     Format: %s\n", fmtName);
    ahprintf(AH, ";     Integer: %d bytes\n", (int) AH->intSize);
    ahprintf(AH, ";     Offset: %d bytes\n", (int) AH->offSize);

    if (AH->archiveRemoteVersion)
        ahprintf(AH, ";     Dumped from database version: %s\n", AH->archiveRemoteVersion);
    if (AH->archiveDumpVersion)
        ahprintf(AH, ";     Dumped by pg_dump version: %s\n", AH->archiveDumpVersion);

    ahprintf(AH, ";\n;\n; Selected TOC Entries:\n;\n");

    // Process TOC entries
    curSection = SECTION_PRE_DATA;
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        // Update current section and check requirements
        if (te->section != SECTION_NONE)
            curSection = te->section;
        te->reqs = _tocEntryRequired(te, curSection, AH);

        // Print entry if verbose or if required
        if (ropt->verbose || (te->reqs & (REQ_SCHEMA | REQ_DATA)) != 0) {
            char *sanitized_name = sanitize_line(te->tag, false);
            char *sanitized_schema = sanitize_line(te->namespace, true);
            char *sanitized_owner = sanitize_line(te->owner, false);

            ahprintf(AH, "%d; %u %u %s %s %s %s\n", te->dumpId,
                     te->catalogId.tableoid, te->catalogId.oid,
                     te->desc, sanitized_schema, sanitized_name, sanitized_owner);

            free(sanitized_name);
            free(sanitized_schema);
            free(sanitized_owner);
        }

        // Print dependencies in verbose mode
        if (ropt->verbose && te->nDeps > 0) {
            ahprintf(AH, ";\tdepends on:");
            for (int i = 0; i < te->nDeps; i++)
                ahprintf(AH, " %d", te->dependencies[i]);
            ahprintf(AH, "\n");
        }
    }

    // Enforce strict name checking if requested
    if (ropt->strict_names)
        StrictNamesCheck(ropt);

    // Restore original output
    if (ropt->filename)
        RestoreOutput(AH, sav);
}
```