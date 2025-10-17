# _discoverArchiveFormat

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2221-2354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2221-L2354)

## Overview
_discoverArchiveFormat is a static function that automatically detects the format of a PostgreSQL archive by examining its contents and structure.

## Definition
```c
static int _discoverArchiveFormat(ArchiveHandle *AH)
```

## Detailed Description
This function performs archive format detection by examining file signatures, directory structures, and header patterns. It supports detection of custom format archives (PGDMP signature), directory format archives (by checking for toc.dat files), tar format archives (by validating tar headers), and identifies text format dumps to provide appropriate error messages. The function sets up a lookahead buffer to cache initial file content for subsequent processing and handles various compression formats including gzip, LZ4, and Zstandard.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer - the archive handle containing file specification and format information to be determined

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug
  - [pg_malloc0](../p/pg_malloc0.md)
  - [_fileExistsInDirectory](../f/_fileExistsInDirectory.md)
  - [isValidTarHeader](../i/isValidTarHeader.md)
  - S_ISDIR
  - archDirectory, archCustom, archTar (format constants)
  - TEXT_DUMP_HEADER, TEXT_DUMPALL_HEADER (header constants)
  - PG_BINARY_R, READ_ERROR_EXIT (I/O macros)
- Called from (representative examples):
  - [_allocAH](../a/_allocAH.md)

## Notes and Other Information
- Static function, only accessible within pg_backup_archiver.c
- Sets up a 512-byte lookahead buffer that can be used by subsequent operations
- Handles both file-based and stdin input sources
- Supports detection of compressed TOC files in directory format (.gz, .lz4, .zst)
- Provides specific error messages for text format dumps suggesting use of psql
- Critical for proper initialization of archive handlers in pg_restore operations
- Returns format identifier that determines which archive-specific functions are used

## Simplified Source

```c
static int
_discoverArchiveFormat(ArchiveHandle *AH)
{
    FILE *fh;
    char sig[6];
    size_t cnt;
    int wantClose = 0;

    pg_log_debug("attempting to ascertain archive format");

    // Initialize lookahead buffer
    free(AH->lookahead);
    AH->readHeader = 0;
    AH->lookaheadSize = 512;
    AH->lookahead = pg_malloc0(512);
    AH->lookaheadLen = 0;
    AH->lookaheadPos = 0;

    if (AH->fSpec)
    {
        struct stat st;
        wantClose = 1;

        // Check if it's a directory (directory format)
        if (stat(AH->fSpec, &st) == 0 && S_ISDIR(st.st_mode))
        {
            AH->format = archDirectory;
            if (_fileExistsInDirectory(AH->fSpec, "toc.dat") ||
                _fileExistsInDirectory(AH->fSpec, "toc.dat.gz") ||
                _fileExistsInDirectory(AH->fSpec, "toc.dat.lz4") ||
                _fileExistsInDirectory(AH->fSpec, "toc.dat.zst"))
                return AH->format;

            pg_fatal("directory \"%s\" does not appear to be a valid archive", AH->fSpec);
        }
        else
        {
            fh = fopen(AH->fSpec, PG_BINARY_R);
            if (!fh)
                pg_fatal("could not open input file \"%s\": %m", AH->fSpec);
        }
    }
    else
    {
        fh = stdin;
        if (!fh)
            pg_fatal("could not open input file: %m");
    }

    // Read first 5 bytes to check signature
    if ((cnt = fread(sig, 1, 5, fh)) != 5)
        pg_fatal("input file is too short (read %lu, expected 5)", (unsigned long) cnt);

    // Save to lookahead buffer
    memcpy(&AH->lookahead[0], sig, 5);
    AH->lookaheadLen = 5;

    if (strncmp(sig, "PGDMP", 5) == 0)
    {
        // Custom format detected
        AH->format = archCustom;
        AH->readHeader = 1;
    }
    else
    {
        // Read more data to distinguish tar vs text
        cnt = fread(&AH->lookahead[AH->lookaheadLen], 1, 512 - AH->lookaheadLen, fh);
        AH->lookaheadLen += cnt;

        if (AH->lookaheadLen >= strlen(TEXT_DUMPALL_HEADER) &&
            (strncmp(AH->lookahead, TEXT_DUMP_HEADER, strlen(TEXT_DUMP_HEADER)) == 0 ||
             strncmp(AH->lookahead, TEXT_DUMPALL_HEADER, strlen(TEXT_DUMPALL_HEADER)) == 0))
        {
            pg_fatal("input file appears to be a text format dump. Please use psql.");
        }

        if (AH->lookaheadLen != 512)
            pg_fatal("input file does not appear to be a valid archive");

        if (!isValidTarHeader(AH->lookahead))
            pg_fatal("input file does not appear to be a valid archive");

        AH->format = archTar;
    }

    // Clean up file handle
    if (wantClose)
    {
        if (fclose(fh) != 0)
            pg_fatal("could not close input file: %m");
        AH->readHeader = 0;
        AH->lookaheadLen = 0;
    }

    return AH->format;
}
```