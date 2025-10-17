# _tarPositionTo

## Location
[src/bin/pg_dump/pg_backup_tar.c:1066-1139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L1066-L1139)

## Overview
A static function that locates a specific file within a TAR archive, reads its header, and positions the file pointer to the beginning of the file data.

## Definition
```c
static TAR_MEMBER *_tarPositionTo(ArchiveHandle *AH, const char *filename)
```

## Detailed Description
This function performs TAR archive navigation to locate and position to a specific file or the next available file. It handles sequential scanning through TAR members, validating each header and skipping files until the target is found. When a filename is specified, it searches for an exact match; when filename is NULL, it returns the next available member. The function enforces data restoration order requirements and calculates proper positioning including TAR block padding. It maintains archive position tracking and sets up the TAR_MEMBER structure for subsequent read operations.

## Parameters / Member Variables
- `AH`: Archive handle containing the TAR file stream and context information
- `filename`: Target filename to locate, or NULL to get the next available member

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0_object (zero-initialized memory allocation)
  - [_tarReadRaw](_tarReadRaw.md) (raw TAR archive reading)
  - [_tarGetHeader](_tarGetHeader.md) (TAR header parsing)
  - [TocIDRequired](../T/TocIDRequired.md) (checks data requirements)
  - [tarPaddingBytesRequired](tarPaddingBytesRequired.md) (calculates block padding)
  - pg_log_debug (debug logging)
  - [lclContext](../l/lclContext.md) (local context struct type)
  - [TAR_MEMBER](../T/TAR_MEMBER.md) (TAR member struct type)
  - TAR_BLOCK_SIZE/REQ_DATA (constants)
- Called from (representative examples):
  - [tarOpen](tarOpen.md) (during archive opening/reading)

## Notes and Other Information
- Returns a newly allocated TAR_MEMBER structure on success, NULL if no more members
- Enforces sequential data restoration order - prevents out-of-order access
- Handles TAR block alignment by skipping padding bytes between members  
- Updates archive position tracking for efficient sequential access
- Provides detailed debug logging for archive navigation operations
- Supports both targeted file search and sequential scanning modes
- Located in src/bin/pg_dump/pg_backup_tar.c:1066-1139

## Simplified Source

```c
static TAR_MEMBER *_tarPositionTo(ArchiveHandle *AH, const char *filename) {
    lclContext *ctx = (lclContext *) AH->formatData;
    TAR_MEMBER *tar_member = pg_malloc0_object(TAR_MEMBER);
    char header[TAR_BLOCK_SIZE];
    size_t blocks_to_skip;

    tar_member->AH = AH;

    // Move to end of current file if we're in the middle of one
    if (ctx->tarFHpos != 0) {
        char byte;
        while (ctx->tarFHpos < ctx->tarNextMember)
            _tarReadRaw(AH, &byte, 1, NULL, ctx->tarFH);
    }

    // Read the next TAR header
    if (!_tarGetHeader(AH, tar_member)) {
        if (filename)
            pg_fatal("could not find header for file \"%s\" in tar archive", filename);
        else {
            // Just scanning - no more files
            free(tar_member);
            return NULL;
        }
    }

    // Skip files until we find the target (if specified)
    while (filename != NULL && strcmp(tar_member->targetFile, filename) != 0) {
        // Check data restoration order requirements
        int file_id = atoi(tar_member->targetFile);
        if ((TocIDRequired(AH, file_id) & REQ_DATA) != 0)
            pg_fatal("restoring data out of order is not supported");

        // Skip this file's data and padding
        size_t file_length = tar_member->fileLen;
        size_t total_length = file_length + tarPaddingBytesRequired(file_length);
        blocks_to_skip = total_length / TAR_BLOCK_SIZE;

        for (size_t i = 0; i < blocks_to_skip; i++)
            _tarReadRaw(AH, header, TAR_BLOCK_SIZE, NULL, ctx->tarFH);

        // Read next header
        if (!_tarGetHeader(AH, tar_member))
            pg_fatal("could not find header for file \"%s\" in tar archive", filename);
    }

    // Calculate position of next member for future navigation
    ctx->tarNextMember = ctx->tarFHpos + tar_member->fileLen +
                         tarPaddingBytesRequired(tar_member->fileLen);
    tar_member->pos = 0;

    return tar_member;
}
```