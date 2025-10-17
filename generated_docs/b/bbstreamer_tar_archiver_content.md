# bbstreamer_tar_archiver_content

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:390-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L390-L441)

## Overview
Processes input chunks to create a valid tar archive by fixing up headers, trailers, and content as needed.

## Definition
```c
static void bbstreamer_tar_archiver_content(bbstreamer *streamer,
                                          bbstreamer_member *member,
                                          const char *data, int len,
                                          bbstreamer_archive_context context)
```

## Detailed Description
This function is the core content processing logic for the tar archiver bbstreamer. It handles different types of chunks in the tar archive stream and ensures they conform to proper tar format requirements:

- **BBSTREAMER_MEMBER_HEADER**: If the header chunk is size 0, it constructs a new tar header using tarCreateHeader(). If it's TAR_BLOCK_SIZE, it passes through unchanged. Any other size is invalid.
- **BBSTREAMER_MEMBER_TRAILER**: When a header was regenerated (rearchive_member flag is true), it recalculates and generates proper padding bytes to align to tar block boundaries.
- **BBSTREAMER_MEMBER_CONTENTS**: Passed through without modification.
- **BBSTREAMER_ARCHIVE_TRAILER**: Replaced with exactly two blocks of zero bytes as required by some tar implementations.

The function maintains state through the rearchive_member flag to coordinate header and trailer regeneration.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance being processed
- `member`: Information about the current tar archive member (file metadata)
- `data`: The chunk data to process
- `len`: Length of the data chunk
- `context`: Type of chunk being processed (header, content, trailer, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - tarCreateHeader (creates tar file headers)
  - [tarPaddingBytesRequired](../t/tarPaddingBytesRequired.md) (calculates padding needed)
  - time (gets current timestamp)
  - [bbstreamer_content](bbstreamer_content.md) (forwards processed data to next streamer)
  - memset (zeroes memory for padding/trailers)
- Called from (representative examples):
  - Via bbstreamer_tar_archiver_ops.content function pointer

## Notes and Other Information
- Uses a local buffer of 2 * TAR_BLOCK_SIZE for constructing headers and trailers
- The rearchive_member flag ensures trailer padding is recalculated only when headers are regenerated
- [Archive](../A/Archive.md) trailers are always normalized to two zero blocks regardless of input
- All tar format requirements (block alignment, proper headers) are enforced
- Part of PostgreSQL's backup streaming system for creating reliable tar archives

## Simplified Source

```c
static void
bbstreamer_tar_archiver_content(bbstreamer *streamer,
                                bbstreamer_member *member,
                                const char *data, int len,
                                bbstreamer_archive_context context)
{
    bbstreamer_tar_archiver *mystreamer = (bbstreamer_tar_archiver *) streamer;
    char buffer[2 * TAR_BLOCK_SIZE];

    Assert(context != BBSTREAMER_UNKNOWN);

    // Handle header chunks
    if (context == BBSTREAMER_MEMBER_HEADER && len != TAR_BLOCK_SIZE) {
        Assert(len == 0);

        // Construct new tar header to replace zero-length header
        tarCreateHeader(buffer, member->pathname, NULL,
                       member->size, member->mode, member->uid, member->gid,
                       time(NULL));
        data = buffer;
        len = TAR_BLOCK_SIZE;

        // Mark that we need to regenerate trailer padding
        mystreamer->rearchive_member = true;
    }
    // Handle trailer chunks when header was regenerated
    else if (context == BBSTREAMER_MEMBER_TRAILER && mystreamer->rearchive_member) {
        int pad_bytes = tarPaddingBytesRequired(member->size);

        // Regenerate padding to match new header
        memset(buffer, 0, pad_bytes);
        data = buffer;
        len = pad_bytes;

        // Reset flag until next header regeneration
        mystreamer->rearchive_member = false;
    }
    // Handle archive trailer
    else if (context == BBSTREAMER_ARCHIVE_TRAILER) {
        // Always use two blocks of zero bytes for archive trailer
        memset(buffer, 0, 2 * TAR_BLOCK_SIZE);
        data = buffer;
        len = 2 * TAR_BLOCK_SIZE;
    }

    // Forward processed chunk to next bbstreamer
    bbstreamer_content(streamer->bbs_next, member, data, len, context);
}
```