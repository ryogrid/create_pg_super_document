# bbstreamer_recovery_injector_content

## Location
[src/bin/pg_basebackup/bbstreamer_inject.c:85-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_inject.c#L85-L199)

## Overview
Handles each chunk of tar content while injecting recovery configuration, managing file filtering and content modification based on the archive context.

## Definition

```c
static void
bbstreamer_recovery_injector_content(bbstreamer *streamer,
									 bbstreamer_member *member,
									 const char *data, int len,
									 bbstreamer_archive_context context)
```
## Detailed Description
This function processes archive stream chunks and selectively modifies them to inject recovery configuration. It operates differently based on the archive context and recovery GUC support:

**BBSTREAMER_MEMBER_HEADER context:**
- Copies member data for potential modification
- For modern PostgreSQL (recovery GUCs supported): skips standby.signal files and modifies postgresql.auto.conf by increasing its size to accommodate injected content
- For legacy PostgreSQL: skips recovery.conf files
- Invalidates archive headers when content will be modified

**BBSTREAMER_MEMBER_CONTENTS and BBSTREAMER_MEMBER_TRAILER contexts:**
- Skips forwarding data for files marked to be skipped
- Appends recovery configuration content to postgresql.auto.conf files during trailer processing

**BBSTREAMER_ARCHIVE_TRAILER context:**
- For modern PostgreSQL: creates postgresql.auto.conf if not found, and injects empty standby.signal file
- For legacy PostgreSQL: injects recovery.conf with specified contents

The function ensures proper recovery configuration injection while maintaining archive stream integrity.

## Parameters / Member Variables
- `*streamer`: The bbstreamer instance (cast to bbstreamer_recovery_injector)
- `*member`: Archive member information (NULL for trailer context)
- `*data`: Chunk data to process
- `len`: Length of the data chunk
- `context`: Current archive context (header, contents, trailer, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - memcpy
  - strcmp
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_inject_file](bbstreamer_inject_file.md)
  - [pg_fatal](../p/pg_fatal.md)
  - bbstreamer_member (struct type)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (enum)
  - BBSTREAMER_* constants
- Called from (representative examples):
  - No direct references found (likely called via function pointer in operations table)

## Notes and Other Information
- Static function used as part of the bbstreamer_recovery_injector operations table
- Handles complex logic for different PostgreSQL versions (legacy vs. recovery GUC support)
- Modifies archive headers when injecting content, requiring subsequent bbstreamers to regenerate them
- Critical for maintaining archive integrity while injecting recovery configuration files
- Located in src/bin/pg_basebackup/bbstreamer_inject.c:85-199

## Simplified Source

```c
static void
bbstreamer_recovery_injector_content(bbstreamer *streamer,
                                   bbstreamer_member *member,
                                   const char *data, int len,
                                   bbstreamer_archive_context context)
{
    bbstreamer_recovery_injector *mystreamer;

    mystreamer = (bbstreamer_recovery_injector *) streamer;

    switch (context)
    {
        case BBSTREAMER_MEMBER_HEADER:
            // Copy member info for potential modification
            memcpy(&mystreamer->member, member, sizeof(bbstreamer_member));

            if (mystreamer->is_recovery_guc_supported)
            {
                // For modern PostgreSQL: skip standby.signal, modify postgresql.auto.conf
                mystreamer->skip_file = (strcmp(member->pathname, "standby.signal") == 0);
                mystreamer->is_postgresql_auto_conf = (strcmp(member->pathname, "postgresql.auto.conf") == 0);

                if (mystreamer->is_postgresql_auto_conf)
                {
                    mystreamer->found_postgresql_auto_conf = true;
                    // Increase file size to accommodate injected content
                    mystreamer->member.size += mystreamer->recoveryconfcontents->len;
                    data = NULL; // Invalidate header for regeneration
                    len = 0;
                }
            }
            else
            {
                // For legacy PostgreSQL: skip recovery.conf
                mystreamer->skip_file = (strcmp(member->pathname, "recovery.conf") == 0);
            }

            if (mystreamer->skip_file)
                return;
            break;

        case BBSTREAMER_MEMBER_CONTENTS:
        case BBSTREAMER_MEMBER_TRAILER:
            if (mystreamer->skip_file)
                return;

            // Append recovery config to postgresql.auto.conf
            if (context == BBSTREAMER_MEMBER_TRAILER && mystreamer->is_postgresql_auto_conf)
                bbstreamer_content(mystreamer->base.bbs_next, member,
                                 mystreamer->recoveryconfcontents->data,
                                 mystreamer->recoveryconfcontents->len,
                                 BBSTREAMER_MEMBER_CONTENTS);
            break;

        case BBSTREAMER_ARCHIVE_TRAILER:
            if (mystreamer->is_recovery_guc_supported)
            {
                // Inject postgresql.auto.conf if not found
                if (!mystreamer->found_postgresql_auto_conf)
                    bbstreamer_inject_file(mystreamer->base.bbs_next,
                                         "postgresql.auto.conf",
                                         mystreamer->recoveryconfcontents->data,
                                         mystreamer->recoveryconfcontents->len);

                // Inject empty standby.signal file
                bbstreamer_inject_file(mystreamer->base.bbs_next, "standby.signal", "", 0);
            }
            else
            {
                // Inject recovery.conf for legacy PostgreSQL
                bbstreamer_inject_file(mystreamer->base.bbs_next,
                                     "recovery.conf",
                                     mystreamer->recoveryconfcontents->data,
                                     mystreamer->recoveryconfcontents->len);
            }
            break;

        default:
            pg_fatal("unexpected state while injecting recovery settings");
    }

    // Forward data to next streamer
    bbstreamer_content(mystreamer->base.bbs_next, &mystreamer->member, data, len, context);
}
```