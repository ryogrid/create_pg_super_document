# strlcat

## Location
[src/port/strlcat.c:33-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/strlcat.c#L33-L60)

## Overview
`strlcat` is a safe string concatenation function that appends the source string to the destination string while ensuring buffer bounds safety and null termination.

## Definition
```c
size_t strlcat(char *dst, const char *src, size_t siz);
```

## Detailed Description
`strlcat` is a safer alternative to the standard C library function `strncat`. It appends the string pointed to by `src` to the end of the string pointed to by `dst`, but unlike `strncat`, the `siz` parameter represents the full size of the destination buffer, not just the space left. This design makes it easier to use correctly and avoid buffer overflows.

The function always ensures null termination of the result (unless `siz <= strlen(dst)`, in which case no concatenation occurs). It copies at most `siz-1` characters total in the destination buffer. The function is particularly useful in PostgreSQL for building strings safely without risking buffer overflows.

The return value indicates whether truncation occurred: if the return value is greater than or equal to `siz`, then truncation happened.

## Parameters / Member Variables
- `dst`: Pointer to the destination string buffer where the source will be appended
- `src`: Pointer to the source string to be appended to the destination
- `siz`: Total size of the destination buffer (including space for null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - `strlen` (standard C library function)
- Called from (representative examples):
  - `CreateLockFile` (src/backend/utils/init/miscinit.c:1442)
  - `CreateBackupStreamer` (src/bin/pg_basebackup/pg_basebackup.c:1195, 1201, 1208)
  - `_PrepParallelRestore` (src/bin/pg_dump/pg_backup_directory.c:774, 776, 778)
  - `MAX_PROMPT_SIZE` (src/bin/psql/prompt.c:334)
  - `get_configdata` (src/common/config_info.c:121)
  - `pqsecure_raw_write` (src/interfaces/libpq/fe-secure.c:408, 423)

## Notes and Other Information
- This is a portability function located in `src/port/strlcat.c`, providing the `strlcat` function for systems that do not have it natively
- The function is conditionally declared in `src/include/port.h` only if `HAVE_DECL_STRLCAT` is not defined
- Unlike `strncat`, which takes the number of characters to append, `strlcat` takes the full buffer size, making it less error-prone
- The function returns the total length that would have been created if there was unlimited space, allowing detection of truncation
- Originally developed by Todd C. Miller and Theo de Raadt for OpenBSD
- The implementation handles edge cases carefully, including when the destination buffer is already full or when no space is available for concatenation