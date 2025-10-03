# get_tsearch_config_filename

## Location
[src/backend/tsearch/ts_utils.c:33-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_utils.c#L33-L67)

## Overview
Constructs the full path name for a text search configuration file given its base name and extension, with security validation to prevent pathname attacks.

## Definition

```c
char *
get_tsearch_config_filename(const char *basename,
							const char *extension)
```
## Detailed Description
This function takes a user-supplied base filename and a safe extension, validates the base name for security, and constructs the full path to a text search configuration file within the PostgreSQL installation's tsearch_data directory. The function implements strict security measures by limiting the basename to contain only lowercase letters (a-z), digits (0-9), and underscores to prevent directory traversal attacks and ensure cross-platform compatibility.

The function uses  to determine the PostgreSQL share directory path and constructs the final path as . The result is allocated using  and must be freed by the caller.

## Parameters / Member Variables
- `*basename`: User-supplied base name of the configuration file, restricted to alphanumeric characters and underscores for security
- `*extension`: File extension (assumed to be safe/validated by caller)
## Dependencies
- Functions called/Symbols referenced:
  - [get_share_path](get_share_path.md)
  - strspn (standard C library)
  - strlen (standard C library)
  - ereport (PostgreSQL error reporting)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - snprintf (standard C library)
- Called from (representative examples):
  - [dispell_init](../d/dispell_init.md) (ispell dictionary initialization)
  - [dsynonym_init](../d/dsynonym_init.md) (synonym dictionary initialization)
  - [thesaurusRead](../t/thesaurusRead.md) (thesaurus dictionary reader)
  - [readstoplist](../r/readstoplist.md) (stop list reader)

## Notes and Other Information
- The function enforces strict filename validation to prevent security vulnerabilities such as directory traversal attacks
- Uppercase letters are explicitly disallowed to avoid case-sensitivity issues across different filesystems
- Non-ASCII characters are prohibited to prevent encoding-related security risks
- The returned string is palloc'd and should be pfree'd when no longer needed
- All text search configuration files are expected to reside in the tsearch_data subdirectory of the PostgreSQL share directory