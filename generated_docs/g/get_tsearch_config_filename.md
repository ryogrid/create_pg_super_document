# get_tsearch_config_filename

## Location
src/backend/tsearch/ts_utils.c: 33 - 67

## Overview
Constructs the full path name for a text search configuration file given its base name and extension, with security validation to prevent pathname attacks.

## Definition


## Detailed Description
This function takes a user-supplied base filename and a safe extension, validates the base name for security, and constructs the full path to a text search configuration file within the PostgreSQL installation's tsearch_data directory. The function implements strict security measures by limiting the basename to contain only lowercase letters (a-z), digits (0-9), and underscores to prevent directory traversal attacks and ensure cross-platform compatibility.

The function uses  to determine the PostgreSQL share directory path and constructs the final path as . The result is allocated using  and must be freed by the caller.

## Parameters / Member Variables
- : User-supplied base name of the configuration file, restricted to alphanumeric characters and underscores for security
- : File extension (assumed to be safe/validated by caller)

## Dependencies
- Functions called/Symbols referenced:
  - get_share_path
  - strspn (standard C library)
  - strlen (standard C library)
  - ereport (PostgreSQL error reporting)
  - palloc (PostgreSQL memory allocation)
  - snprintf (standard C library)
- Called from (representative examples):
  - dispell_init (ispell dictionary initialization)
  - dsynonym_init (synonym dictionary initialization)
  - thesaurusRead (thesaurus dictionary reader)
  - readstoplist (stop list reader)

## Notes and Other Information
- The function enforces strict filename validation to prevent security vulnerabilities such as directory traversal attacks
- Uppercase letters are explicitly disallowed to avoid case-sensitivity issues across different filesystems
- Non-ASCII characters are prohibited to prevent encoding-related security risks
- The returned string is palloc'd and should be pfree'd when no longer needed
- All text search configuration files are expected to reside in the tsearch_data subdirectory of the PostgreSQL share directory