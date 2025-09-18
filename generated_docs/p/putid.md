# putid

## Location
src/backend/utils/adt/acl.c: 218 - 269

## Overview
Formats a role name for output in ACL strings, automatically adding double quotes when necessary and properly escaping embedded quotes.

## Definition


## Detailed Description
This function converts an identifier (role name) into its proper ACL string representation. It determines whether the identifier needs to be quoted based on character safety rules and handles the formatting accordingly. For identifiers containing unsafe characters (including high-bit characters), it wraps the entire identifier in double quotes.

The function implements proper quote escaping by doubling any quote characters that appear within the identifier itself. It ensures the output buffer has sufficient space (at least 2*NAMEDATALEN+2 bytes) to accommodate the worst-case scenario of a fully-quoted identifier with all characters being quotes.

The implementation must stay synchronized with dequoteAclUserName in pg_dump/dumputils.c to ensure compatibility between PostgreSQL server and dump utilities.

## Parameters / Member Variables
- : Output buffer where the formatted identifier will be written (must have at least (2*NAMEDATALEN)+2 bytes available)
- : Input identifier string to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - is_safe_acl_char (determines if characters require quoting - called with false for output context)
- Called from (representative examples):
  - aclitemout (for formatting ACL items for output)

## Notes and Other Information
The function performs a two-pass operation: first scanning to determine if quoting is needed, then formatting the output with appropriate quotes and escaping. The synchronization requirement with pg_dump ensures that ACL strings formatted by the server can be correctly parsed by dump utilities, maintaining consistency across PostgreSQL tools.