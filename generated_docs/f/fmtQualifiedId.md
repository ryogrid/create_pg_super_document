# fmtQualifiedId

## Location
[src/fe_utils/string_utils.c:296-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L296-L312)

## Overview
A convenience wrapper function that constructs a schema-qualified identifier name using the currently configured global encoding setting.

## Definition
```c
const char *fmtQualifiedId(const char *schema, const char *id)
```

## Detailed Description
The `fmtQualifiedId` function is a simplified interface to `fmtQualifiedIdEnc` that uses the globally configured encoding setting obtained via `getFmtEncoding()`. This function provides backward compatibility and convenience for code that doesnt need to specify encoding explicitly. It delegates all the actual formatting work to `fmtQualifiedIdEnc`, making it essentially a thin wrapper.

The function assumes that `setFmtEncoding()` has been previously called to configure the appropriate encoding for the current context. For new code, it is recommended to use `fmtQualifiedIdEnc()` directly with an explicit encoding parameter for better clarity and control.

## Parameters / Member Variables
- `schema`: Schema name to prepend to the identifier (can be NULL or empty)
- `id`: The identifier name to be formatted (required)

## Dependencies
- Functions called/Symbols referenced:
  - [fmtQualifiedIdEnc](fmtQualifiedIdEnc.md) (main implementation)
  - [getFmtEncoding](../g/getFmtEncoding.md) (to obtain current encoding setting)
- Called from (representative examples):
  - [lockTableForWorker](../l/lockTableForWorker.md) (src/bin/pg_dump/parallel.c:1313)
  - [restore_toc_entry](../r/restore_toc_entry.md) (src/bin/pg_dump/pg_backup_archiver.c:1020)
  - [_disableTriggersIfNecessary](../d/_disableTriggersIfNecessary.md) (src/bin/pg_dump/pg_backup_archiver.c:1129)
  - `fmtQualifiedDumpable` (src/bin/pg_dump/pg_dump.c:178)

## Notes and Other Information
- This is a convenience wrapper around `fmtQualifiedIdEnc`
- Requires prior call to `setFmtEncoding()` to configure encoding
- Recommended to use `fmtQualifiedIdEnc()` directly in new code for explicit encoding control
- Widely used in pg_dump utilities for database object name formatting
- Inherits all behavior and limitations from `fmtQualifiedIdEnc`
- [Result](../R/Result.md) should be used immediately before making other formatting function calls

## Simplified Source

```c
const char *fmtQualifiedId(const char *schema, const char *id) {
    // Use the globally configured encoding
    return fmtQualifiedIdEnc(schema, id, getFmtEncoding());
}
```