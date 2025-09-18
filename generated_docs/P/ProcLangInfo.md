# ProcLangInfo

## Location
src/bin/pg_dump/pg_dump.h: 505 - 506

## Overview
ProcLangInfo is a structure used by pg_dump to store metadata about procedural languages during the dump and restore process.

## Definition
```c
typedef struct _procLangInfo
{
    DumpableObject dobj;
    DumpableAcl dacl;
    bool        lanpltrusted;
    Oid         lanplcallfoid;
    Oid         laninline;
    Oid         lanvalidator;
    const char *lanowner;
} ProcLangInfo;
```

## Detailed Description
ProcLangInfo represents procedural language metadata in PostgreSQL's pg_dump utility. It extends the base DumpableObject structure and includes DumpableAcl for access control information. This structure stores all the necessary information about procedural languages (like PL/pgSQL, PL/Python, etc.) including their handler functions, validation functions, and security properties necessary for recreating the language during restore.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common dump metadata (OID, name, etc.)
- `dacl`: DumpableAcl structure containing access control list information for the language
- `lanpltrusted`: Boolean flag indicating whether the language is trusted (safe for non-superusers)
- `lanplcallfoid`: OID of the language's call handler function
- `laninline`: OID of the language's inline handler function (for anonymous code blocks)
- `lanvalidator`: OID of the language's validator function (for syntax checking)
- `lanowner`: String containing the name of the language owner

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - DumpableAcl (for access control information)
- Called from (representative examples):
  - [selectDumpableProcLang](../s/selectDumpableProcLang.md) (src/bin/pg_dump/pg_dump.c:2001)
  - [getProcLangs](../g/getProcLangs.md) (src/bin/pg_dump/pg_dump.c:8514, 8542)
  - [dumpProcLang](../d/dumpProcLang.md) (src/bin/pg_dump/pg_dump.c:12128)
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:240)

## Notes and Other Information
- This structure handles the complete metadata for procedural languages including their associated functions
- The lanpltrusted field is critical for security - trusted languages can be used by non-superusers
- Handler functions (lanplcallfoid) are required for all procedural languages to execute code
- Inline handlers (laninline) are optional and used for anonymous DO blocks
- Validator functions (lanvalidator) are optional and used for syntax validation when functions are created
- The structure includes ACL information since languages can have specific access permissions