# _policyInfo

## Location
src/bin/pg_dump/pg_dump.h: 617 - 626

## Overview
The `_policyInfo` structure represents row-level security (RLS) policies on tables and indicates whether RLS is enabled, used by pg_dump to preserve security policy configurations during database operations.

## Definition
```c
typedef struct _policyInfo
{
    DumpableObject dobj;
    TableInfo  *poltable;
    char       *polname;        /* null indicates RLS is enabled on rel */
    char        polcmd;
    bool        polpermissive;
    char       *polroles;
    char       *polqual;
    char       *polwithcheck;
} PolicyInfo;
```

## Detailed Description
This structure is part of pg_dump's internal representation for PostgreSQL's Row-Level Security (RLS) feature. It serves a dual purpose: when `polname` is NULL, the record indicates that RLS is enabled on the table without defining a specific policy; when `polname` is non-NULL, it represents an actual security policy definition. RLS policies allow fine-grained access control by filtering rows based on the current user and specified conditions. The structure captures all necessary metadata to recreate both the RLS enablement and individual policy definitions during database restoration.

## Parameters / Member Variables
- `dobj`: Base dumpable object information containing catalog ID, name, and dump ordering details
- `poltable`: Pointer to the TableInfo structure representing the table this policy applies to
- `polname`: Name of the security policy; NULL indicates this record represents "ENABLE ROW SECURITY" rather than a specific policy
- `polcmd`: Single character indicating the command type this policy applies to ('r' for SELECT, 'a' for INSERT, 'w' for UPDATE, 'd' for DELETE, '*' for ALL)
- `polpermissive`: Boolean flag indicating whether this is a permissive policy (true) or restrictive policy (false)
- `polroles`: Comma-separated string of role names to which this policy applies
- `polqual`: SQL expression defining the policy's row filtering condition (USING clause)
- `polwithcheck`: SQL expression for the policy's row modification check condition (WITH CHECK clause)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - TableInfo
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This structure is defined in pg_dump.h as part of the pg_dump utility's internal data structures
- The typedef creates an alias `PolicyInfo` for easier reference throughout the codebase
- Row-Level Security is a PostgreSQL feature that provides fine-grained access control at the row level
- The dual-purpose design (RLS enablement vs. policy definition) is indicated by the `polname` field being NULL or non-NULL
- Policy commands use single character codes: 'r' (SELECT), 'a' (INSERT), 'w' (UPDATE), 'd' (DELETE), '*' (ALL)
- Permissive policies are additive (allow access if any permissive policy passes), while restrictive policies are subtractive (deny access if any restrictive policy fails)
- The structure enables pg_dump to preserve complex RLS configurations across database migrations and backups