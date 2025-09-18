# dumpPolicy

## Location
src/bin/pg_dump/pg_dump.c: 4117 - 4234

## Overview
Generates the SQL statements to recreate a Row-Level Security policy or RLS enablement for a table during pg_dump restore operations.

## Definition


## Detailed Description
The `dumpPolicy` function creates the SQL DDL statements needed to recreate security policies during database restoration. It handles two distinct cases:

1. **RLS Enablement**: When `polinfo->polname` is NULL, it generates an "ALTER TABLE ... ENABLE ROW LEVEL SECURITY" statement to enable RLS on the table.

2. **Actual Policies**: For named policies, it constructs a "CREATE POLICY" statement with all the policy attributes including:
   - Policy name and target table
   - Command type (SELECT, INSERT, UPDATE, DELETE, or ALL)
   - Restrictive vs. permissive nature
   - Target roles
   - USING qualifier expression
   - WITH CHECK expression

The function also generates the corresponding DROP statement for cleanup during restoration and handles comments and security labels if present.

## Parameters / Member Variables
- `fout`: Archive pointer for output operations and dump options
- `polinfo`: PolicyInfo structure containing policy details including:
  - `polname`: Policy name (NULL for RLS enablement)
  - `poltable`: Associated TableInfo structure
  - `polcmd`: Policy command type ('*', 'r', 'a', 'w', 'd')
  - `polpermissive`: Whether policy is permissive (vs restrictive)
  - `polroles`: Target roles for the policy
  - `polqual`: USING clause expression
  - `polwithcheck`: WITH CHECK clause expression

## Dependencies
- Functions called/Symbols referenced:
  - `DumpOptions`, `TableInfo` (data structures)
  - `createPQExpBuffer`, `appendPQExpBuffer` series (query building)
  - `fmtQualifiedDumpable`, `fmtId` (identifier formatting)
  - [ArchiveEntry](../A/ArchiveEntry.md) (archive entry creation)
  - [dumpComment](dumpComment.md), `dumpSecLabel` (auxiliary object dumping)
  - `DUMP_COMPONENT_DEFINITION`, `SECTION_POST_DATA` (component flags)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (main dump dispatch function)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- Maps policy command characters to SQL keywords: 'r'→SELECT, 'a'→INSERT, 'w'→UPDATE, 'd'→DELETE, '*'→ALL
- Creates archive entries in SECTION_POST_DATA to ensure policies are created after tables
- Handles both restrictive (default) and permissive policies (PostgreSQL 10+)
- Generates qualified table names to handle cross-schema references correctly
- Part of the comprehensive database object recreation system in pg_dump/pg_restore