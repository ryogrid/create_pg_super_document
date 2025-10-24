# dumpProcLang

## Location
[src/bin/pg_dump/pg_dump.c:12128-12259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12128-L12259)

## Overview
The dumpProcLang function generates SQL statements to recreate a user-defined procedural language during PostgreSQL database dumps.

## Definition

```c
static void
dumpProcLang(Archive *fout, const ProcLangInfo *plang)
```
## Detailed Description
This function processes a procedural language definition and generates the appropriate CREATE PROCEDURAL LANGUAGE statement. It handles two different scenarios: when the language's support functions are available for dumping (creating a complete definition with parameters), and when they are not (creating a parameterless definition that relies on extension templates).

The function searches for the language's handler, inline, and validator functions. If all required functions are found and dumpable, it creates a complete CREATE PROCEDURAL LANGUAGE statement with all parameters. Otherwise, it creates a CREATE OR REPLACE PROCEDURAL LANGUAGE statement without parameters, which modern servers interpret as CREATE EXTENSION IF NOT EXISTS.

The function also handles dumping of associated comments, security labels, and access control lists. For trusted languages, ACL information is included in the dump.

## Parameters / Member Variables
- `*fout`: Archive handle for the dump output stream
- `*plang`: ProcLangInfo structure containing metadata about the procedural language
## Dependencies
- Functions called/Symbols referenced:
  - [findFuncByOid](../f/findFuncByOid.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Returns early if dataOnly dump mode is specified since languages are schema constructs
- Attempts to locate handler, inline, and validator functions by OID
- Uses parameterless CREATE OR REPLACE when support functions are not dumpable
- Includes TRUSTED keyword when the language is marked as trusted
- ACL dumping is conditional on the language being trusted
- Archived in SECTION_PRE_DATA to ensure proper dependency ordering
- In binary upgrade mode, handles extension membership properly
- Modern servers interpret parameterless commands as extension creation

## Simplified Source

```c
static void
dumpProcLang(Archive *fout, const ProcLangInfo *plang)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer defqry, delqry;
    bool useParams;
    char *qlanname;
    FuncInfo *funcInfo, *inlineInfo = NULL, *validatorInfo = NULL;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    // Find support functions for the language
    funcInfo = findFuncByOid(plang->lanplcallfoid);
    if (funcInfo != NULL && !funcInfo->dobj.dump)
        funcInfo = NULL;

    if (OidIsValid(plang->laninline)) {
        inlineInfo = findFuncByOid(plang->laninline);
        if (inlineInfo != NULL && !inlineInfo->dobj.dump)
            inlineInfo = NULL;
    }

    if (OidIsValid(plang->lanvalidator)) {
        validatorInfo = findFuncByOid(plang->lanvalidator);
        if (validatorInfo != NULL && !validatorInfo->dobj.dump)
            validatorInfo = NULL;
    }

    // Determine if we can create complete definition with parameters
    useParams = (funcInfo != NULL &&
                 (inlineInfo != NULL || !OidIsValid(plang->laninline)) &&
                 (validatorInfo != NULL || !OidIsValid(plang->lanvalidator)));

    defqry = createPQExpBuffer();
    delqry = createPQExpBuffer();
    qlanname = pg_strdup(fmtId(plang->dobj.name));

    // Build DROP statement
    appendPQExpBuffer(delqry, "DROP PROCEDURAL LANGUAGE %s;\n", qlanname);

    // Build CREATE statement - with or without parameters
    if (useParams) {
        // Complete definition with handler, inline, and validator functions
        appendPQExpBuffer(defqry, "CREATE %sPROCEDURAL LANGUAGE %s",
                          plang->lanpltrusted ? "TRUSTED " : "", qlanname);
        appendPQExpBuffer(defqry, " HANDLER %s", fmtQualifiedDumpable(funcInfo));

        if (OidIsValid(plang->laninline))
            appendPQExpBuffer(defqry, " INLINE %s", fmtQualifiedDumpable(inlineInfo));
        if (OidIsValid(plang->lanvalidator))
            appendPQExpBuffer(defqry, " VALIDATOR %s", fmtQualifiedDumpable(validatorInfo));
    } else {
        // Parameterless definition - relies on extension template
        appendPQExpBuffer(defqry, "CREATE OR REPLACE PROCEDURAL LANGUAGE %s", qlanname);
    }
    appendPQExpBufferStr(defqry, ";\n");

    // Handle binary upgrade extension membership
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(defqry, &plang->dobj, "LANGUAGE", qlanname, NULL);

    // Archive the language definition
    if (plang->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, plang->dobj.catId, plang->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = plang->dobj.name,
                                  .owner = plang->lanowner,
                                  .description = "PROCEDURAL LANGUAGE",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = defqry->data,
                                  .dropStmt = delqry->data));

    // Dump associated metadata
    if (plang->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "LANGUAGE", qlanname, NULL, plang->lanowner,
                    plang->dobj.catId, 0, plang->dobj.dumpId);

    if (plang->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "LANGUAGE", qlanname, NULL, plang->lanowner,
                     plang->dobj.catId, 0, plang->dobj.dumpId);

    if (plang->lanpltrusted && plang->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, plang->dobj.dumpId, InvalidDumpId, "LANGUAGE",
                qlanname, NULL, NULL, NULL, plang->lanowner, &plang->dacl);

    // Cleanup
    free(qlanname);
    destroyPQExpBuffer(defqry);
    destroyPQExpBuffer(delqry);
}
```