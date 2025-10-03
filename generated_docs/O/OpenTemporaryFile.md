# OpenTemporaryFile

## Location
[src/backend/storage/file/fd.c:1721-1775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1721-L1775)

## Overview
OpenTemporaryFile creates a temporary file that automatically disappears when closed, with intelligent tablespace selection and resource management integration for PostgreSQL's temporary file system.

## Definition

```c
File
OpenTemporaryFile(bool interXact)
```
## Detailed Description
OpenTemporaryFile is the primary interface for creating temporary files in PostgreSQL. It handles automatic temporary filename generation, tablespace selection logic, and resource ownership management. The function first attempts to use configured temporary tablespaces, falling back to the database's default tablespace if necessary. For files that outlive the current transaction (interXact=true), it forces placement in the default tablespace to avoid conflicts with tablespace drop operations. The function integrates with PostgreSQL's resource management system by registering non-interXact files with the current resource owner, ensuring automatic cleanup at transaction end. All temporary files are marked for deletion when closed and are subject to temporary file size limits.

## Parameters / Member Variables
- `interXact`: If true, the file outlives the current transaction and won't be registered with the resource owner; if false, the file is tied to the current transaction lifecycle
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [GetNextTempTableSpace](../G/GetNextTempTableSpace.md)
  - [OpenTemporaryFileInTablespace](OpenTemporaryFileInTablespace.md)
  - [RegisterTemporaryFile](../R/RegisterTemporaryFile.md)
  - Assert
  - OidIsValid
- Called from (representative examples):
  - [extendBufFile](../e/extendBufFile.md)
  - [BufFileCreateTemp](../B/BufFileCreateTemp.md)

## Notes and Other Information
This function is part of PostgreSQL's temporary file management system in src/backend/storage/file/fd.c. It implements sophisticated tablespace management logic, preferring user-configured temporary tablespaces for transaction-scoped files while ensuring long-lived files don't interfere with tablespace administration. The function requires that temporary_files_allowed is true before proceeding. All temporary files created are automatically marked with FD_DELETE_AT_CLOSE and FD_TEMP_FILE_LIMIT flags. The resource owner integration ensures that transaction-scoped temporary files are automatically cleaned up even if not explicitly closed, preventing resource leaks during error conditions.

## Simplified Source

```c
File
OpenTemporaryFile(bool interXact)
{
    File file = 0;

    // Ensure temporary files are allowed
    Assert(temporary_files_allowed);

    // Prepare resource owner for file registration (if not interXact)
    if (!interXact)
        ResourceOwnerEnlarge(CurrentResourceOwner);

    // Try to use configured temporary tablespaces first
    if (numTempTableSpaces > 0 && !interXact) {
        Oid tblspcOid = GetNextTempTableSpace();
        if (OidIsValid(tblspcOid))
            file = OpenTemporaryFileInTablespace(tblspcOid, false);
    }

    // Fall back to database default tablespace if needed
    if (file <= 0) {
        Oid defaultTablespace = MyDatabaseTableSpace ?
                                MyDatabaseTableSpace : DEFAULTTABLESPACE_OID;
        file = OpenTemporaryFileInTablespace(defaultTablespace, true);
    }

    // Mark file for deletion at close and set size limits
    VfdCache[file].fdstate |= FD_DELETE_AT_CLOSE | FD_TEMP_FILE_LIMIT;

    // Register with resource owner for automatic cleanup (if not interXact)
    if (!interXact)
        RegisterTemporaryFile(file);

    return file;
}
```