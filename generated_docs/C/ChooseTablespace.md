# ChooseTablespace

## Location
[src/backend/storage/file/fileset.c:186-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fileset.c#L186-L196)

## Overview
The ChooseTablespace function determines which tablespace a given temporary file should belong to by using hash-based distribution across available tablespaces.

## Definition
```c
static Oid
ChooseTablespace(const FileSet *fileset, const char *name)
```

## Detailed Description
ChooseTablespace implements a hash-based tablespace selection algorithm for temporary files within a FileSet. It computes a hash value from the file name and uses modular arithmetic to distribute files across the available tablespaces configured for the FileSet. This approach ensures relatively even distribution of temporary files across multiple tablespaces, which can help with I/O load balancing and storage utilization.

The function is static and serves as an internal utility within the fileset.c module, primarily used during file creation and path resolution operations.

## Parameters / Member Variables
- `fileset`: Pointer to the FileSet structure containing the array of available tablespaces and their count
- `name`: The name of the temporary file for which to select a tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [hash_any](../h/hash_any.md) (computes hash value from the file name string)
  - [FileSet](../F/FileSet.md) (struct type containing tablespace configuration)
- Called from (representative examples):
  - [FileSetCreate](../F/FileSetCreate.md) (when setting up initial tablespace selection)
  - [FilePath](../F/FilePath.md) (when determining the tablespace for a specific file path)

## Notes and Other Information
- This is a static function, only accessible within the fileset.c compilation unit
- The hash-based distribution provides good load balancing but is deterministic for the same file name
- The modular arithmetic ensures the result is always within the valid range of configured tablespaces
- Used as part of PostgreSQL's temporary file management system for parallel operations
- The function assumes that fileset->ntablespaces is greater than 0

## Simplified Source

```c
static Oid ChooseTablespace(const FileSet *fileset, const char *name) {
    // Hash the file name to get a distribution value
    uint32 hash = hash_any((const unsigned char *) name, strlen(name));

    // Use modular arithmetic to select from available tablespaces
    return fileset->tablespaces[hash % fileset->ntablespaces];
}
```