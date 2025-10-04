# ECPGallocate_desc

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:792-831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L792-L831)

## Overview
Allocates and initializes a new SQL descriptor with the specified name, adding it to the global descriptor list.

## Definition
```c
bool ECPGallocate_desc(int line, const char *name)
```

## Detailed Description
This function creates a new SQL descriptor structure, initializing all its fields to appropriate default values. The function allocates memory for both the descriptor structure and its name string, initializes the descriptor with an empty PostgreSQL result set, and adds it to the front of the global descriptor linked list. The function performs comprehensive error checking, ensuring that all memory allocations succeed and properly cleaning up on failure. The newly created descriptor is initialized with a count of -1 (indicating no fields have been described yet), NULL items list, and an empty PGresult structure.

## Parameters / Member Variables
- `line`: Line number for error reporting purposes in the ECPG preprocessor context
- `name`: Name to assign to the new descriptor (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca: Gets the SQLCA structure for error handling
  - [ecpg_init_sqlca](../e/ecpg_init_sqlca.md): Initializes the SQLCA structure
  - [ecpg_alloc](../e/ecpg_alloc.md): ECPG-specific memory allocation function
  - [ecpg_free](../e/ecpg_free.md): ECPG-specific memory deallocation function
  - [get_descriptors](../g/get_descriptors.md): Retrieves the current head of the descriptor list
  - [set_descriptors](../s/set_descriptors.md): Updates the head of the descriptor list
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md): Creates an empty PostgreSQL result structure
  - strcpy: Standard string copy function
  - strlen: Standard string length function
  - [ecpg_raise](../e/ecpg_raise.md): Raises ECPG errors with appropriate error codes
  - ECPG_OUT_OF_MEMORY: Error constant for memory allocation failures
  - ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY: SQL state for out-of-memory conditions

- Called from (representative examples):
  - Various test programs in src/interfaces/ecpg/test/expected/
  - ECPG-generated code for ALLOCATE DESCRIPTOR statements

## Notes and Other Information
- Returns `true` on successful allocation, `false` on error
- The new descriptor is added to the front of the global descriptor list for O(1) insertion
- Proper cleanup is performed on any allocation failure to prevent memory leaks
- The descriptor is initialized with count = -1, items = NULL, and an empty PGresult
- Error conditions include: SQLCA allocation failure, descriptor memory allocation failure, name memory allocation failure, PGresult creation failure
- This function is part of the ECPG (Embedded SQL in C) library for PostgreSQL
- Thread-safe as evidenced by usage in thread-descriptor tests
- The function is typically called by ECPG-generated code when processing ALLOCATE DESCRIPTOR SQL statements

## Simplified Source

```c
bool ECPGallocate_desc(int line, const char *name) {
    struct descriptor *new;
    struct sqlca_t *sqlca = ECPGget_sqlca();

    // Basic validation
    if (sqlca == NULL) {
        ecpg_raise(line, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    ecpg_init_sqlca(sqlca);

    // Allocate new descriptor structure
    new = (struct descriptor *)ecpg_alloc(sizeof(struct descriptor), line);
    if (!new)
        return false;

    // Allocate memory for descriptor name
    new->name = ecpg_alloc(strlen(name) + 1, line);
    if (!new->name) {
        ecpg_free(new);
        return false;
    }

    // Initialize descriptor fields
    new->next = get_descriptors();
    new->count = -1;
    new->items = NULL;

    // Create empty PostgreSQL result
    new->result = PQmakeEmptyPGresult(NULL, 0);
    if (!new->result) {
        ecpg_free(new->name);
        ecpg_free(new);
        ecpg_raise(line, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    // Set name and add to global descriptor list
    strcpy(new->name, name);
    set_descriptors(new);
    return true;
}
```