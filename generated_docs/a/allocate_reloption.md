# allocate_reloption

## Location
[src/backend/access/common/reloptions.c:775-831](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L775-L831)

## Overview
Allocates a new reloption structure and initializes the type-agnostic fields for various reloption types (excluding string-specific initialization).

## Definition

```c
enum);
```
## Detailed Description
This static function is responsible for allocating memory for a new reloption structure based on the specified type. It handles memory context switching for non-local reloptions to ensure they are allocated in TopMemoryContext for persistence. The function determines the appropriate structure size based on the reloption type (bool, int, real, enum, or string) and initializes common fields like name, description, kinds, type, and lock mode requirements.

## Parameters / Member Variables
- : A bits32 value specifying the kinds of relations this option applies to
- : Integer constant specifying the reloption type (RELOPT_TYPE_BOOL, RELOPT_TYPE_INT, etc.)
- : String name of the reloption
- : Optional description string for the reloption (can be NULL)
- : The lock mode required when setting this reloption

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - strlen (string length calculation)
  - elog (error logging)
- Called from (representative examples):
  - [init_bool_reloption](../i/init_bool_reloption.md)
  - [init_int_reloption](../i/init_int_reloption.md)
  - [init_real_reloption](../i/init_real_reloption.md)
  - [init_enum_reloption](../i/init_enum_reloption.md)
  - [init_string_reloption](../i/init_string_reloption.md)

## Notes and Other Information
- This is a static function, only accessible within the reloptions.c file
- For non-local reloptions, memory is allocated in TopMemoryContext to ensure persistence
- The function supports all standard reloption types and will error on unsupported types
- Memory context is properly restored after allocation for non-local reloptions
- The function duplicates name and description strings to ensure they persist independently

## Simplified Source

```c
static relopt_gen *allocate_reloption(bits32 kinds, int type, const char *name,
                                     const char *desc, LOCKMODE lockmode) {
    MemoryContext oldcxt;
    size_t size;
    relopt_gen *newoption;

    // Switch to TopMemoryContext for non-local options
    if (kinds != RELOPT_KIND_LOCAL) {
        oldcxt = MemoryContextSwitchTo(TopMemoryContext);
    } else {
        oldcxt = NULL;
    }

    // Determine structure size based on option type
    switch (type) {
        case RELOPT_TYPE_BOOL:   size = sizeof(relopt_bool); break;
        case RELOPT_TYPE_INT:    size = sizeof(relopt_int); break;
        case RELOPT_TYPE_REAL:   size = sizeof(relopt_real); break;
        case RELOPT_TYPE_ENUM:   size = sizeof(relopt_enum); break;
        case RELOPT_TYPE_STRING: size = sizeof(relopt_string); break;
        default:
            elog(ERROR, "unsupported reloption type %d", type);
            return NULL;
    }

    // Allocate and initialize the option structure
    newoption = palloc(size);
    newoption->name = pstrdup(name);
    newoption->desc = desc ? pstrdup(desc) : NULL;
    newoption->kinds = kinds;
    newoption->namelen = strlen(name);
    newoption->type = type;
    newoption->lockmode = lockmode;

    // Restore previous memory context
    if (oldcxt != NULL) {
        MemoryContextSwitchTo(oldcxt);
    }

    return newoption;
}
```