# var_list

## Location
src/interfaces/ecpg/ecpglib/ecpglib_extern.h: 155 - 241

## Overview
The `var_list` struct represents a simple linked list node used for managing numbered items with generic pointers in the ECPG library.

## Definition
```c
struct var_list
{
    int         number;
    void       *pointer;
    struct var_list *next;
};
```

## Detailed Description
This structure provides a lightweight linked list implementation for managing collections of numbered items with associated pointers. It serves as a general-purpose container within the ECPG library, allowing for dynamic management of various types of objects that need to be identified by number and accessed through generic pointers. The simplicity of the structure makes it suitable for various internal bookkeeping tasks where a flexible, dynamically-sized collection is needed.

## Parameters / Member Variables
- `number`: Integer identifier or sequence number for this list item
- `pointer`: Generic void pointer that can reference any type of data or object
- `next`: Pointer to the next var_list structure in the linked list, enabling traversal

## Dependencies
- Functions called/Symbols referenced:
  - var_list (self-reference for linked list structure)
  - Multiple ECPG library functions and types are referenced in the broader context
- Called from (representative examples):
  - ecpg_finish (cleanup operations)
  - ECPGset_var (variable setting operations)
  - ECPGget_var (variable retrieval operations)
  - ecpg_gettext (localization support)

## Notes and Other Information
This structure appears in the context of a larger header file that contains extensive function declarations and type definitions for the ECPG library. The var_list serves as a fundamental building block for various internal data management tasks within ECPG, providing a flexible way to maintain ordered collections of diverse objects. Its generic nature allows it to be repurposed for different use cases throughout the library without requiring specialized data structures.