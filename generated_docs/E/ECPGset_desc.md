# ECPGset_desc

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:605-727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L605-L727)

## Overview
ECPGset_desc sets descriptor item attributes by processing variable arguments containing descriptor type and data pairs, managing the creation and modification of descriptor items.

## Definition

```c
struct descriptor *desc;
```
## Detailed Description
ECPGset_desc is a variadic function that modifies or creates descriptor items within a named descriptor. It processes variable arguments consisting of descriptor type/variable pairs to set various attributes of a specific descriptor item identified by index. The function handles dynamic creation of descriptor items if they don't exist and supports setting multiple attributes including data, indicator, length, precision, scale, and type.

The function operates by:
1. Finding the named descriptor using ecpg_find_desc
2. Locating or creating the descriptor item for the specified index
3. Processing variable argument pairs of descriptor types and variables
4. Setting the appropriate attribute based on the descriptor type
5. Managing memory allocation and cleanup throughout the process

For data items, it calls ecpg_store_input to convert and format the input data, then uses set_desc_attr to configure binary/non-binary attributes. For numeric attributes (indicator, length, precision, scale, type), it uses set_int_item to store integer values.

## Parameters / Member Variables
- : Source code line number for error reporting and debugging
- : Name of the descriptor to modify
- : Index of the descriptor item to set (creates if doesn't exist)
- : Variable arguments consisting of ECPGdtype/variable pairs terminated by ECPGd_EODT

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_find_desc](../e/ecpg_find_desc.md)
  - [ecpg_alloc](../e/ecpg_alloc.md), ecpg_free
  - [ecpg_store_input](../e/ecpg_store_input.md)
  - [set_desc_attr](../s/set_desc_attr.md), set_int_item
  - [ecpg_raise](../e/ecpg_raise.md)
  - [descriptor](../d/descriptor.md), descriptor_item (struct types)
  - ECPGdtype, ECPGttype (enum types)
- Called from (representative examples):
  - ECPG test programs (sql-desc.c, sql-bytea.c)
  - SQL descriptor manipulation applications

## Notes and Other Information
- Supports descriptor types: ECPGd_data, ECPGd_indicator, ECPGd_length, ECPGd_precision, ECPGd_scale, ECPGd_type
- Creates descriptor items dynamically if they don't exist for the specified index
- Updates descriptor count when creating items with higher indices
- Manages memory for both descriptor items and temporary variable structures
- Returns false on any error condition with appropriate SQLSTATE codes
- Handles both fixed-size and dynamic arrays through arrsize/varcharsize parameters
- Critical for implementing SQL SET DESCRIPTOR functionality in ECPG applications

## Simplified Source

```c
bool ECPGset_desc(int lineno, const char *desc_name, int index, ...) {
    va_list args;
    struct descriptor *desc;
    struct descriptor_item *desc_item;
    struct variable *var;

    // Find the named descriptor
    desc = ecpg_find_desc(lineno, desc_name);
    if (desc == NULL)
        return false;

    // Find existing descriptor item or create new one
    for (desc_item = desc->items; desc_item; desc_item = desc_item->next) {
        if (desc_item->num == index)
            break;
    }

    if (desc_item == NULL) {
        // Create new descriptor item
        desc_item = (struct descriptor_item *)ecpg_alloc(sizeof(*desc_item), lineno);
        if (!desc_item)
            return false;

        desc_item->num = index;
        if (desc->count < index)
            desc->count = index;

        // Insert at head of list
        desc_item->next = desc->items;
        desc->items = desc_item;
    }

    // Allocate temporary variable structure
    if (!(var = (struct variable *)ecpg_alloc(sizeof(struct variable), lineno)))
        return false;

    va_start(args, index);

    // Process variable arguments until ECPGd_EODT
    for (;;) {
        enum ECPGdtype itemtype;
        char *tobeinserted = NULL;

        itemtype = va_arg(args, enum ECPGdtype);
        if (itemtype == ECPGd_EODT)
            break;

        // Extract variable information from arguments
        var->type = va_arg(args, enum ECPGttype);
        var->pointer = va_arg(args, char *);
        var->varcharsize = va_arg(args, long);
        var->arrsize = va_arg(args, long);
        var->offset = va_arg(args, long);

        // Set value pointer based on array configuration
        if (var->arrsize == 0 || var->varcharsize == 0)
            var->value = *((char **)(var->pointer));
        else
            var->value = var->pointer;

        // Handle negative values (reset to zero)
        if (var->arrsize < 0)
            var->arrsize = 0;
        if (var->varcharsize < 0)
            var->varcharsize = 0;

        var->next = NULL;

        switch (itemtype) {
            case ECPGd_data:
                // Store input data and set descriptor attributes
                if (!ecpg_store_input(lineno, true, var, &tobeinserted, false)) {
                    ecpg_free(var);
                    va_end(args);
                    return false;
                }
                set_desc_attr(desc_item, var, tobeinserted);
                tobeinserted = NULL;
                break;

            case ECPGd_indicator:
                // Set indicator value
                set_int_item(lineno, &desc_item->indicator, var->pointer, var->type);
                break;

            case ECPGd_length:
                // Set length value
                set_int_item(lineno, &desc_item->length, var->pointer, var->type);
                break;

            case ECPGd_precision:
                // Set precision value
                set_int_item(lineno, &desc_item->precision, var->pointer, var->type);
                break;

            case ECPGd_scale:
                // Set scale value
                set_int_item(lineno, &desc_item->scale, var->pointer, var->type);
                break;

            case ECPGd_type:
                // Set type value
                set_int_item(lineno, &desc_item->type, var->pointer, var->type);
                break;

            default:
                // Unknown descriptor item type
                ecpg_raise(lineno, ECPG_UNKNOWN_DESCRIPTOR_ITEM, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR, NULL);
                ecpg_free(var);
                va_end(args);
                return false;
        }
    }

    ecpg_free(var);
    va_end(args);
    return true;
}
```