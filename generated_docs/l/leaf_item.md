# leaf_item

## Location
[src/backend/lib/integerset.c:165-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L165-L166)

## Overview
The leaf_item structure is a fundamental storage unit in PostgreSQL's IntegerSet implementation that holds a cluster of compressed 64-bit integers in leaf nodes of the B-tree data structure.

## Definition


## Detailed Description
The leaf_item structure serves as a basic storage element in the leaf nodes of PostgreSQL's IntegerSet B-tree. Each leaf_item can store between 1 and 241 integers using a hybrid storage approach: the first integer is stored directly in plain format, while up to 240 additional integers are stored as differences from the first integer, compressed using the Simple-8b encoding algorithm.

This design enables efficient storage of clustered integers while maintaining the ability to perform binary searches on the first value of each item. The Simple-8b encoding is particularly effective when integers are close to each other in value, as the differences can be represented with fewer bits.

The structure is part of PostgreSQL's memory-efficient integer set implementation that can achieve as low as 0.1 bytes per integer in optimal cases (consecutive integers) and provides a worst-case usage of about 8 bytes per integer when values are more than 2^32 apart.

## Parameters / Member Variables
- : The first (base) integer stored in this leaf item, stored in uncompressed 64-bit format. This value serves as the base for calculating differences for the Simple-8b encoded values.
- : A 64-bit Simple-8b encoded value containing up to 240 additional integers stored as differences from the 'first' value. The encoding packs multiple small difference values into a single 64-bit word.

## Dependencies
- Functions called/Symbols referenced:
  - No direct function calls (this is a data structure)
- Used by (representative examples):
  - [intset_leaf_node](../i/intset_leaf_node.md) (as items array member)
  - [intset_flush_buffered_values](../i/intset_flush_buffered_values.md)
  - [intset_is_member](../i/intset_is_member.md)
  - [intset_iterate_next](../i/intset_iterate_next.md)
  - [intset_binsrch_leaf](../i/intset_binsrch_leaf.md)

## Notes and Other Information
- Each leaf_item can store a maximum of MAX_VALUES_PER_LEAF_ITEM (241) integers: 1 direct value + up to 240 Simple-8b encoded values
- The Simple-8b encoding algorithm is based on the research paper by Vo Ngoc Anh and Alistair Moffat (2010)
- The structure is designed to work efficiently with PostgreSQL's requirement that integers be added in ascending order
- Binary search is performed on the 'first' member to quickly locate the appropriate leaf_item that contains or should contain a target integer
- Memory efficiency depends heavily on the clustering of integer values - consecutive or nearby integers compress much better than scattered values
- The leaf_item is always used within the context of intset_leaf_node structures, which can contain up to MAX_LEAF_ITEMS (64) leaf_item elements