F.19. intarray — manipulate arrays of integers  
---  
[Prev](intagg.md "F.18. intagg — integer aggregator and enumerator") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](isn.md "F.20. isn — data types for international standard numbers \(ISBN, EAN, UPC, etc.\)")  
  
* * *

## F.19. intarray — manipulate arrays of integers #

[F.19.1. `intarray` Functions and Operators](intarray.md#INTARRAY-FUNCS-OPS)
[F.19.2. Index Support](intarray.md#INTARRAY-INDEX)
[F.19.3. Example](intarray.md#INTARRAY-EXAMPLE)
[F.19.4. Benchmark](intarray.md#INTARRAY-BENCHMARK)
[F.19.5. Authors](intarray.md#INTARRAY-AUTHORS)

The `intarray` module provides a number of useful functions and operators for manipulating null-free arrays of integers. There is also support for indexed searches using some of the operators. 

All of these operations will throw an error if a supplied array contains any NULL elements. 

Many of these operations are only sensible for one-dimensional arrays. Although they will accept input arrays of more dimensions, the data is treated as though it were a linear array in storage order. 

This module is considered “trusted”, that is, it can be installed by non-superusers who have `CREATE` privilege on the current database. 

### F.19.1. `intarray` Functions and Operators #

The functions provided by the `intarray` module are shown in [Table F.8](intarray.md#INTARRAY-FUNC-TABLE "Table F.8. intarray Functions"), the operators in [Table F.9](intarray.md#INTARRAY-OP-TABLE "Table F.9. intarray Operators"). 

**Table F.8.`intarray` Functions**

Function  Description  Example(s)   
---  
`icount` ( `integer[]` ) → `integer` Returns the number of elements in the array.  `icount('{1,2,3}'::integer[])` → `3`  
`sort` ( `integer[]`, _`dir`_ `text` ) → `integer[]` Sorts the array in either ascending or descending order. _`dir`_ must be `asc` or `desc`.  `sort('{1,3,2}'::integer[], 'desc')` → `{3,2,1}`  
`sort` ( `integer[]` ) → `integer[]` `sort_asc` ( `integer[]` ) → `integer[]` Sorts in ascending order.  `sort(array[11,77,44])` → `{11,44,77}`  
`sort_desc` ( `integer[]` ) → `integer[]` Sorts in descending order.  `sort_desc(array[11,77,44])` → `{77,44,11}`  
`uniq` ( `integer[]` ) → `integer[]` Removes adjacent duplicates. Often used with `sort` to remove all duplicates.  `uniq('{1,2,2,3,1,1}'::integer[])` → `{1,2,3,1}` `uniq(sort('{1,2,3,2,1}'::integer[]))` → `{1,2,3}`  
`idx` ( `integer[]`, _`item`_ `integer` ) → `integer` Returns index of the first array element matching _`item`_ , or 0 if no match.  `idx(array[11,22,33,22,11], 22)` → `2`  
`subarray` ( `integer[]`, _`start`_ `integer`, _`len`_ `integer` ) → `integer[]` Extracts the portion of the array starting at position _`start`_ , with _`len`_ elements.  `subarray('{1,2,3,2,1}'::integer[], 2, 3)` → `{2,3,2}`  
`subarray` ( `integer[]`, _`start`_ `integer` ) → `integer[]` Extracts the portion of the array starting at position _`start`_.  `subarray('{1,2,3,2,1}'::integer[], 2)` → `{2,3,2,1}`  
`intset` ( `integer` ) → `integer[]` Makes a single-element array.  `intset(42)` → `{42}`  
  
  


**Table F.9.`intarray` Operators**

Operator  Description   
---  
`integer[]` `&&` `integer[]` → `boolean` Do arrays overlap (have at least one element in common)?   
`integer[]` `@>` `integer[]` → `boolean` Does left array contain right array?   
`integer[]` `<@` `integer[]` → `boolean` Is left array contained in right array?   
`` `#` `integer[]` → `integer` Returns the number of elements in the array.   
`integer[]` `#` `integer` → `integer` Returns index of the first array element matching the right argument, or 0 if no match. (Same as `idx` function.)   
`integer[]` `+` `integer` → `integer[]` Adds element to end of array.   
`integer[]` `+` `integer[]` → `integer[]` Concatenates the arrays.   
`integer[]` `-` `integer` → `integer[]` Removes entries matching the right argument from the array.   
`integer[]` `-` `integer[]` → `integer[]` Removes elements of the right array from the left array.   
`integer[]` `|` `integer` → `integer[]` Computes the union of the arguments.   
`integer[]` `|` `integer[]` → `integer[]` Computes the union of the arguments.   
`integer[]` `&` `integer[]` → `integer[]` Computes the intersection of the arguments.   
`integer[]` `@@` `query_int` → `boolean` Does array satisfy query? (see below)   
`query_int` `~~` `integer[]` → `boolean` Does array satisfy query? (commutator of `@@`)   
  
  


The operators `&&`, `@>` and `<@` are equivalent to PostgreSQL's built-in operators of the same names, except that they work only on integer arrays that do not contain nulls, while the built-in operators work for any array type. This restriction makes them faster than the built-in operators in many cases. 

The `@@` and `~~` operators test whether an array satisfies a _query_ , which is expressed as a value of a specialized data type `query_int`. A _query_ consists of integer values that are checked against the elements of the array, possibly combined using the operators `&` (AND), `|` (OR), and `!` (NOT). Parentheses can be used as needed. For example, the query `1&(2|3)` matches arrays that contain 1 and also contain either 2 or 3. 

### F.19.2. Index Support #

`intarray` provides index support for the `&&`, `@>`, and `@@` operators, as well as regular array equality. 

Two parameterized GiST index operator classes are provided: `gist__int_ops` (used by default) is suitable for small- to medium-size data sets, while `gist__intbig_ops` uses a larger signature and is more suitable for indexing large data sets (i.e., columns containing a large number of distinct array values). The implementation uses an RD-tree data structure with built-in lossy compression. 

`gist__int_ops` approximates an integer set as an array of integer ranges. Its optional integer parameter `numranges` determines the maximum number of ranges in one index key. The default value of `numranges` is 100. Valid values are between 1 and 253. Using larger arrays as GiST index keys leads to a more precise search (scanning a smaller fraction of the index and fewer heap pages), at the cost of a larger index. 

`gist__intbig_ops` approximates an integer set as a bitmap signature. Its optional integer parameter `siglen` determines the signature length in bytes. The default signature length is 16 bytes. Valid values of signature length are between 1 and 2024 bytes. Longer signatures lead to a more precise search (scanning a smaller fraction of the index and fewer heap pages), at the cost of a larger index. 

There is also a non-default GIN operator class `gin__int_ops`, which supports these operators as well as `<@`. 

The choice between GiST and GIN indexing depends on the relative performance characteristics of GiST and GIN, which are discussed elsewhere. 

### F.19.3. Example #
    
    
    -- a message can be in one or more “sections”
    CREATE TABLE message (mid INT PRIMARY KEY, sections INT[], ...);
    
    -- create specialized index with signature length of 32 bytes
    CREATE INDEX message_rdtree_idx ON message USING GIST (sections gist__intbig_ops (siglen = 32));
    
    -- select messages in section 1 OR 2 - OVERLAP operator
    SELECT message.mid FROM message WHERE message.sections && '{1,2}';
    
    -- select messages in sections 1 AND 2 - CONTAINS operator
    SELECT message.mid FROM message WHERE message.sections @> '{1,2}';
    
    -- the same, using QUERY operator
    SELECT message.mid FROM message WHERE message.sections @@ '1&2'::query_int;
    

### F.19.4. Benchmark #

The source directory `contrib/intarray/bench` contains a benchmark test suite, which can be run against an installed PostgreSQL server. (It also requires `DBD::Pg` to be installed.) To run: 
    
    
    cd .../contrib/intarray/bench
    createdb TEST
    psql -c "CREATE EXTENSION intarray" TEST
    ./create_test.pl | psql TEST
    ./bench.pl
    

The `bench.pl` script has numerous options, which are displayed when it is run without any arguments. 

### F.19.5. Authors #

All work was done by Teodor Sigaev (`<[teodor@sigaev.ru](mailto:teodor@sigaev.ru)>`) and Oleg Bartunov (`<[oleg@sai.msu.su](mailto:oleg@sai.msu.su)>`). See <http://www.sai.msu.su/~megera/postgres/gist/> for additional information. Andrey Oktyabrski did a great work on adding new functions and operations. 

* * *

[Prev](intagg.md "F.18. intagg — integer aggregator and enumerator") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](isn.md "F.20. isn — data types for international standard numbers \(ISBN, EAN, UPC, etc.\)")  
---|---|---  
F.18. intagg — integer aggregator and enumerator | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.20. isn — data types for international standard numbers (ISBN, EAN, UPC, etc.)
