F.22. ltree — hierarchical tree-like data type  
---  
[Prev](lo.md "F.21. lo — manage large objects") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](pageinspect.md "F.23. pageinspect — low-level inspection of database pages")  
  
* * *

## F.22. ltree — hierarchical tree-like data type #

[F.22.1. Definitions](ltree.md#LTREE-DEFINITIONS)
[F.22.2. Operators and Functions](ltree.md#LTREE-OPS-FUNCS)
[F.22.3. Indexes](ltree.md#LTREE-INDEXES)
[F.22.4. Example](ltree.md#LTREE-EXAMPLE)
[F.22.5. Transforms](ltree.md#LTREE-TRANSFORMS)
[F.22.6. Authors](ltree.md#LTREE-AUTHORS)

This module implements a data type `ltree` for representing labels of data stored in a hierarchical tree-like structure. Extensive facilities for searching through label trees are provided. 

This module is considered “trusted”, that is, it can be installed by non-superusers who have `CREATE` privilege on the current database. 

### F.22.1. Definitions #

A _label_ is a sequence of alphanumeric characters, underscores, and hyphens. Valid alphanumeric character ranges are dependent on the database locale. For example, in C locale, the characters `A-Za-z0-9_-` are allowed. Labels must be no more than 1000 characters long. 

Examples: `42`, `Personal_Services`

A _label path_ is a sequence of zero or more labels separated by dots, for example `L1.L2.L3`, representing a path from the root of a hierarchical tree to a particular node. The length of a label path cannot exceed 65535 labels. 

Example: `Top.Countries.Europe.Russia`

The `ltree` module provides several data types: 

  * `ltree` stores a label path. 

  * `lquery` represents a regular-expression-like pattern for matching `ltree` values. A simple word matches that label within a path. A star symbol (`*`) matches zero or more labels. These can be joined with dots to form a pattern that must match the whole label path. For example: 
        
        foo         _Match the exact label pathfoo_
        *.foo.*     _Match any label path containing the labelfoo_
        *.foo       _Match any label path whose last label isfoo_
        

Both star symbols and simple words can be quantified to restrict how many labels they can match: 
        
        *{_n_}        _Match exactly _n_ labels_
        *{_n_ ,}       _Match at least _n_ labels_
        *{_n_ ,_m_}      _Match at least _n_ but not more than _m_ labels_
        *{,_m_}       _Match at most _m_ labels — same as _*{0,_m_}
        foo{_n_ ,_m_}    _Match at least _n_ but not more than _m_ occurrences of foo_
        foo{,}      _Match any number of occurrences offoo, including zero_
        

In the absence of any explicit quantifier, the default for a star symbol is to match any number of labels (that is, `{,}`) while the default for a non-star item is to match exactly once (that is, `{1}`). 

There are several modifiers that can be put at the end of a non-star `lquery` item to make it match more than just the exact match: 
        
        @           _Match case-insensitively, for examplea@ matches A_
        *           _Match any label with this prefix, for examplefoo* matches foobar_
        %           _Match initial underscore-separated words_
        

The behavior of `%` is a bit complicated. It tries to match words rather than the entire label. For example `foo_bar%` matches `foo_bar_baz` but not `foo_barbaz`. If combined with `*`, prefix matching applies to each word separately, for example `foo_bar%*` matches `foo1_bar2_baz` but not `foo1_br2_baz`. 

Also, you can write several possibly-modified non-star items separated with `|` (OR) to match any of those items, and you can put `!` (NOT) at the start of a non-star group to match any label that doesn't match any of the alternatives. A quantifier, if any, goes at the end of the group; it means some number of matches for the group as a whole (that is, some number of labels matching or not matching any of the alternatives). 

Here's an annotated example of `lquery`: 
        
        Top.*{0,2}.sport*@.!football|tennis{1,}.Russ*|Spain
        a.  b.     c.      d.                   e.
        

This query will match any label path that: 

    1. begins with the label `Top`

    2. and next has zero to two labels before 

    3. a label beginning with the case-insensitive prefix `sport`

    4. then has one or more labels, none of which match `football` nor `tennis`

    5. and then ends with a label beginning with `Russ` or exactly matching `Spain`. 

  * `ltxtquery` represents a full-text-search-like pattern for matching `ltree` values. An `ltxtquery` value contains words, possibly with the modifiers `@`, `*`, `%` at the end; the modifiers have the same meanings as in `lquery`. Words can be combined with `&` (AND), `|` (OR), `!` (NOT), and parentheses. The key difference from `lquery` is that `ltxtquery` matches words without regard to their position in the label path. 

Here's an example `ltxtquery`: 
        
        Europe & Russia*@ & !Transportation
        

This will match paths that contain the label `Europe` and any label beginning with `Russia` (case-insensitive), but not paths containing the label `Transportation`. The location of these words within the path is not important. Also, when `%` is used, the word can be matched to any underscore-separated word within a label, regardless of position. 




Note: `ltxtquery` allows whitespace between symbols, but `ltree` and `lquery` do not. 

### F.22.2. Operators and Functions #

Type `ltree` has the usual comparison operators `=`, `<>`, `<`, `>`, `<=`, `>=`. Comparison sorts in the order of a tree traversal, with the children of a node sorted by label text. In addition, the specialized operators shown in [Table F.12](ltree.md#LTREE-OP-TABLE "Table F.12. ltree Operators") are available. 

**Table F.12.`ltree` Operators**

Operator  Description   
---  
`ltree` `@>` `ltree` → `boolean` Is left argument an ancestor of right (or equal)?   
`ltree` `<@` `ltree` → `boolean` Is left argument a descendant of right (or equal)?   
`ltree` `~` `lquery` → `boolean` `lquery` `~` `ltree` → `boolean` Does `ltree` match `lquery`?   
`ltree` `?` `lquery[]` → `boolean` `lquery[]` `?` `ltree` → `boolean` Does `ltree` match any `lquery` in array?   
`ltree` `@` `ltxtquery` → `boolean` `ltxtquery` `@` `ltree` → `boolean` Does `ltree` match `ltxtquery`?   
`ltree` `||` `ltree` → `ltree` Concatenates `ltree` paths.   
`ltree` `||` `text` → `ltree` `text` `||` `ltree` → `ltree` Converts text to `ltree` and concatenates.   
`ltree[]` `@>` `ltree` → `boolean` `ltree` `<@` `ltree[]` → `boolean` Does array contain an ancestor of `ltree`?   
`ltree[]` `<@` `ltree` → `boolean` `ltree` `@>` `ltree[]` → `boolean` Does array contain a descendant of `ltree`?   
`ltree[]` `~` `lquery` → `boolean` `lquery` `~` `ltree[]` → `boolean` Does array contain any path matching `lquery`?   
`ltree[]` `?` `lquery[]` → `boolean` `lquery[]` `?` `ltree[]` → `boolean` Does `ltree` array contain any path matching any `lquery`?   
`ltree[]` `@` `ltxtquery` → `boolean` `ltxtquery` `@` `ltree[]` → `boolean` Does array contain any path matching `ltxtquery`?   
`ltree[]` `?@>` `ltree` → `ltree` Returns first array entry that is an ancestor of `ltree`, or `NULL` if none.   
`ltree[]` `?<@` `ltree` → `ltree` Returns first array entry that is a descendant of `ltree`, or `NULL` if none.   
`ltree[]` `?~` `lquery` → `ltree` Returns first array entry that matches `lquery`, or `NULL` if none.   
`ltree[]` `?@` `ltxtquery` → `ltree` Returns first array entry that matches `ltxtquery`, or `NULL` if none.   
  
  


The operators `<@`, `@>`, `@` and `~` have analogues `^<@`, `^@>`, `^@`, `^~`, which are the same except they do not use indexes. These are useful only for testing purposes. 

The available functions are shown in [Table F.13](ltree.md#LTREE-FUNC-TABLE "Table F.13. ltree Functions"). 

**Table F.13.`ltree` Functions**

Function  Description  Example(s)   
---  
`subltree` ( `ltree`, _`start`_ `integer`, _`end`_ `integer` ) → `ltree` Returns subpath of `ltree` from position _`start`_ to position _`end`_ -1 (counting from 0).  `subltree('Top.Child1.Child2', 1, 2)` → `Child1`  
`subpath` ( `ltree`, _`offset`_ `integer`, _`len`_ `integer` ) → `ltree` Returns subpath of `ltree` starting at position _`offset`_ , with length _`len`_. If _`offset`_ is negative, subpath starts that far from the end of the path. If _`len`_ is negative, leaves that many labels off the end of the path.  `subpath('Top.Child1.Child2', 0, 2)` → `Top.Child1`  
`subpath` ( `ltree`, _`offset`_ `integer` ) → `ltree` Returns subpath of `ltree` starting at position _`offset`_ , extending to end of path. If _`offset`_ is negative, subpath starts that far from the end of the path.  `subpath('Top.Child1.Child2', 1)` → `Child1.Child2`  
`nlevel` ( `ltree` ) → `integer` Returns number of labels in path.  `nlevel('Top.Child1.Child2')` → `3`  
`index` ( _`a`_ `ltree`, _`b`_ `ltree` ) → `integer` Returns position of first occurrence of _`b`_ in _`a`_ , or -1 if not found.  `index('0.1.2.3.5.4.5.6.8.5.6.8', '5.6')` → `6`  
`index` ( _`a`_ `ltree`, _`b`_ `ltree`, _`offset`_ `integer` ) → `integer` Returns position of first occurrence of _`b`_ in _`a`_ , or -1 if not found. The search starts at position _`offset`_ ; negative _`offset`_ means start _`-offset`_ labels from the end of the path.  `index('0.1.2.3.5.4.5.6.8.5.6.8', '5.6', -4)` → `9`  
`text2ltree` ( `text` ) → `ltree` Casts `text` to `ltree`.   
`ltree2text` ( `ltree` ) → `text` Casts `ltree` to `text`.   
`lca` ( `ltree` [, `ltree` [, ... ]] ) → `ltree` Computes longest common ancestor of paths (up to 8 arguments are supported).  `lca('1.2.3', '1.2.3.4.5.6')` → `1.2`  
`lca` ( `ltree[]` ) → `ltree` Computes longest common ancestor of paths in array.  `lca(array['1.2.3'::ltree,'1.2.3.4'])` → `1.2`  
  
  


### F.22.3. Indexes #

`ltree` supports several types of indexes that can speed up the indicated operators: 

  * B-tree index over `ltree`: `<`, `<=`, `=`, `>=`, `>`

  * Hash index over `ltree`: `=`

  * GiST index over `ltree` (`gist_ltree_ops` opclass): `<`, `<=`, `=`, `>=`, `>`, `@>`, `<@`, `@`, `~`, `?`

`gist_ltree_ops` GiST opclass approximates a set of path labels as a bitmap signature. Its optional integer parameter `siglen` determines the signature length in bytes. The default signature length is 8 bytes. The length must be a positive multiple of `int` alignment (4 bytes on most machines)) up to 2024. Longer signatures lead to a more precise search (scanning a smaller fraction of the index and fewer heap pages), at the cost of a larger index. 

Example of creating such an index with the default signature length of 8 bytes: 
        
        CREATE INDEX path_gist_idx ON test USING GIST (path);
        

Example of creating such an index with a signature length of 100 bytes: 
        
        CREATE INDEX path_gist_idx ON test USING GIST (path gist_ltree_ops(siglen=100));
        

  * GiST index over `ltree[]` (`gist__ltree_ops` opclass): `ltree[] <@ ltree`, `ltree @> ltree[]`, `@`, `~`, `?`

`gist__ltree_ops` GiST opclass works similarly to `gist_ltree_ops` and also takes signature length as a parameter. The default value of `siglen` in `gist__ltree_ops` is 28 bytes. 

Example of creating such an index with the default signature length of 28 bytes: 
        
        CREATE INDEX path_gist_idx ON test USING GIST (array_path);
        

Example of creating such an index with a signature length of 100 bytes: 
        
        CREATE INDEX path_gist_idx ON test USING GIST (array_path gist__ltree_ops(siglen=100));
        

Note: This index type is lossy. 




### F.22.4. Example #

This example uses the following data (also available in file `contrib/ltree/ltreetest.sql` in the source distribution): 
    
    
    CREATE TABLE test (path ltree);
    INSERT INTO test VALUES ('Top');
    INSERT INTO test VALUES ('Top.Science');
    INSERT INTO test VALUES ('Top.Science.Astronomy');
    INSERT INTO test VALUES ('Top.Science.Astronomy.Astrophysics');
    INSERT INTO test VALUES ('Top.Science.Astronomy.Cosmology');
    INSERT INTO test VALUES ('Top.Hobbies');
    INSERT INTO test VALUES ('Top.Hobbies.Amateurs_Astronomy');
    INSERT INTO test VALUES ('Top.Collections');
    INSERT INTO test VALUES ('Top.Collections.Pictures');
    INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy');
    INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy.Stars');
    INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy.Galaxies');
    INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy.Astronauts');
    CREATE INDEX path_gist_idx ON test USING GIST (path);
    CREATE INDEX path_idx ON test USING BTREE (path);
    CREATE INDEX path_hash_idx ON test USING HASH (path);
    

Now, we have a table `test` populated with data describing the hierarchy shown below: 
    
    
                            Top
                         /   |  \
                 Science Hobbies Collections
                     /       |              \
            Astronomy   Amateurs_Astronomy Pictures
               /  \                            |
    Astrophysics  Cosmology                Astronomy
                                            /  |    \
                                     Galaxies Stars Astronauts
    

We can do inheritance: 
    
    
    ltreetest=> SELECT path FROM test WHERE path <@ 'Top.Science';
                    path
    ------------------------------------
     Top.Science
     Top.Science.Astronomy
     Top.Science.Astronomy.Astrophysics
     Top.Science.Astronomy.Cosmology
    (4 rows)
    

Here are some examples of path matching: 
    
    
    ltreetest=> SELECT path FROM test WHERE path ~ '*.Astronomy.*';
                         path
    -----------------------------------------------
     Top.Science.Astronomy
     Top.Science.Astronomy.Astrophysics
     Top.Science.Astronomy.Cosmology
     Top.Collections.Pictures.Astronomy
     Top.Collections.Pictures.Astronomy.Stars
     Top.Collections.Pictures.Astronomy.Galaxies
     Top.Collections.Pictures.Astronomy.Astronauts
    (7 rows)
    
    ltreetest=> SELECT path FROM test WHERE path ~ '*.!pictures@.Astronomy.*';
                    path
    ------------------------------------
     Top.Science.Astronomy
     Top.Science.Astronomy.Astrophysics
     Top.Science.Astronomy.Cosmology
    (3 rows)
    

Here are some examples of full text search: 
    
    
    ltreetest=> SELECT path FROM test WHERE path @ 'Astro*% & !pictures@';
                    path
    ------------------------------------
     Top.Science.Astronomy
     Top.Science.Astronomy.Astrophysics
     Top.Science.Astronomy.Cosmology
     Top.Hobbies.Amateurs_Astronomy
    (4 rows)
    
    ltreetest=> SELECT path FROM test WHERE path @ 'Astro* & !pictures@';
                    path
    ------------------------------------
     Top.Science.Astronomy
     Top.Science.Astronomy.Astrophysics
     Top.Science.Astronomy.Cosmology
    (3 rows)
    

Path construction using functions: 
    
    
    ltreetest=> SELECT subpath(path,0,2)||'Space'||subpath(path,2) FROM test WHERE path <@ 'Top.Science.Astronomy';
                     ?column?
    ------------------------------------------
     Top.Science.Space.Astronomy
     Top.Science.Space.Astronomy.Astrophysics
     Top.Science.Space.Astronomy.Cosmology
    (3 rows)
    

We could simplify this by creating an SQL function that inserts a label at a specified position in a path: 
    
    
    CREATE FUNCTION ins_label(ltree, int, text) RETURNS ltree
        AS 'select subpath($1,0,$2) || $3 || subpath($1,$2);'
        LANGUAGE SQL IMMUTABLE;
    
    ltreetest=> SELECT ins_label(path,2,'Space') FROM test WHERE path <@ 'Top.Science.Astronomy';
                    ins_label
    ------------------------------------------
     Top.Science.Space.Astronomy
     Top.Science.Space.Astronomy.Astrophysics
     Top.Science.Space.Astronomy.Cosmology
    (3 rows)
    

### F.22.5. Transforms #

The `ltree_plpython3u` extension implements transforms for the `ltree` type for PL/Python. If installed and specified when creating a function, `ltree` values are mapped to Python lists. (The reverse is currently not supported, however.) 

### Caution

It is strongly recommended that the transform extension be installed in the same schema as `ltree`. Otherwise there are installation-time security hazards if a transform extension's schema contains objects defined by a hostile user. 

### F.22.6. Authors #

All work was done by Teodor Sigaev (`<[teodor@stack.net](mailto:teodor@stack.net)>`) and Oleg Bartunov (`<[oleg@sai.msu.su](mailto:oleg@sai.msu.su)>`). See <http://www.sai.msu.su/~megera/postgres/gist/> for additional information. Authors would like to thank Eugeny Rodichev for helpful discussions. Comments and bug reports are welcome. 

* * *

[Prev](lo.md "F.21. lo — manage large objects") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](pageinspect.md "F.23. pageinspect — low-level inspection of database pages")  
---|---|---  
F.21. lo — manage large objects | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.23. pageinspect — low-level inspection of database pages
