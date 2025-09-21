64.5. BRIN Indexes  
---  
[Prev](gin.md "64.4. GIN Indexes") | [Up](indextypes.md "Chapter 64. Built-in Index Access Methods")| Chapter 64. Built-in Index Access Methods| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](hash-index.md "64.6. Hash Indexes")  
  
* * *

## 64.5. BRIN Indexes #

[64.5.1. Introduction](brin.md#BRIN-INTRO)
[64.5.2. Built-in Operator Classes](brin.md#BRIN-BUILTIN-OPCLASSES)
[64.5.3. Extensibility](brin.md#BRIN-EXTENSIBILITY)

### 64.5.1. Introduction #

BRIN stands for Block Range Index. BRIN is designed for handling very large tables in which certain columns have some natural correlation with their physical location within the table. 

BRIN works in terms of _block ranges_ (or “page ranges”). A block range is a group of pages that are physically adjacent in the table; for each block range, some summary info is stored by the index. For example, a table storing a store's sale orders might have a date column on which each order was placed, and most of the time the entries for earlier orders will appear earlier in the table as well; a table storing a ZIP code column might have all codes for a city grouped together naturally. 

BRIN indexes can satisfy queries via regular bitmap index scans, and will return all tuples in all pages within each range if the summary info stored by the index is _consistent_ with the query conditions. The query executor is in charge of rechecking these tuples and discarding those that do not match the query conditions — in other words, these indexes are lossy. Because a BRIN index is very small, scanning the index adds little overhead compared to a sequential scan, but may avoid scanning large parts of the table that are known not to contain matching tuples. 

The specific data that a BRIN index will store, as well as the specific queries that the index will be able to satisfy, depend on the operator class selected for each column of the index. Data types having a linear sort order can have operator classes that store the minimum and maximum value within each block range, for instance; geometrical types might store the bounding box for all the objects in the block range. 

The size of the block range is determined at index creation time by the `pages_per_range` storage parameter. The number of index entries will be equal to the size of the relation in pages divided by the selected value for `pages_per_range`. Therefore, the smaller the number, the larger the index becomes (because of the need to store more index entries), but at the same time the summary data stored can be more precise and more data blocks can be skipped during an index scan. 

#### 64.5.1.1. Index Maintenance #

At the time of creation, all existing heap pages are scanned and a summary index tuple is created for each range, including the possibly-incomplete range at the end. As new pages are filled with data, page ranges that are already summarized will cause the summary information to be updated with data from the new tuples. When a new page is created that does not fall within the last summarized range, the range that the new page belongs to does not automatically acquire a summary tuple; those tuples remain unsummarized until a summarization run is invoked later, creating the initial summary for that range. 

There are several ways to trigger the initial summarization of a page range. If the table is vacuumed, either manually or by [autovacuum](routine-vacuuming.md#AUTOVACUUM "24.1.6. The Autovacuum Daemon"), all existing unsummarized page ranges are summarized. Also, if the index's [autosummarize](sql-createindex.md#INDEX-RELOPTION-AUTOSUMMARIZE) parameter is enabled, which it isn't by default, whenever autovacuum runs in that database, summarization will occur for all unsummarized page ranges that have been filled, regardless of whether the table itself is processed by autovacuum; see below. 

Lastly, the following functions can be used (while these functions run, [search_path](runtime-config-client.md#GUC-SEARCH-PATH) is temporarily changed to `pg_catalog, pg_temp`): 

`brin_summarize_new_values(regclass)` which summarizes all unsummarized ranges;   
---  
`brin_summarize_range(regclass, bigint)` which summarizes only the range containing the given page, if it is unsummarized.   
  
When autosummarization is enabled, a request is sent to `autovacuum` to execute a targeted summarization for a block range when an insertion is detected for the first item of the first page of the next block range, to be fulfilled the next time an autovacuum worker finishes running in the same database. If the request queue is full, the request is not recorded and a message is sent to the server log: 
    
    
    LOG:  request for BRIN range summarization for index "brin_wi_idx" page 128 was not recorded
    

When this happens, the range will remain unsummarized until the next regular vacuum run on the table, or one of the functions mentioned above are invoked. 

Conversely, a range can be de-summarized using the `brin_desummarize_range(regclass, bigint)` function, which is useful when the index tuple is no longer a very good representation because the existing values have changed. See [Section 9.28.8](functions-admin.md#FUNCTIONS-ADMIN-INDEX "9.28.8. Index Maintenance Functions") for details. 

### 64.5.2. Built-in Operator Classes #

The core PostgreSQL distribution includes the BRIN operator classes shown in [Table 64.4](brin.md#BRIN-BUILTIN-OPCLASSES-TABLE "Table 64.4. Built-in BRIN Operator Classes"). 

The _minmax_ operator classes store the minimum and the maximum values appearing in the indexed column within the range. The _inclusion_ operator classes store a value which includes the values in the indexed column within the range. The _bloom_ operator classes build a Bloom filter for all values in the range. The _minmax-multi_ operator classes store multiple minimum and maximum values, representing values appearing in the indexed column within the range. 

**Table 64.4. Built-in BRIN Operator Classes**

Name| Indexable Operators  
---|---  
`bit_minmax_ops`| `= (bit,bit)`  
`< (bit,bit)`  
`> (bit,bit)`  
`<= (bit,bit)`  
`>= (bit,bit)`  
`box_inclusion_ops`| `@> (box,point)`  
`<< (box,box)`  
`&< (box,box)`  
`&> (box,box)`  
`>> (box,box)`  
`<@ (box,box)`  
`@> (box,box)`  
`~= (box,box)`  
`&& (box,box)`  
`<<| (box,box)`  
`&<| (box,box)`  
`|&> (box,box)`  
`|>> (box,box)`  
`bpchar_bloom_ops`| `= (character,character)`  
`bpchar_minmax_ops`| `= (character,character)`  
`< (character,character)`  
`<= (character,character)`  
`> (character,character)`  
`>= (character,character)`  
`bytea_bloom_ops`| `= (bytea,bytea)`  
`bytea_minmax_ops`| `= (bytea,bytea)`  
`< (bytea,bytea)`  
`<= (bytea,bytea)`  
`> (bytea,bytea)`  
`>= (bytea,bytea)`  
`char_bloom_ops`| `= ("char","char")`  
`char_minmax_ops`| `= ("char","char")`  
`< ("char","char")`  
`<= ("char","char")`  
`> ("char","char")`  
`>= ("char","char")`  
`date_bloom_ops`| `= (date,date)`  
`date_minmax_ops`| `= (date,date)`  
`< (date,date)`  
`<= (date,date)`  
`> (date,date)`  
`>= (date,date)`  
`date_minmax_multi_ops`| `= (date,date)`  
`< (date,date)`  
`<= (date,date)`  
`> (date,date)`  
`>= (date,date)`  
`float4_bloom_ops`| `= (float4,float4)`  
`float4_minmax_ops`| `= (float4,float4)`  
`< (float4,float4)`  
`> (float4,float4)`  
`<= (float4,float4)`  
`>= (float4,float4)`  
`float4_minmax_multi_ops`| `= (float4,float4)`  
`< (float4,float4)`  
`> (float4,float4)`  
`<= (float4,float4)`  
`>= (float4,float4)`  
`float8_bloom_ops`| `= (float8,float8)`  
`float8_minmax_ops`| `= (float8,float8)`  
`< (float8,float8)`  
`<= (float8,float8)`  
`> (float8,float8)`  
`>= (float8,float8)`  
`float8_minmax_multi_ops`| `= (float8,float8)`  
`< (float8,float8)`  
`<= (float8,float8)`  
`> (float8,float8)`  
`>= (float8,float8)`  
`inet_inclusion_ops`| `<< (inet,inet)`  
`<<= (inet,inet)`  
`>> (inet,inet)`  
`>>= (inet,inet)`  
`= (inet,inet)`  
`&& (inet,inet)`  
`inet_bloom_ops`| `= (inet,inet)`  
`inet_minmax_ops`| `= (inet,inet)`  
`< (inet,inet)`  
`<= (inet,inet)`  
`> (inet,inet)`  
`>= (inet,inet)`  
`inet_minmax_multi_ops`| `= (inet,inet)`  
`< (inet,inet)`  
`<= (inet,inet)`  
`> (inet,inet)`  
`>= (inet,inet)`  
`int2_bloom_ops`| `= (int2,int2)`  
`int2_minmax_ops`| `= (int2,int2)`  
`< (int2,int2)`  
`> (int2,int2)`  
`<= (int2,int2)`  
`>= (int2,int2)`  
`int2_minmax_multi_ops`| `= (int2,int2)`  
`< (int2,int2)`  
`> (int2,int2)`  
`<= (int2,int2)`  
`>= (int2,int2)`  
`int4_bloom_ops`| `= (int4,int4)`  
`int4_minmax_ops`| `= (int4,int4)`  
`< (int4,int4)`  
`> (int4,int4)`  
`<= (int4,int4)`  
`>= (int4,int4)`  
`int4_minmax_multi_ops`| `= (int4,int4)`  
`< (int4,int4)`  
`> (int4,int4)`  
`<= (int4,int4)`  
`>= (int4,int4)`  
`int8_bloom_ops`| `= (bigint,bigint)`  
`int8_minmax_ops`| `= (bigint,bigint)`  
`< (bigint,bigint)`  
`> (bigint,bigint)`  
`<= (bigint,bigint)`  
`>= (bigint,bigint)`  
`int8_minmax_multi_ops`| `= (bigint,bigint)`  
`< (bigint,bigint)`  
`> (bigint,bigint)`  
`<= (bigint,bigint)`  
`>= (bigint,bigint)`  
`interval_bloom_ops`| `= (interval,interval)`  
`interval_minmax_ops`| `= (interval,interval)`  
`< (interval,interval)`  
`<= (interval,interval)`  
`> (interval,interval)`  
`>= (interval,interval)`  
`interval_minmax_multi_ops`| `= (interval,interval)`  
`< (interval,interval)`  
`<= (interval,interval)`  
`> (interval,interval)`  
`>= (interval,interval)`  
`macaddr_bloom_ops`| `= (macaddr,macaddr)`  
`macaddr_minmax_ops`| `= (macaddr,macaddr)`  
`< (macaddr,macaddr)`  
`<= (macaddr,macaddr)`  
`> (macaddr,macaddr)`  
`>= (macaddr,macaddr)`  
`macaddr_minmax_multi_ops`| `= (macaddr,macaddr)`  
`< (macaddr,macaddr)`  
`<= (macaddr,macaddr)`  
`> (macaddr,macaddr)`  
`>= (macaddr,macaddr)`  
`macaddr8_bloom_ops`| `= (macaddr8,macaddr8)`  
`macaddr8_minmax_ops`| `= (macaddr8,macaddr8)`  
`< (macaddr8,macaddr8)`  
`<= (macaddr8,macaddr8)`  
`> (macaddr8,macaddr8)`  
`>= (macaddr8,macaddr8)`  
`macaddr8_minmax_multi_ops`| `= (macaddr8,macaddr8)`  
`< (macaddr8,macaddr8)`  
`<= (macaddr8,macaddr8)`  
`> (macaddr8,macaddr8)`  
`>= (macaddr8,macaddr8)`  
`name_bloom_ops`| `= (name,name)`  
`name_minmax_ops`| `= (name,name)`  
`< (name,name)`  
`<= (name,name)`  
`> (name,name)`  
`>= (name,name)`  
`numeric_bloom_ops`| `= (numeric,numeric)`  
`numeric_minmax_ops`| `= (numeric,numeric)`  
`< (numeric,numeric)`  
`<= (numeric,numeric)`  
`> (numeric,numeric)`  
`>= (numeric,numeric)`  
`numeric_minmax_multi_ops`| `= (numeric,numeric)`  
`< (numeric,numeric)`  
`<= (numeric,numeric)`  
`> (numeric,numeric)`  
`>= (numeric,numeric)`  
`oid_bloom_ops`| `= (oid,oid)`  
`oid_minmax_ops`| `= (oid,oid)`  
`< (oid,oid)`  
`> (oid,oid)`  
`<= (oid,oid)`  
`>= (oid,oid)`  
`oid_minmax_multi_ops`| `= (oid,oid)`  
`< (oid,oid)`  
`> (oid,oid)`  
`<= (oid,oid)`  
`>= (oid,oid)`  
`pg_lsn_bloom_ops`| `= (pg_lsn,pg_lsn)`  
`pg_lsn_minmax_ops`| `= (pg_lsn,pg_lsn)`  
`< (pg_lsn,pg_lsn)`  
`> (pg_lsn,pg_lsn)`  
`<= (pg_lsn,pg_lsn)`  
`>= (pg_lsn,pg_lsn)`  
`pg_lsn_minmax_multi_ops`| `= (pg_lsn,pg_lsn)`  
`< (pg_lsn,pg_lsn)`  
`> (pg_lsn,pg_lsn)`  
`<= (pg_lsn,pg_lsn)`  
`>= (pg_lsn,pg_lsn)`  
`range_inclusion_ops`| `= (anyrange,anyrange)`  
`< (anyrange,anyrange)`  
`<= (anyrange,anyrange)`  
`>= (anyrange,anyrange)`  
`> (anyrange,anyrange)`  
`&& (anyrange,anyrange)`  
`@> (anyrange,anyelement)`  
`@> (anyrange,anyrange)`  
`<@ (anyrange,anyrange)`  
`<< (anyrange,anyrange)`  
`>> (anyrange,anyrange)`  
`&< (anyrange,anyrange)`  
`&> (anyrange,anyrange)`  
`-|- (anyrange,anyrange)`  
`text_bloom_ops`| `= (text,text)`  
`text_minmax_ops`| `= (text,text)`  
`< (text,text)`  
`<= (text,text)`  
`> (text,text)`  
`>= (text,text)`  
`tid_bloom_ops`| `= (tid,tid)`  
`tid_minmax_ops`| `= (tid,tid)`  
`< (tid,tid)`  
`> (tid,tid)`  
`<= (tid,tid)`  
`>= (tid,tid)`  
`tid_minmax_multi_ops`| `= (tid,tid)`  
`< (tid,tid)`  
`> (tid,tid)`  
`<= (tid,tid)`  
`>= (tid,tid)`  
`timestamp_bloom_ops`| `= (timestamp,timestamp)`  
`timestamp_minmax_ops`| `= (timestamp,timestamp)`  
`< (timestamp,timestamp)`  
`<= (timestamp,timestamp)`  
`> (timestamp,timestamp)`  
`>= (timestamp,timestamp)`  
`timestamp_minmax_multi_ops`| `= (timestamp,timestamp)`  
`< (timestamp,timestamp)`  
`<= (timestamp,timestamp)`  
`> (timestamp,timestamp)`  
`>= (timestamp,timestamp)`  
`timestamptz_bloom_ops`| `= (timestamptz,timestamptz)`  
`timestamptz_minmax_ops`| `= (timestamptz,timestamptz)`  
`< (timestamptz,timestamptz)`  
`<= (timestamptz,timestamptz)`  
`> (timestamptz,timestamptz)`  
`>= (timestamptz,timestamptz)`  
`timestamptz_minmax_multi_ops`| `= (timestamptz,timestamptz)`  
`< (timestamptz,timestamptz)`  
`<= (timestamptz,timestamptz)`  
`> (timestamptz,timestamptz)`  
`>= (timestamptz,timestamptz)`  
`time_bloom_ops`| `= (time,time)`  
`time_minmax_ops`| `= (time,time)`  
`< (time,time)`  
`<= (time,time)`  
`> (time,time)`  
`>= (time,time)`  
`time_minmax_multi_ops`| `= (time,time)`  
`< (time,time)`  
`<= (time,time)`  
`> (time,time)`  
`>= (time,time)`  
`timetz_bloom_ops`| `= (timetz,timetz)`  
`timetz_minmax_ops`| `= (timetz,timetz)`  
`< (timetz,timetz)`  
`<= (timetz,timetz)`  
`> (timetz,timetz)`  
`>= (timetz,timetz)`  
`timetz_minmax_multi_ops`| `= (timetz,timetz)`  
`< (timetz,timetz)`  
`<= (timetz,timetz)`  
`> (timetz,timetz)`  
`>= (timetz,timetz)`  
`uuid_bloom_ops`| `= (uuid,uuid)`  
`uuid_minmax_ops`| `= (uuid,uuid)`  
`< (uuid,uuid)`  
`> (uuid,uuid)`  
`<= (uuid,uuid)`  
`>= (uuid,uuid)`  
`uuid_minmax_multi_ops`| `= (uuid,uuid)`  
`< (uuid,uuid)`  
`> (uuid,uuid)`  
`<= (uuid,uuid)`  
`>= (uuid,uuid)`  
`varbit_minmax_ops`| `= (varbit,varbit)`  
`< (varbit,varbit)`  
`> (varbit,varbit)`  
`<= (varbit,varbit)`  
`>= (varbit,varbit)`  
  
  


#### 64.5.2.1. Operator Class Parameters #

Some of the built-in operator classes allow specifying parameters affecting behavior of the operator class. Each operator class has its own set of allowed parameters. Only the `bloom` and `minmax-multi` operator classes allow specifying parameters: 

bloom operator classes accept these parameters: 

`n_distinct_per_range`
    

Defines the estimated number of distinct non-null values in the block range, used by BRIN bloom indexes for sizing of the Bloom filter. It behaves similarly to `n_distinct` option for [ALTER TABLE](sql-altertable.md "ALTER TABLE"). When set to a positive value, each block range is assumed to contain this number of distinct non-null values. When set to a negative value, which must be greater than or equal to -1, the number of distinct non-null values is assumed to grow linearly with the maximum possible number of tuples in the block range (about 290 rows per block). The default value is `-0.1`, and the minimum number of distinct non-null values is `16`. 

`false_positive_rate`
    

Defines the desired false positive rate used by BRIN bloom indexes for sizing of the Bloom filter. The values must be between 0.0001 and 0.25. The default value is 0.01, which is 1% false positive rate. 

minmax-multi operator classes accept these parameters: 

`values_per_range`
    

Defines the maximum number of values stored by BRIN minmax indexes to summarize a block range. Each value may represent either a point, or a boundary of an interval. Values must be between 8 and 256, and the default value is 32. 

### 64.5.3. Extensibility #

The BRIN interface has a high level of abstraction, requiring the access method implementer only to implement the semantics of the data type being accessed. The BRIN layer itself takes care of concurrency, logging and searching the index structure. 

All it takes to get a BRIN access method working is to implement a few user-defined methods, which define the behavior of summary values stored in the index and the way they interact with scan keys. In short, BRIN combines extensibility with generality, code reuse, and a clean interface. 

There are four methods that an operator class for BRIN must provide: 

`BrinOpcInfo *opcInfo(Oid type_oid)`
    

Returns internal information about the indexed columns' summary data. The return value must point to a palloc'd `BrinOpcInfo`, which has this definition: 
    
    
    typedef struct BrinOpcInfo
    {
        /* Number of columns stored in an index column of this opclass */
        uint16      oi_nstored;
    
        /* Opaque pointer for the opclass' private use */
        void       *oi_opaque;
    
        /* Type cache entries of the stored columns */
        TypeCacheEntry *oi_typcache[FLEXIBLE_ARRAY_MEMBER];
    } BrinOpcInfo;
    

`BrinOpcInfo`.`oi_opaque` can be used by the operator class routines to pass information between support functions during an index scan. 

`bool consistent(BrinDesc *bdesc, BrinValues *column, ScanKey *keys, int nkeys)`
    

Returns whether all the ScanKey entries are consistent with the given indexed values for a range. The attribute number to use is passed as part of the scan key. Multiple scan keys for the same attribute may be passed at once; the number of entries is determined by the `nkeys` parameter. 

`bool consistent(BrinDesc *bdesc, BrinValues *column, ScanKey key)`
    

Returns whether the ScanKey is consistent with the given indexed values for a range. The attribute number to use is passed as part of the scan key. This is an older backward-compatible variant of the consistent function. 

`bool addValue(BrinDesc *bdesc, BrinValues *column, Datum newval, bool isnull)`
    

Given an index tuple and an indexed value, modifies the indicated attribute of the tuple so that it additionally represents the new value. If any modification was done to the tuple, `true` is returned. 

`bool unionTuples(BrinDesc *bdesc, BrinValues *a, BrinValues *b)`
    

Consolidates two index tuples. Given two index tuples, modifies the indicated attribute of the first of them so that it represents both tuples. The second tuple is not modified. 

An operator class for BRIN can optionally specify the following method: 

`void options(local_relopts *relopts)`
    

Defines a set of user-visible parameters that control operator class behavior. 

The `options` function is passed a pointer to a `local_relopts` struct, which needs to be filled with a set of operator class specific options. The options can be accessed from other support functions using the `PG_HAS_OPCLASS_OPTIONS()` and `PG_GET_OPCLASS_OPTIONS()` macros. 

Since both key extraction of indexed values and representation of the key in BRIN are flexible, they may depend on user-specified parameters. 

The core distribution includes support for four types of operator classes: minmax, minmax-multi, inclusion and bloom. Operator class definitions using them are shipped for in-core data types as appropriate. Additional operator classes can be defined by the user for other data types using equivalent definitions, without having to write any source code; appropriate catalog entries being declared is enough. Note that assumptions about the semantics of operator strategies are embedded in the support functions' source code. 

Operator classes that implement completely different semantics are also possible, provided implementations of the four main support functions described above are written. Note that backwards compatibility across major releases is not guaranteed: for example, additional support functions might be required in later releases. 

To write an operator class for a data type that implements a totally ordered set, it is possible to use the minmax support functions alongside the corresponding operators, as shown in [Table 64.5](brin.md#BRIN-EXTENSIBILITY-MINMAX-TABLE "Table 64.5. Function and Support Numbers for Minmax Operator Classes"). All operator class members (functions and operators) are mandatory. 

**Table 64.5. Function and Support Numbers for Minmax Operator Classes**

Operator class member| Object  
---|---  
Support Function 1| internal function `brin_minmax_opcinfo()`  
Support Function 2| internal function `brin_minmax_add_value()`  
Support Function 3| internal function `brin_minmax_consistent()`  
Support Function 4| internal function `brin_minmax_union()`  
Operator Strategy 1| operator less-than  
Operator Strategy 2| operator less-than-or-equal-to  
Operator Strategy 3| operator equal-to  
Operator Strategy 4| operator greater-than-or-equal-to  
Operator Strategy 5| operator greater-than  
  
  


To write an operator class for a complex data type which has values included within another type, it's possible to use the inclusion support functions alongside the corresponding operators, as shown in [Table 64.6](brin.md#BRIN-EXTENSIBILITY-INCLUSION-TABLE "Table 64.6. Function and Support Numbers for Inclusion Operator Classes"). It requires only a single additional function, which can be written in any language. More functions can be defined for additional functionality. All operators are optional. Some operators require other operators, as shown as dependencies on the table. 

**Table 64.6. Function and Support Numbers for Inclusion Operator Classes**

Operator class member| Object| Dependency  
---|---|---  
Support Function 1| internal function `brin_inclusion_opcinfo()`|   
Support Function 2| internal function `brin_inclusion_add_value()`|   
Support Function 3| internal function `brin_inclusion_consistent()`|   
Support Function 4| internal function `brin_inclusion_union()`|   
Support Function 11| function to merge two elements|   
Support Function 12| optional function to check whether two elements are mergeable|   
Support Function 13| optional function to check if an element is contained within another|   
Support Function 14| optional function to check whether an element is empty|   
Operator Strategy 1| operator left-of| Operator Strategy 4  
Operator Strategy 2| operator does-not-extend-to-the-right-of| Operator Strategy 5  
Operator Strategy 3| operator overlaps|   
Operator Strategy 4| operator does-not-extend-to-the-left-of| Operator Strategy 1  
Operator Strategy 5| operator right-of| Operator Strategy 2  
Operator Strategy 6, 18| operator same-as-or-equal-to| Operator Strategy 7  
Operator Strategy 7, 16, 24, 25| operator contains-or-equal-to|   
Operator Strategy 8, 26, 27| operator is-contained-by-or-equal-to| Operator Strategy 3  
Operator Strategy 9| operator does-not-extend-above| Operator Strategy 11  
Operator Strategy 10| operator is-below| Operator Strategy 12  
Operator Strategy 11| operator is-above| Operator Strategy 9  
Operator Strategy 12| operator does-not-extend-below| Operator Strategy 10  
Operator Strategy 20| operator less-than| Operator Strategy 5  
Operator Strategy 21| operator less-than-or-equal-to| Operator Strategy 5  
Operator Strategy 22| operator greater-than| Operator Strategy 1  
Operator Strategy 23| operator greater-than-or-equal-to| Operator Strategy 1  
  
  


Support function numbers 1 through 10 are reserved for the BRIN internal functions, so the SQL level functions start with number 11. Support function number 11 is the main function required to build the index. It should accept two arguments with the same data type as the operator class, and return the union of them. The inclusion operator class can store union values with different data types if it is defined with the `STORAGE` parameter. The return value of the union function should match the `STORAGE` data type. 

Support function numbers 12 and 14 are provided to support irregularities of built-in data types. Function number 12 is used to support network addresses from different families which are not mergeable. Function number 14 is used to support empty ranges. Function number 13 is an optional but recommended one, which allows the new value to be checked before it is passed to the union function. As the BRIN framework can shortcut some operations when the union is not changed, using this function can improve index performance. 

To write an operator class for a data type that implements only an equality operator and supports hashing, it is possible to use the bloom support procedures alongside the corresponding operators, as shown in [Table 64.7](brin.md#BRIN-EXTENSIBILITY-BLOOM-TABLE "Table 64.7. Procedure and Support Numbers for Bloom Operator Classes"). All operator class members (procedures and operators) are mandatory. 

**Table 64.7. Procedure and Support Numbers for Bloom Operator Classes**

Operator class member| Object  
---|---  
Support Procedure 1| internal function `brin_bloom_opcinfo()`  
Support Procedure 2| internal function `brin_bloom_add_value()`  
Support Procedure 3| internal function `brin_bloom_consistent()`  
Support Procedure 4| internal function `brin_bloom_union()`  
Support Procedure 5| internal function `brin_bloom_options()`  
Support Procedure 11| function to compute hash of an element  
Operator Strategy 1| operator equal-to  
  
  


Support procedure numbers 1-10 are reserved for the BRIN internal functions, so the SQL level functions start with number 11. Support function number 11 is the main function required to build the index. It should accept one argument with the same data type as the operator class, and return a hash of the value. 

The minmax-multi operator class is also intended for data types implementing a totally ordered set, and may be seen as a simple extension of the minmax operator class. While minmax operator class summarizes values from each block range into a single contiguous interval, minmax-multi allows summarization into multiple smaller intervals to improve handling of outlier values. It is possible to use the minmax-multi support procedures alongside the corresponding operators, as shown in [Table 64.8](brin.md#BRIN-EXTENSIBILITY-MINMAX-MULTI-TABLE "Table 64.8. Procedure and Support Numbers for minmax-multi Operator Classes"). All operator class members (procedures and operators) are mandatory. 

**Table 64.8. Procedure and Support Numbers for minmax-multi Operator Classes**

Operator class member| Object  
---|---  
Support Procedure 1| internal function `brin_minmax_multi_opcinfo()`  
Support Procedure 2| internal function `brin_minmax_multi_add_value()`  
Support Procedure 3| internal function `brin_minmax_multi_consistent()`  
Support Procedure 4| internal function `brin_minmax_multi_union()`  
Support Procedure 5| internal function `brin_minmax_multi_options()`  
Support Procedure 11| function to compute distance between two values (length of a range)  
Operator Strategy 1| operator less-than  
Operator Strategy 2| operator less-than-or-equal-to  
Operator Strategy 3| operator equal-to  
Operator Strategy 4| operator greater-than-or-equal-to  
Operator Strategy 5| operator greater-than  
  
  


Both minmax and inclusion operator classes support cross-data-type operators, though with these the dependencies become more complicated. The minmax operator class requires a full set of operators to be defined with both arguments having the same data type. It allows additional data types to be supported by defining extra sets of operators. Inclusion operator class operator strategies are dependent on another operator strategy as shown in [Table 64.6](brin.md#BRIN-EXTENSIBILITY-INCLUSION-TABLE "Table 64.6. Function and Support Numbers for Inclusion Operator Classes"), or the same operator strategy as themselves. They require the dependency operator to be defined with the `STORAGE` data type as the left-hand-side argument and the other supported data type to be the right-hand-side argument of the supported operator. See `float4_minmax_ops` as an example of minmax, and `box_inclusion_ops` as an example of inclusion. 

* * *

[Prev](gin.md "64.4. GIN Indexes") | [Up](indextypes.md "Chapter 64. Built-in Index Access Methods")|  [Next](hash-index.md "64.6. Hash Indexes")  
---|---|---  
64.4. GIN Indexes | [Home](index.md "PostgreSQL 17.5 Documentation")|  64.6. Hash Indexes
