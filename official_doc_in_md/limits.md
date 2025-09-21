Appendix K. PostgreSQL Limits  
---  
[Prev](docguide-style.md "J.6. Style Guide") | [Up](appendixes.md "Part VIII. Appendixes")| Part VIII. Appendixes| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](acronyms.md "Appendix L. Acronyms")  
  
* * *

## Appendix K. PostgreSQL Limits

[Table K.1](limits.md#LIMITS-TABLE "Table K.1. PostgreSQL Limitations") describes various hard limits of PostgreSQL. However, practical limits, such as performance limitations or available disk space may apply before absolute hard limits are reached. 

**Table K.1. PostgreSQL Limitations**

Item| Upper Limit| Comment  
---|---|---  
database size| unlimited|   
number of databases| 4,294,950,911|   
relations per database| 1,431,650,303|   
relation size| 32 TB| with the default `BLCKSZ` of 8192 bytes  
rows per table| limited by the number of tuples that can fit onto 4,294,967,295 pages|   
columns per table| 1,600| further limited by tuple size fitting on a single page; see note below  
columns in a result set| 1,664|   
field size| 1 GB|   
indexes per table| unlimited| constrained by maximum relations per database  
columns per index| 32| can be increased by recompiling PostgreSQL  
partition keys| 32| can be increased by recompiling PostgreSQL  
identifier length| 63 bytes| can be increased by recompiling PostgreSQL  
function arguments| 100| can be increased by recompiling PostgreSQL  
query parameters| 65,535|   
  
  


The maximum number of columns for a table is further reduced as the tuple being stored must fit in a single 8192-byte heap page. For example, excluding the tuple header, a tuple made up of 1,600 `int` columns would consume 6400 bytes and could be stored in a heap page, but a tuple of 1,600 `bigint` columns would consume 12800 bytes and would therefore not fit inside a heap page. Variable-length fields of types such as `text`, `varchar`, and `char` can have their values stored out of line in the table's TOAST table when the values are large enough to require it. Only an 18-byte pointer must remain inside the tuple in the table's heap. For shorter length variable-length fields, either a 4-byte or 1-byte field header is used and the value is stored inside the heap tuple. 

Columns that have been dropped from the table also contribute to the maximum column limit. Moreover, although the dropped column values for newly created tuples are internally marked as null in the tuple's null bitmap, the null bitmap also occupies space. 

Each table can store a theoretical maximum of 2^32 out-of-line values; see [Section 65.2](storage-toast.md "65.2. TOAST") for a detailed discussion of out-of-line storage. This limit arises from the use of a 32-bit OID to identify each such value. The practical limit is significantly less than the theoretical limit, because as the OID space fills up, finding an OID that is still free can become expensive, in turn slowing down INSERT/UPDATE statements. Typically, this is only an issue for tables containing many terabytes of data; partitioning is a possible workaround. 

* * *

[Prev](docguide-style.md "J.6. Style Guide") | [Up](appendixes.md "Part VIII. Appendixes")|  [Next](acronyms.md "Appendix L. Acronyms")  
---|---|---  
J.6. Style Guide | [Home](index.md "PostgreSQL 17.5 Documentation")|  Appendix L. Acronyms
