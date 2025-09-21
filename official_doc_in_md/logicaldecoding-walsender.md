47.3. Streaming Replication Protocol Interface  
---  
[Prev](logicaldecoding-explanation.md "47.2. Logical Decoding Concepts") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")| Chapter 47. Logical Decoding| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logicaldecoding-sql.md "47.4. Logical Decoding SQL Interface")  
  
* * *

## 47.3. Streaming Replication Protocol Interface #

The commands 

  * `CREATE_REPLICATION_SLOT _`slot_name`_ LOGICAL _`output_plugin`_`

  * `DROP_REPLICATION_SLOT _`slot_name`_` [ `WAIT` ]

  * `START_REPLICATION SLOT _`slot_name`_ LOGICAL ...`




are used to create, drop, and stream changes from a replication slot, respectively. These commands are only available over a replication connection; they cannot be used via SQL. See [Section 53.4](protocol-replication.md "53.4. Streaming Replication Protocol") for details on these commands. 

The command [pg_recvlogical](app-pgrecvlogical.md "pg_recvlogical") can be used to control logical decoding over a streaming replication connection. (It uses these commands internally.) 

* * *

[Prev](logicaldecoding-explanation.md "47.2. Logical Decoding Concepts") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")|  [Next](logicaldecoding-sql.md "47.4. Logical Decoding SQL Interface")  
---|---|---  
47.2. Logical Decoding Concepts | [Home](index.md "PostgreSQL 17.5 Documentation")|  47.4. Logical Decoding SQL Interface
