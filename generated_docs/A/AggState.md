# AggState

## Location
[src/include/nodes/execnodes.h:2463-2537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2463-L2537)

## Overview
AggState is the primary execution state node for PostgreSQL's aggregation operations, managing the complete lifecycle of aggregate computations including hash tables, sorting, grouping sets, and spill-to-disk operations.

## Definition

```c
typedef struct AggState
{
	ScanState	ss;				/* its first field is NodeTag */
	List	   *aggs;			/* all Aggref nodes in targetlist & quals */
	int			numaggs;		/* length of list (could be zero!) */
	int			numtrans;		/* number of pertrans items */
	AggStrategy aggstrategy;	/* strategy mode */
	AggSplit	aggsplit;		/* agg-splitting mode, see nodes.h */
	AggStatePerPhase phase;		/* pointer to current phase data */
	int			numphases;		/* number of phases (including phase 0) */
	int			current_phase;	/* current phase number */
	AggStatePerAgg peragg;		/* per-Aggref information */
	AggStatePerTrans pertrans;	/* per-Trans state information */
	ExprContext *hashcontext;	/* econtexts for long-lived data (hashtable) */
	ExprContext **aggcontexts;	/* econtexts for long-lived data (per GS) */
	ExprContext *tmpcontext;	/* econtext for input expressions */
#define FIELDNO_AGGSTATE_CURAGGCONTEXT 14
	ExprContext *curaggcontext; /* currently active aggcontext */
	AggStatePerAgg curperagg;	/* currently active aggregate, if any */
#define FIELDNO_AGGSTATE_CURPERTRANS 16
	AggStatePerTrans curpertrans;	/* currently active trans state, if any */
	bool		input_done;		/* indicates end of input */
	bool		agg_done;		/* indicates completion of Agg scan */
	int			projected_set;	/* The last projected grouping set */
#define FIELDNO_AGGSTATE_CURRENT_SET 20
	int			current_set;	/* The current grouping set being evaluated */
	Bitmapset  *grouped_cols;	/* grouped cols in current projection */
	List	   *all_grouped_cols;	/* list of all grouped cols in DESC order */
	Bitmapset  *colnos_needed;	/* all columns needed from the outer plan */
	int			max_colno_needed;	/* highest colno needed from outer plan */
	bool		all_cols_needed;	/* are all cols from outer plan needed? */
	/* These fields are for grouping set phase data */
	int			maxsets;		/* The max number of sets in any phase */
	AggStatePerPhase phases;	/* array of all phases */
	Tuplesortstate *sort_in;	/* sorted input to phases > 1 */
	Tuplesortstate *sort_out;	/* input is copied here for next phase */
	TupleTableSlot *sort_slot;	/* slot for sort results */
	/* these fields are used in AGG_PLAIN and AGG_SORTED modes: */
	AggStatePerGroup *pergroups;	/* grouping set indexed array of per-group
									 * pointers */
	HeapTuple	grp_firstTuple; /* copy of first tuple of current group */
	/* these fields are used in AGG_HASHED and AGG_MIXED modes: */
	bool		table_filled;	/* hash table filled yet? */
	int			num_hashes;
	MemoryContext hash_metacxt; /* memory for hash table itself */
	struct LogicalTapeSet *hash_tapeset;	/* tape set for hash spill tapes */
	struct HashAggSpill *hash_spills;	/* HashAggSpill for each grouping set,
										 * exists only during first pass */
	TupleTableSlot *hash_spill_rslot;	/* for reading spill files */
	TupleTableSlot *hash_spill_wslot;	/* for writing spill files */
	List	   *hash_batches;	/* hash batches remaining to be processed */
	bool		hash_ever_spilled;	/* ever spilled during this execution? */
	bool		hash_spill_mode;	/* we hit a limit during the current batch
									 * and we must not create new groups */
	Size		hash_mem_limit; /* limit before spilling hash table */
	uint64		hash_ngroups_limit; /* limit before spilling hash table */
	int			hash_planned_partitions;	/* number of partitions planned
											 * for first pass */
	double		hashentrysize;	/* estimate revised during execution */
	Size		hash_mem_peak;	/* peak hash table memory usage */
	uint64		hash_ngroups_current;	/* number of groups currently in
										 * memory in all hash tables */
	uint64		hash_disk_used; /* kB of disk space used */
	int			hash_batches_used;	/* batches used during entire execution */

	AggStatePerHash perhash;	/* array of per-hashtable data */
	AggStatePerGroup *hash_pergroup;	/* grouping set indexed array of
										 * per-group pointers */

	/* support for evaluation of agg input expressions: */
#define FIELDNO_AGGSTATE_ALL_PERGROUPS 53
	AggStatePerGroup *all_pergroups;	/* array of first ->pergroups, than
										 * ->hash_pergroup */
	SharedAggInfo *shared_info; /* one entry per worker */
} AggState;
```
## Detailed Description
AggState is the central execution state structure for all aggregate operations in PostgreSQL. It inherits from ScanState and manages the complete execution lifecycle of aggregation, including multiple aggregation strategies (plain, sorted, hashed, and mixed modes), grouping sets, and advanced features like spill-to-disk for large datasets. The structure supports both single-phase and multi-phase execution, parallel aggregation, and complex grouping operations. It maintains separate contexts for different aspects of aggregation execution and tracks memory usage to implement intelligent spilling when memory limits are exceeded.

## Parameters / Member Variables
- Netid State   Recv-Q Send-Q                              Local Address:Port       Peer Address:Port      Process
u_str ESTAB   0      0                                               * 16331009              * 16331008         
u_str ESTAB   0      0                                               * 18065001              * 18065000         
u_str ESTAB   0      0                                               * 20055638              * 20055637         
u_str ESTAB   0      0                                               * 16883729              * 16883728         
u_str ESTAB   0      0                                               * 16876188              * 16876189         
u_str ESTAB   0      0                                               * 1998                  * 3655             
u_str ESTAB   0      0                                               * 16331008              * 16331009         
u_str ESTAB   0      0                                               * 16323072              * 16323071         
u_str ESTAB   0      0                                               * 16930623              * 16930624         
u_str ESTAB   0      0                                               * 16948454              * 16948455         
u_str ESTAB   0      0                                               * 16318726              * 16318727         
u_str ESTAB   0      0                                               * 18061758              * 18061757         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515568              * 15531228         
u_str ESTAB   0      0                                               * 16876182              * 16876183         
u_str ESTAB   0      0                                               * 10305                 * 10304            
u_str ESTAB   0      0                                               * 16321329              * 16321330         
u_str ESTAB   0      0                                               * 16318723              * 16318722         
u_str ESTAB   0      0                                               * 18061762              * 18061761         
u_str ESTAB   0      0                                               * 16948457              * 16948456         
u_str ESTAB   0      0                                               * 16876189              * 16876188         
u_str ESTAB   0      0                            /tmp/dbus-vEvJ09Fzqf 10314                 * 3654             
u_str ESTAB   0      0                                               * 20055633              * 20055634         
u_str ESTAB   0      0                                               * 16321327              * 16321328         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15372                 * 3606             
u_str ESTAB   0      0                                               * 16321326              * 16321325         
u_str ESTAB   0      0                                               * 16948458              * 16948459         
u_str ESTAB   0      0                                               * 16876185              * 16876184         
u_str ESTAB   0      0                                               * 20055634              * 20055633         
u_str ESTAB   0      0                                               * 18065005              * 18065004         
u_str ESTAB   0      0                                               * 16323069              * 16323070         
u_str ESTAB   0      0                                               * 20055636              * 20055635         
u_str ESTAB   0      0                                               * 18061759              * 18061760         
u_str ESTAB   0      0                                               * 16883731              * 16883730         
u_str ESTAB   0      0                                               * 18065007              * 18065006         
u_str ESTAB   0      0                                               * 16331007              * 16331006         
u_str ESTAB   0      0                                               * 16318725              * 16318724         
u_str ESTAB   0      0                                               * 16930624              * 16930623         
u_str ESTAB   0      0                                               * 16930619              * 16930620         
u_str ESTAB   0      0                                               * 16948461              * 16948460         
u_str ESTAB   0      0                                               * 16876184              * 16876185         
u_str ESTAB   0      0                                               * 18061764              * 18061763         
u_str ESTAB   0      0                                               * 18065000              * 18065001         
u_str ESTAB   0      0                                               * 16883735              * 16883734         
u_str ESTAB   0      0                                               * 15536473              * 15536472         
u_str ESTAB   0      0                                               * 20055637              * 20055638         
u_str ESTAB   0      0                                               * 16948455              * 16948454         
u_str ESTAB   0      0                                               * 7569                  * 1891             
u_str ESTAB   0      0                                               * 11302                 * 11303            
u_str ESTAB   0      0                                               * 16883730              * 16883731         
u_str ESTAB   0      0                                               * 9317                  * 9318             
u_str ESTAB   0      0                                               * 16323070              * 16323069         
u_str ESTAB   0      0                                               * 10304                 * 10305            
u_str ESTAB   0      0                                               * 14343                 * 0                
u_str ESTAB   0      0                                               * 16876183              * 16876182         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 1891                  * 7569             
u_str ESTAB   0      0                                               * 16323076              * 16323075         
u_str ESTAB   0      0                                               * 16330961              * 16330960         
u_str ESTAB   0      0                                               * 16883733              * 16883732         
u_str ESTAB   0      0                                               * 16323073              * 16323074         
u_str ESTAB   0      0                                               * 16331006              * 16331007         
u_str ESTAB   0      0                                               * 16883732              * 16883733         
u_str ESTAB   0      0                                               * 15536472              * 15536473         
u_str ESTAB   0      0                                               * 9318                  * 9317             
u_str ESTAB   0      0                                               * 16930620              * 16930619         
u_str ESTAB   0      0                                               * 16323075              * 16323076         
u_str ESTAB   0      0                                               * 16331002              * 16331003         
u_str ESTAB   0      0                                               * 16948456              * 16948457         
u_str ESTAB   0      0                                               * 16331004              * 16331005         
u_str ESTAB   0      0                                               * 18065002              * 18065003         
u_str ESTAB   0      0                                               * 16930622              * 16930621         
u_str ESTAB   0      0                                               * 16318724              * 16318725         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515571              * 15519569         
u_str ESTAB   0      0                                               * 16330957              * 16330958         
u_str ESTAB   0      0                                               * 3654                  * 10314            
u_str ESTAB   0      0                                               * 16321323              * 16321324         
u_str ESTAB   0      0                                               * 18061757              * 18061758         
u_str ESTAB   0      0                                               * 16323071              * 16323072         
u_str ESTAB   0      0                                               * 16330960              * 16330961         
u_str ESTAB   0      0                                               * 16321330              * 16321329         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 4219                  * 15398            
u_str ESTAB   0      0                                               * 3614                  * 3615             
u_str ESTAB   0      0                                               * 16930621              * 16930622         
u_str ESTAB   0      0                                               * 16930626              * 16930625         
u_str ESTAB   0      0                                               * 16948459              * 16948458         
u_str ESTAB   0      0                                               * 16876186              * 16876187         
u_str ESTAB   0      0                                               * 18061763              * 18061764         
u_str ESTAB   0      0                                               * 18065004              * 18065005         
u_str ESTAB   0      0                                               * 16883728              * 16883729         
u_str ESTAB   0      0                                               * 16321325              * 16321326         
u_str ESTAB   0      0                                               * 11303                 * 11302            
u_str ESTAB   0      0                                               * 11300                 * 11301            
u_str ESTAB   0      0                                               * 16323074              * 16323073         
u_str ESTAB   0      0                                               * 16330958              * 16330957         
u_str ESTAB   0      0                                               * 16318722              * 16318723         
u_str ESTAB   0      0                     /mnt/wslg/PulseAudioRDPSink 3655                  * 1998             
u_str ESTAB   0      0                                               * 16876187              * 16876186         
u_str ESTAB   0      0                                               * 16883734              * 16883735         
u_str ESTAB   0      0                                               * 3615                  * 3614             
u_str ESTAB   0      0                                               * 16948460              * 16948461         
u_str ESTAB   0      0                                               * 16331005              * 16331004         
u_str ESTAB   0      0                                               * 16930625              * 16930626         
u_str ESTAB   0      0                                               * 18061761              * 18061762         
u_str ESTAB   0      0                                               * 11301                 * 11300            
u_str ESTAB   0      0                                               * 3606                  * 15372            
u_str ESTAB   0      0                                               * 15531228              * 15515568         
u_str ESTAB   0      0                                               * 18065006              * 18065007         
u_str ESTAB   0      0                                               * 16321328              * 16321327         
u_str ESTAB   0      0                                               * 15398                 * 4219             
u_str ESTAB   0      0                                               * 16318727              * 16318726         
u_str ESTAB   0      0                                               * 20055635              * 20055636         
u_str ESTAB   0      0                                               * 18061760              * 18061759         
u_str ESTAB   0      0                                               * 15519569              * 15515571         
u_str ESTAB   0      0                                               * 16321324              * 16321323         
u_str ESTAB   0      0                                               * 18065003              * 18065002         
u_str ESTAB   0      0                                               * 16331003              * 16331002         
tcp   ESTAB   0      0                                  172.30.249.175:35148     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37962         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:59952         127.0.0.1:37353            
tcp   ESTAB   0      0                                  172.30.249.175:3400       172.30.240.1:60802            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:37962            
tcp   ESTAB   0      0                                  172.30.249.175:46968     20.27.177.116:https            
tcp   ESTAB   0      0                                       127.0.0.1:59958         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:48262         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:48268            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:37970            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:48262            
tcp   ESTAB   0      0                                  172.30.249.175:53320     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:59958            
tcp   ESTAB   0      0                                       127.0.0.1:48268         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37970         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:45894         127.0.0.1:37353            
tcp   ESTAB   0      0                                  172.30.249.175:45608     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:45894            
tcp   ESTAB   0      0                                       127.0.0.1:45882         127.0.0.1:37353            
tcp   ESTAB   0      0                                  172.30.249.175:45592     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:45882            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:59952            
tcp   ESTAB   0      0                                  172.30.249.175:35162     160.79.104.10:https            
v_str ESTAB   0      0                                               *:633275402             2:50000            
v_str ESTAB   0      0                                               *:633275403             2:50000            
v_str ESTAB   0      0                                               *:633275404             2:50000            
v_str ESTAB   0      0                                               *:633275405             2:50000            
v_str ESTAB   0      0                                               *:633275406             2:50000            
v_str ESTAB   0      0                                               *:633275408             2:50001            
v_str ESTAB   0      0                                               *:633275409             2:50001            
v_str ESTAB   0      0                                               *:633275410             2:50001            
v_str ESTAB   0      0                                               *:633275424             2:50000            
v_str ESTAB   0      0                                               *:633275425             2:50000            
v_str ESTAB   0      0                                               *:633275426             2:50002            
v_str ESTAB   0      0                                               *:633275427             2:50002            
v_str ESTAB   0      0                                               *:633275428             2:50002            
v_str ESTAB   0      0                                               *:633275431             2:50002            
v_str ESTAB   0      0                                               *:633275432             2:50002            
v_str ESTAB   0      0                                               *:633275433             2:50002            
v_str ESTAB   0      0                                               *:1                     2:4102841729       
v_str ESTAB   0      0                                               *:633275411             2:4102841364       
v_str ESTAB   0      0                                               *:633275671             2:342791897        
v_str ESTAB   0      0                                               *:633275674             2:342791913        
v_str ESTAB   0      0                                               *:633275674             2:342791912        
v_str ESTAB   0      0                                               *:633275674             2:342791911        
v_str ESTAB   0      0                                               *:633275674             2:342791910        
v_str ESTAB   0      0                                               *:633275674             2:342791909        
v_str ESTAB   0      0                                               *:633275675             2:342791919        
v_str ESTAB   0      0                                               *:633275672             2:342791902        
v_str ESTAB   0      0                                               *:633275672             2:342791901        
v_str ESTAB   0      0                                               *:633275672             2:342791900        
v_str ESTAB   0      0                                               *:633275672             2:342791899        
v_str ESTAB   0      0                                               *:633275672             2:342791898        
v_str ESTAB   0      0                                               *:633275673             2:342791908        
v_str ESTAB   0      0                                               *:633275676             2:342791924        
v_str ESTAB   0      0                                               *:633275676             2:342791923        
v_str ESTAB   0      0                                               *:633275676             2:342791922        
v_str ESTAB   0      0                                               *:633275676             2:342791921        
v_str ESTAB   0      0                                               *:633275676             2:342791920        
v_str ESTAB   0      0                                               *:633275430             2:4102841703       
v_str ESTAB   0      0                                               *:633275430             2:4102841702       
v_str ESTAB   0      0                                               *:633275430             2:4102841701       
v_str CLOSING 0      0                                               *:633275430             2:4102841700       
v_str ESTAB   0      0                                               *:633275429             2:4102841697       
v_str ESTAB   0      0                                               *:633275435             2:4102841707       
v_str ESTAB   0      0                                               *:633275691             2:342792670        
v_str ESTAB   0      0                                               *:633275694             2:342792686        
v_str ESTAB   0      0                                               *:633275694             2:342792685        
v_str ESTAB   0      0                                               *:633275694             2:342792684        
v_str ESTAB   0      0                                               *:633275694             2:342792683        
v_str ESTAB   0      0                                               *:633275694             2:342792682        
v_str ESTAB   0      0                                               *:633275692             2:342792675        
v_str ESTAB   0      0                                               *:633275692             2:342792674        
v_str ESTAB   0      0                                               *:633275692             2:342792673        
v_str ESTAB   0      0                                               *:633275692             2:342792672        
v_str ESTAB   0      0                                               *:633275692             2:342792671        
v_str ESTAB   0      0                                               *:633275693             2:342792681        
v_str ESTAB   0      0                                               *:633275703             2:342792823        
v_str ESTAB   0      0                                               *:633275706             2:342792839        
v_str ESTAB   0      0                                               *:633275706             2:342792838        
v_str ESTAB   0      0                                               *:633275706             2:342792837        
v_str ESTAB   0      0                                               *:633275706             2:342792836        
v_str ESTAB   0      0                                               *:633275706             2:342792835        
v_str ESTAB   0      0                                               *:633275704             2:342792828        
v_str ESTAB   0      0                                               *:633275704             2:342792827        
v_str ESTAB   0      0                                               *:633275704             2:342792826        
v_str ESTAB   0      0                                               *:633275704             2:342792825        
v_str ESTAB   0      0                                               *:633275704             2:342792824        
v_str ESTAB   0      0                                               *:633275705             2:342792834        
v_str ESTAB   0      0                                               *:633275458             2:4102841830       
v_str ESTAB   0      0                                               *:633275458             2:4102841829       
v_str CLOSING 0      0                                               *:633275458             2:4102841828       
v_str CLOSING 0      0                                               *:633275458             2:4102841827       
v_str ESTAB   0      0                                               *:633275458             2:4102841826       
v_str ESTAB   0      0                                               *:633275711             2:342793593        
v_str ESTAB   0      0                                               *:633275457             2:4102841825       
v_str ESTAB   0      0                                               *:633275462             2:4102842074       
v_str ESTAB   0      0                                               *:633275462             2:4102842073       
v_str ESTAB   0      0                                               *:633275462             2:4102842072       
v_str ESTAB   0      0                                               *:633275462             2:4102842071       
v_str ESTAB   0      0                                               *:633275462             2:4102842070       
v_str ESTAB   0      0                                               *:633275714             2:342793609        
v_str ESTAB   0      0                                               *:633275714             2:342793608        
v_str ESTAB   0      0                                               *:633275714             2:342793607        
v_str ESTAB   0      0                                               *:633275714             2:342793606        
v_str ESTAB   0      0                                               *:633275714             2:342793605        
v_str ESTAB   0      0                                               *:633275463             2:4102842086       
v_str ESTAB   0      0                                               *:633275712             2:342793598        
v_str ESTAB   0      0                                               *:633275712             2:342793597        
v_str ESTAB   0      0                                               *:633275712             2:342793596        
v_str ESTAB   0      0                                               *:633275712             2:342793595        
v_str ESTAB   0      0                                               *:633275712             2:342793594        
v_str ESTAB   0      0                                               *:633275461             2:4102842069       
v_str ESTAB   0      0                                               *:633275713             2:342793604        
v_str ESTAB   0      0                                               *:633275466             2:4102842133       
v_str ESTAB   0      0                                               *:633275466             2:4102842132       
v_str ESTAB   0      0                                               *:633275466             2:4102842131       
v_str ESTAB   0      0                                               *:633275466             2:4102842130       
v_str ESTAB   0      0                                               *:633275466             2:4102842129       
v_str ESTAB   0      0                                               *:633275467             2:4102842156       
v_str ESTAB   0      0                                               *:633275464             2:4102842091       
v_str ESTAB   0      0                                               *:633275464             2:4102842090       
v_str ESTAB   0      0                                               *:633275464             2:4102842089       
v_str ESTAB   0      0                                               *:633275464             2:4102842088       
v_str ESTAB   0      0                                               *:633275464             2:4102842087       
v_str ESTAB   0      0                                               *:633275465             2:4102842128       
v_str ESTAB   0      0                                               *:633275470             2:4102842564       
v_str ESTAB   0      0                                               *:633275470             2:4102842563       
v_str ESTAB   0      0                                               *:633275470             2:4102842562       
v_str ESTAB   0      0                                               *:633275470             2:4102842561       
v_str ESTAB   0      0                                               *:633275470             2:4102842560       
v_str ESTAB   0      0                                               *:633275471             2:4102843465       
v_str ESTAB   0      0                                               *:633275468             2:4102842161       
v_str ESTAB   0      0                                               *:633275468             2:4102842160       
v_str ESTAB   0      0                                               *:633275468             2:4102842159       
v_str ESTAB   0      0                                               *:633275468             2:4102842158       
v_str ESTAB   0      0                                               *:633275468             2:4102842157       
v_str ESTAB   0      0                                               *:633275469             2:4102842559       
v_str ESTAB   0      0                                               *:633275472             2:4102843470       
v_str ESTAB   0      0                                               *:633275472             2:4102843469       
v_str ESTAB   0      0                                               *:633275472             2:4102843468       
v_str ESTAB   0      0                                               *:633275472             2:4102843467       
v_str ESTAB   0      0                                               *:633275472             2:4102843466       : Base ScanState structure containing common execution node fields
- : List of all Aggref nodes found in target list and qualifications
- : Count of aggregate functions (can be zero for GROUP BY only)
- : Number of transition state items for optimization
- : Execution strategy (AGG_PLAIN, AGG_SORTED, AGG_HASHED, AGG_MIXED)
- : Aggregation splitting mode for parallel execution
- : Pointer to current phase execution data
- : Total number of execution phases
- : Currently executing phase number
- : Array of per-aggregate function information
- : Array of per-transition state information
- : Memory context for hash table long-lived data
- : Array of memory contexts for grouping sets
- : Memory context for temporary input expression evaluation
- : Currently active aggregate memory context
- : Currently active aggregate being processed
- : Currently active transition state
- : Flag indicating end of input stream
- : Flag indicating completion of aggregation scan
- : Last projected grouping set identifier
- : Current grouping set being evaluated
- : Bitmap of grouped columns in current projection
- : List of all grouped columns in descending order
- : Bitmap of all columns needed from outer plan
- : Highest column number needed from outer plan
- : Flag indicating if all outer plan columns are needed
- : Maximum number of sets in any phase
- : Array containing all phase execution data
- : Tuplesort state for sorted input to phases > 1
- : Tuplesort state for copying input to next phase
- : Tuple slot for sort operation results
- : Array of per-group state pointers indexed by grouping set
- : Copy of first tuple in current group
- : Flag indicating if hash table has been populated
- : Number of hash tables in use
- : Memory context for hash table metadata
- : Logical tape set for hash spill operations
- : Array of HashAggSpill structures for each grouping set
- : Tuple slot for reading spill files
- : Tuple slot for writing spill files
- : List of remaining hash batches to process
- : Flag indicating if spilling occurred during execution
- : Flag indicating current batch hit limits and no new groups allowed
- : Memory limit before triggering hash table spill
- : Group count limit before triggering spill
- : Number of partitions planned for first pass
- : Estimated hash entry size, revised during execution
- : Peak memory usage by hash tables
- : Current number of groups in memory across all hash tables
- : Disk space used in kilobytes
- : Total batches used during entire execution
- : Array of per-hash table state data
- : Array of per-group pointers for hash mode
- : Combined array of pergroups and hash_pergroup
- : Shared information for parallel workers

## Dependencies
- Functions called/Symbols referenced:
  - [ScanState](../S/ScanState.md)
  - AggStrategy
  - AggSplit
  - [AggStatePerPhase](AggStatePerPhase.md)
  - [AggStatePerAgg](AggStatePerAgg.md)
  - [AggStatePerTrans](AggStatePerTrans.md)
  - [AggStatePerGroup](AggStatePerGroup.md)
  - [AggStatePerHash](AggStatePerHash.md)
  - [HashAggSpill](../H/HashAggSpill.md)
  - [LogicalTapeSet](../L/LogicalTapeSet.md)
  - [SharedAggInfo](../S/SharedAggInfo.md)
  - [Tuplesortstate](../T/Tuplesortstate.md)
  - [ExprContext](../E/ExprContext.md)
- Called from (representative examples):
  - [ExecAgg](../E/ExecAgg.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [ExecEndAgg](../E/ExecEndAgg.md)
  - [ExecReScanAgg](../E/ExecReScanAgg.md)
  - [AggCheckCallContext](AggCheckCallContext.md)
  - [advance_aggregates](../a/advance_aggregates.md)
  - [finalize_aggregates](../f/finalize_aggregates.md)

## Notes and Other Information
AggState is one of the most complex execution node structures in PostgreSQL, supporting multiple aggregation strategies and advanced features like intelligent memory management with spill-to-disk capabilities. The structure is designed to handle everything from simple aggregations to complex grouping sets with parallel execution. The hash spilling mechanism allows PostgreSQL to process datasets larger than available memory by partitioning data across temporary files. The multi-phase execution capability enables efficient processing of complex grouping set queries by organizing computation into optimal phases.