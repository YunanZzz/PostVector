#ifndef HNSWFLAT_H
#define HNSWFLAT_H

#include "postgres.h"

#if PG_VERSION_NUM < 110000
#error "Requires PostgreSQL 11+"
#endif

#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "nodes/execnodes.h"
#include "port.h"				/* for strtof() and random() */
#include "utils/sampling.h"
#include "utils/tuplesort.h"
#include "vector.h"

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif

#ifdef HNSWFLAT_BENCH
#include "portability/instr_time.h"
#endif

#define HNSWFLAT_MAX_DIM 2000
#define HNSWFLAT_MAX_LEVEL 100
#define	RAND_MAX	0x7fffffff

/* Support functions */
#define HNSWFLAT_DISTANCE_PROC 1
#define HNSWFLAT_NORM_PROC 2

#define HNSWFLAT_VERSION	1
#define HNSWFLAT_MAGIC_NUMBER 0x14FF1A7
#define HNSWFLAT_PAGE_ID	0xFF84

/* Preserved page numbers */
#define HNSWFLAT_METAPAGE_BLKNO	0
#define HNSWFLAT_HEAD_BLKNO		1	/* first index tuple page */

#define HNSWFLAT_DEFAULT_BNN	16
#define HNSWFLAT_MAX_BNN		64

#define HNSWFLAT_DEFAULT_EFB	40
#define HNSWFLAT_MAX_EFB		320

#define HNSWFLAT_DEFAULT_EFS	50
#define HNSWFLAT_MAX_EFS		400

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_HNSWFLAT_PHASE_LOAD		2


#define HnswflatVertexTupleHeaderSize offsetof(HnswflatVertexData, vector)
#define HnswflatEdgeTupleHeaderSize offsetof(HnswflatEdgeData, target)
#define HnswflatPageGetOpaque(page)	((HnswflatPageOpaque) PageGetSpecialPointer(page))
#define HnswflatPageGetMeta(page)	((HnswflatMetaPageData *) PageGetContents(page))

#ifdef HNSWFLAT_BENCH
#define HnswflatBench(name, code) \
	do { \
		instr_time	start; \
		instr_time	duration; \
		INSTR_TIME_SET_CURRENT(start); \
		(code); \
		INSTR_TIME_SET_CURRENT(duration); \
		INSTR_TIME_SUBTRACT(duration, start); \
		elog(INFO, "%s: %.3f ms", name, INSTR_TIME_GET_MILLISEC(duration)); \
	} while (0)
#else
#define HnswflatBench(name, code) (code)
#endif

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#define RandomInt() pg_prng_uint32(&pg_global_prng_state)
#else
#define RandomDouble() (((double) random()) / MAX_RANDOM_VALUE)
#define RandomInt() random()
#endif

/* Variables */

/* Exported functions */
PGDLLEXPORT void _PG_init(void);

typedef struct VectorArrayData
{
	int			length;
	int			maxlen;
	int			dim;
	Vector	   *items;
}			VectorArrayData;

typedef VectorArrayData * VectorArray;

typedef struct HnswGid {
  	int64		vector_id;
	int64		neighbor_offset;	
} 			HnswGid;

/* HNSWFlat index options */
typedef struct HnswflatOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int         base_nb_num;              /* max number of neighbors for each layer */
    int         ef_build;            /* efConstruction for HNSW */
    int         ef_search;            /* ef for HNSW */
}			HnswflatOptions;

typedef struct HnswflatBuildState
{
	/* Info */
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;

	/* Settings */
	int			dimensions;
    int         base_nb_num;           
    int         ef_build;            
    int         ef_search;

	/* Statistics */
	double		indtuples;
	double		reltuples;
	int64_t		edgetuples;

	/* Support functions */
	FmgrInfo   *procinfo;
	Oid			collation;

	/* Variables */
	int64_t		ep_id;
	int			ep_level;
	int			vertex_tuple_size;
	int			edge_tuple_size;
	int			max_vertex_per_page;

	/* Level Probability */
	int real_max_level;
	uint16 cum_nn_per_level[HNSWFLAT_MAX_LEVEL + 1];
  	double assign_probas[HNSWFLAT_MAX_LEVEL];

	/* Loading */
	Buffer		cbuf;
	Page		cpage;
	GenericXLogState *xlogstate;

	/* Memory */
	MemoryContext tmpCtx;
}			HnswflatBuildState;

typedef struct HnswflatMetaPageData
{
	int64 		ep_id;
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16      base_nb_num;           
    uint16      ef_build;            
    uint16      ef_search;
	uint16		max_vertex_per_page;
	int16		ep_level;
    BlockNumber edgeStartPage;
}			HnswflatMetaPageData;

typedef HnswflatMetaPageData * HnswflatMetaPage;

typedef struct HnswflatPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* for identification of HNSWFlat indexes */
}			HnswflatPageOpaqueData;

typedef HnswflatPageOpaqueData * HnswflatPageOpaque;

typedef struct HnswflatVertexData {
  	uint16 		level;
	int64		offset;
	ItemPointerData heap_ptr;
	float 		vector[FLEXIBLE_ARRAY_MEMBER];
} HnswflatVertexData;

typedef HnswflatVertexData * HnswflatVertex;

typedef struct HnswflatEdgeData	/* bnn neighbors of a vertex in one layer */
{
	int64		source_id;
	HnswGid		target[FLEXIBLE_ARRAY_MEMBER];
}			HnswflatEdgeData;

typedef HnswflatEdgeData * HnswflatEdge;

typedef struct HnswflatScanList
{
	pairingheap_node ph_node;
	BlockNumber startPage;
	double		distance;
}			HnswflatScanList;

typedef struct HnswflatScanOpaqueData
{
	bool		first;
	Buffer		buf;

	/* Sorting */
	Tuplesortstate *sortstate;
	TupleDesc	tupdesc;
	TupleTableSlot *slot;
	bool		isnull;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;

	/* Setting */
	int64 		ep_id;
	int			ep_level;
	uint16      base_nb_num; 
	uint16      ef_search;
	int			max_vertex_per_page;

	/* In memory Results */
	int64		*id;
	double		*dis;

	/* Lists */
	pairingheap *listQueue;
}			HnswflatScanOpaqueData;

typedef HnswflatScanOpaqueData * HnswflatScanOpaque;

#define VECTOR_ARRAY_SIZE(_length, _dim) (sizeof(VectorArrayData) + (_length) * VECTOR_SIZE(_dim))
#define VECTOR_ARRAY_OFFSET(_arr, _offset) ((char*) (_arr)->items + (_offset) * VECTOR_SIZE((_arr)->dim))
#define VectorArrayGet(_arr, _offset) ((Vector *) VECTOR_ARRAY_OFFSET(_arr, _offset))
#define VectorArraySet(_arr, _offset, _val) memcpy(VECTOR_ARRAY_OFFSET(_arr, _offset), _val, VECTOR_SIZE((_arr)->dim))

/* Methods */
VectorArray VectorArrayInit(int maxlen, int dimensions);
void		VectorArrayFree(VectorArray arr);
void		PrintVectorArray(char *msg, VectorArray arr);
void		HnswflatKmeans(Relation index, VectorArray samples, VectorArray centers);
FmgrInfo   *HnswflatOptionalProcInfo(Relation rel, uint16 procnum);
bool		HnswflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result);
int			HnswflatGetBnn(Relation index);
int			HnswflatGetEfb(Relation index);
int			HnswflatGetEfs(Relation index);
void		HnswflatCommitBuffer(Buffer buf, GenericXLogState *state);
void		HnswflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum);
Buffer		HnswflatNewBuffer(Relation index, ForkNumber forkNum);
void		HnswflatInitPage(Buffer buf, Page page);
void		HnswflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);

/* Index access methods */
IndexBuildResult *ivfflatbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void		ivfflatbuildempty(Relation index);
bool		ivfflatinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
						  ,bool indexUnchanged
#endif
						  ,IndexInfo *indexInfo
);
IndexBulkDeleteResult *ivfflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *ivfflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc ivfflatbeginscan(Relation index, int nkeys, int norderbys);
void		ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool		ivfflatgettuple(IndexScanDesc scan, ScanDirection dir);
void		ivfflatendscan(IndexScanDesc scan);

#endif
