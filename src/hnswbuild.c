#include "postgres.h"

#include <float.h>

#include "catalog/index.h"
#include "hnswflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#elif PG_VERSION_NUM >= 120000
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "access/tableam.h"
#include "commands/progress.h"
#else
#define PROGRESS_CREATEIDX_SUBPHASE 0
#define PROGRESS_CREATEIDX_TUPLES_TOTAL 0
#define PROGRESS_CREATEIDX_TUPLES_DONE 0
#endif

#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"

#if PG_VERSION_NUM >= 130000
#define CALLBACK_ITEM_POINTER ItemPointer tid
#else
#define CALLBACK_ITEM_POINTER HeapTuple hup
#endif

#if PG_VERSION_NUM >= 120000
#define UpdateProgress(index, val) pgstat_progress_update_param(index, val)
#else
#define UpdateProgress(index, val) ((void)val)
#endif

static int
RandomLevel(HnswflatBuildState * buildstate) {
  float4 f = (float4)rand()/(float4)(RAND_MAX);
  int level;

  for (level = 0; level < buildstate->real_max_level; level++) {
    if (f < buildstate->assign_probas[level]) {
      return level;
    }
    f -= buildstate->assign_probas[level];
  }
  return buildstate->real_max_level == 0 ? 0 : buildstate->real_max_level - 1;
}

static void
SetDefaultProbas(HnswflatBuildState * buildstate) {
  int nbNum = 0;
  int level;
  float levelMult = 1.0 / log(buildstate->base_nb_num);
  float proba;

  buildstate->cum_nn_per_level[0] = 0;
  for (level = 0; ;level++) {
    proba = exp(-level / levelMult) * (1 - exp(-1 / levelMult));
    if (proba < 1e-9) {
      break;
    }
    nbNum += (level == 0) ? buildstate->base_nb_num * 2 : buildstate->base_nb_num;
    buildstate->cum_nn_per_level[level + 1] = nbNum;
    buildstate->assign_probas[level] = proba;
  }
  buildstate->real_max_level = level;
  if (buildstate->real_max_level > HNSWFLAT_MAX_LEVEL) {
    elog(ERROR, "real_max_level is greater than HNSWFLAT_MAX_LEVEL");
  }
}

static HnswflatVertex
HnswFormDataTuple(HnswflatBuildState *buildstate,
    ItemPointer iptr, Datum *values, bool *isnull) {
    ArrayType *arr;
    text *rawText;
    int dim, i, len;
    float4 *data;
    char *rawData;
    char dest[1024 * 1024];
    HnswflatVertex res = (HnswflatVertex) palloc(buildState->vertex_tuple_size);

    res->heap_ptr = *iptr;
    if (isnull[0]) {
        pfree(res);
        return NULL;
    }
  
    arr = DatumGetArrayTypeP(values[0]);
    dim = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
    if (dim != buildstate->dimensions) {
        elog(WARNING, "data dimension[%d] not equal to configure dimension[%d]",
          dim, buildstate->dimensions);
        pfree(res);
        return NULL;
    }
    data = ((float *)ARR_DATA_PTR(arr));
    for (i = 0; i < dim; ++i) {
        res->vector[i] = data[i];
    }
    res->level = RandomLevel(opts);
    res->offset = buildstate->edgetuples;
    buildstate->edgetuples += level + 2;

    return res;
}

static HnswflatEdge
HnswInitEdgeTuple(HnswflatBuildState *buildstate, int64 source_id)
{
    int     i;

    HnswflatEdge res = (HnswflatEdge)palloc(buildState->edge_tuple_size);

    res->source_id = source_id;

    for (i = 0; i < buildstate->base_nb_num; i++) {
        res->target[i].vector_id = -1;
        res->target[i].neighbor_offset = -1;
        //TODO I assume id starts from 0, so default value is -1
        //IF id starts from 1, then default value should be 0
    }

    return res;
}

/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	HnswflatBuildState *buildstate = (HnswflatBuildState *) state;
    HnswflatVertex tup;
    GenericXLogState *xlogstate;
    Size		itemsz;
	MemoryContext oldCtx;

#if PG_VERSION_NUM < 130000
	ItemPointer tid = &hup->t_self;
#endif

	/* Skip nulls */
	if (isnull[0])
		return;

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	tup = HnswFormDataTuple(buildState, tid, values, isnull);
    if (!tup) {
        MemoryContextSwitchTo(oldCtx);
        MemoryContextReset(buildState->tmpctx);
        return;
    }
    /* Check for free space */
	itemsz = MAXALIGN(buildstate->vertex_tuple_size);
	if (PageGetFreeSpace(buildstate->cpage) < itemsz)
		HnswflatAppendPage(index, &buildstate->cbuf, &buildstate->cpage, &buildstate->state, MAIN_FORKNUM);

	/* Add the item */
	if (PageAddItem(&buildstate->cpage, (Item) tup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	pfree(tup);
    buildState->indtuples += 1;

	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Initialize the build state
 */
static void
InitBuildState(HnswflatBuildState * buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;

	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;
    buildstate->base_nb_num = HnswflatGetBnn(index);
    buildstate->ef_build = HnswflatGetEfb(index);
    buildstate->ef_search = HnswflatGetEfs(index);

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > HNSWFLAT_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for hnswflat index", HNSWFLAT_MAX_DIM);

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;
    buildstate->edgetuples = 0;
    buildstate->ep_id = -1;
    buildstate->ep_level = -1;
    buildstate->edge_tuple_size = HnswflatEdgeTupleHeaderSize + sizeof(HnswGid) * buildstate->base_nb_num;
    buildstate->vertex_tuple_size = HnswflatVertexTupleHeaderSize + sizeof(float) * buildstate->dimensions;

	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, HNSWFLAT_DISTANCE_PROC);
	buildstate->normprocinfo = HnswflatOptionalProcInfo(index, HNSWFLAT_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	/* Create tuple description for sorting */
#if PG_VERSION_NUM >= 120000
	buildstate->tupdesc = CreateTemplateTupleDesc(4);
#else
	buildstate->tupdesc = CreateTemplateTupleDesc(4, false);
#endif
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 1, "level", INT4OID, -1, 0);
    TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 2, "offset", INT8OID, -1, 0);
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 3, "tid", TIDOID, -1, 0);
	TupleDescInitEntry(buildstate->tupdesc, (AttrNumber) 4, "vector", RelationGetDescr(index)->attrs[0].atttypid, -1, 0);

#if PG_VERSION_NUM >= 120000
	buildstate->slot = MakeSingleTupleTableSlot(buildstate->tupdesc, &TTSOpsVirtual);
#else
	buildstate->slot = MakeSingleTupleTableSlot(buildstate->tupdesc);
#endif

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Hnswflat build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free resources
 */
static void
FreeBuildState(HnswflatBuildState * buildstate)
{
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Create the metapage
 */
static void
CreateMetaPage(Relation index, int dimensions, 
               int base_nb_num, int ef_build, int ef_search, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	HnswflatMetaPage metap;

	buf = HnswflatNewBuffer(index, forkNum);
	HnswflatInitRegisterPage(index, &buf, &page, &state);

	/* Set metapage data */
	metap = HnswflatPageGetMeta(page);
	metap->magicNumber = HNSWFLAT_MAGIC_NUMBER;
	metap->version = HNSWFLAT_VERSION;
	metap->dimensions = dimensions;
	metap->base_nb_num = base_nb_num;
    metap->ef_build = ef_build;
    metap->ef_search = ef_search;
    metap->ep_id = -1;
    metap->ep_level = -1;
    metap->edgeStartPage = InvalidBlockNumber;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(HnswflatMetaPageData)) - (char *) page;

	HnswflatCommitBuffer(buf, state);
}

/*
 * Scan table for tuples to index
 */
static void
ScanTable(HnswflatBuildState * buildstate)
{
#if PG_VERSION_NUM >= 120000
	buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, true, BuildCallback, (void *) buildstate, NULL);
#else
	buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
											   true, BuildCallback, (void *) buildstate, NULL);
#endif
}

/*
 * Create entry pages
 */
static void
CreateEntryPages(HnswflatBuildState * buildstate, ForkNumber forkNum)
{
    CHECK_FOR_INTERRUPTS();

    UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_HNSWFLAT_PHASE_LOAD);

    buildstate->cbuf = IvfflatNewBuffer(buildstate->index, forkNum);
	IvfflatInitRegisterPage(buildstate->index, &buildstate->cbuf, &buildstate->cpage, &buildstate->state);

	/* Add tuples to sort */
	if (buildstate->heap != NULL)
		HnswflatBench("assign tuples", ScanTable(buildstate));

    IvfflatCommitBuffer(buildstate->cbuf, buildstate->state);
}

/*
 * Now we have all vertex tuples stored. Traverse vertex tuples to create empty edge tuples for vertexs.
 * TODO: Create in-memory hnsw structure here.
 * vector array storage: we already know the number of vectors buildState->indtuples, 
 * copy vertex->vector to vector array when traversing.
 * level array and offset array: all stored in vertex tuples
 * neighbors array: array size is buildState->edgetuples. needs to be computed in next step.
 */
static void
InmemoryLoad(HnswflatBuildState * buildstate, ForkNumber forkNum)
{
    Buffer		cbuf;
	Page		cpage;
    GenericXLogState *state;
	OffsetNumber offno;
	OffsetNumber maxoffno;
    HnswflatVertex vertex;
    HnswflatEdge edge;
    HnswflatMetaPage meta;
    BlockNumber edgeStartPage;
    Size        itemsz;
	BlockNumber nextblkno = HNSWFLAT_HEAD_BLKNO;
	int         i;

    /* Search all vertex tuple pages */
	while (BlockNumberIsValid(nextblkno))
    {
        cbuf = ReadBuffer(buildstate->index, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);
        if (nextblkno == HNSWFLAT_HEAD_BLKNO)
            buildstate->max_vertex_per_page = maxoffno;

		for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
        {
            vertex = (HnswflatVertex) PageGetItem(cpage, PageGetItemId(cpage, offno));
            /* TODO create in-memory hnsw structures when traversing vertex tuples
             * NOTICE currently I assume vertex id and level starts from 0. Not sure about how Faiss starts
             * Watch out when loading values of levels and neighbors
             */
        }

        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
    }
}

/*
 * TODO. Given in-memory hnsw array structures, vector storage, levels, offsets,  
 * Compute neighbors
 */
static void
InmemoryCompute(HnswflatBuildState * buildstate)
{
    //TODO Remember to record int64_t		ep_id;
	//                        int			ep_level;
    // in buildstate
}

/*
 * Fill edge tuples based on the neighbors array
 */
static void
CreateEdgePages(HnswflatBuildState * buildstate, ForkNumber forkNum)
{
    Buffer		cbuf;
	Page		cpage;
    GenericXLogState *state;
    HnswflatEdge edge;
    Size        itemsz;
    int64       i;
    int         j;

    cbuf = IvfflatNewBuffer(buildstate->index, forkNum);
	IvfflatInitRegisterPage(buildstate->index, &cbuf, &cpage, &state);

    edgeStartPage = BufferGetBlockNumber(cbuf);
    
    for (i = 0; i < buildstate->indtuples; i++)
    {
        //use in-memory hnsw structure level[i]. (base level = 1 in Faiss!)
        for (j = 0; j < _level[i] + 1; j++)
            {
                edge = HnswInitEdgeTuple(buildstate, i);

               /* offsets[i+1]-offsets[i] should be equal to (_level[i] + 1) * bnn
                * TODO: In this section, we should load neighbors 
                * from neighbors[offsets[i]+j*bnn  :  offsets[i]+(j+1)*bnn] to this edge tuple
                * NOTICE currently I assume vertex id and level starts from 0. Not sure about how Faiss starts
                * Watch out when loading values of levels and neighbors
                */

                /* Check for free space */
	            itemsz = MAXALIGN(buildstate->edge_tuple_size);
	            if (PageGetFreeSpace(cpage) < itemsz)
		            HnswflatAppendPage(buildstate->index, &cbuf, &cpage, &state, forkNum);

	            /* Add the item */
	            if (PageAddItem(&cpage, (Item) edge, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
		            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	            pfree(edge);                
            }
    }

    IvfflatCommitBuffer(cbuf, state);

    cbuf = ReadBufferExtended(buildstate->index, forkNum, HNSWFLAT_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(buildstate->index);
	cpage = GenericXLogRegisterBuffer(state, cbuf, 0);

    meta = (HnswflatMetaPage) PageGetItem(cpage, PageGetItemId(cpage, FirstOffsetNumber));
    meta->edgeStartPage = edgeStartPage;
    meta->ep_id = buildstate->ep_id;
    meta->ep_level = buildstate->ep_level;
    meta->max_vertex_per_page = buildstate->max_vertex_per_page;

    IvfflatCommitBuffer(cbuf, state);

}

/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   HnswflatBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo);
    SetDefaultProbas(buildstate);

	/* Create pages */
	CreateMetaPage(index, buildstate->dimensions, 
        buildstate->base_nb_num, buildstate->ef_build,
        buildstate->ef_search, forkNum);
	CreateEntryPages(buildstate, forkNum);
    InmemoryLoad(buildstate, forkNum);

    //TODO InmemoryCompute() call a function here to compute values of neighbors array

    CreateEdgePages(buildstate, forkNum);

	FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *
hnswflatbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	HnswflatBuildState buildstate;

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * Build the index for an unlogged table
 */
void
hnswflatbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	HnswflatBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
