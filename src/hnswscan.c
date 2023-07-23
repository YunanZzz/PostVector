#include "postgres.h"

#include <float.h>

#include "access/relscan.h"
#include "hnswflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"

#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"

/*
 * Compare list distances
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const HnswflatScanList *) a)->distance > ((const HnswflatScanList *) b)->distance)
		return 1;

	if (((const HnswflatScanList *) a)->distance < ((const HnswflatScanList *) b)->distance)
		return -1;

	return 0;
}

/*
 * Search In-memory HNSW
 */
static void
InmemorySearch(IndexScanDesc scan, Datum value)
{
    HnswflatScanOpaque so = (HnswflatScanOpaque) scan->opaque;

    //  TODO Search In-memory HNSW and return ef results to so->id
    //                                                  so->dis
    //  
}

/*
 * Get Block number and offset according to id
 */
static void
VertexID2Block(HnswflatScanOpaque so, int64 id, BlockNumber *searchPage, OffsetNumber *offno)
{
    *searchPage = HNSWFLAT_HEAD_BLKNO + id / so->max_vertex_per_page;
    *offno = id % so->max_vertex_per_page + 1;
    //TODO if new inserted vertex pages are not consecutive;
}

/*
 * Search
 */
static void
GetScanItems(IndexScanDesc scan)
{
	HnswflatScanOpaque so = (HnswflatScanOpaque) scan->opaque;
	Buffer		buf;
	Page		page;
	HnswflatVertex	itup;
	BlockNumber searchPage;
	OffsetNumber offno;
	OffsetNumber maxoffno;
    int         i;
	Datum		datum;
	bool		isnull;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	double		tuples = 0;

#if PG_VERSION_NUM >= 120000
	TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
#else
	TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc);
#endif

	/* Search closest probes lists */
	for (i = 0; i < so->ef_search; i++)
	{
		VertexID2Block(so, so->id[i], &searchPage, &offno);

		if (BlockNumberIsValid(searchPage))
		{
			buf = ReadBuffer(scan->indexRelation, searchPage);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			itup = (HnswflatVertex) PageGetItem(page, PageGetItemId(page, offno));

			/*
			 * Add virtual tuple
			 *
			 * Use procinfo from the index instead of scan key for
			 * performance
			 */
			ExecClearTuple(slot);
			slot->tts_values[0] = so->dis[i];
			slot->tts_isnull[0] = false;
			slot->tts_values[1] = PointerGetDatum(&itup->heap_ptr);
			slot->tts_isnull[1] = false;
			slot->tts_values[2] = Int32GetDatum((int) searchPage);
			slot->tts_isnull[2] = false;
			ExecStoreVirtualTuple(slot);

			tuplesort_puttupleslot(so->sortstate, slot);

			tuples++;

			UnlockReleaseBuffer(buf);
		}
	}

	tuplesort_performsort(so->sortstate);
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
hnswflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	HnswflatScanOpaque so;
    Buffer		buf;
	Page		page;
	int			lists;
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	scan = RelationGetIndexScan(index, nkeys, norderbys);


	so = (HnswflatScanOpaque) palloc(sizeof(HnswflatScanOpaqueData));
	so->buf = InvalidBuffer;
	so->first = true;

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, HNSWFLAT_DISTANCE_PROC);
	so->normprocinfo = HnswflatOptionalProcInfo(index, HNSWFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	/* Create tuple description for sorting */
#if PG_VERSION_NUM >= 120000
	so->tupdesc = CreateTemplateTupleDesc(3);
#else
	so->tupdesc = CreateTemplateTupleDesc(3, false);
#endif
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "tid", TIDOID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 3, "indexblkno", INT4OID, -1, 0);

	/* Prep sort */
	so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);

#if PG_VERSION_NUM >= 120000
	so->slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
#else
	so->slot = MakeSingleTupleTableSlot(so->tupdesc);
#endif

    buf = ReadBuffer(scan->indexRelation, HNSWFLAT_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

    meta = (HnswflatMetaPage) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    so->ep_id = meta->ep_id;
    so->ep_level = meta->ep_level;
    so->base_nb_num = meta->base_nb_num;
    so->ef_search = meta->ef_search;
    so->max_vertex_per_page = meta->max_vertex_per_page;

    UnlockReleaseBuffer(buf);

    so->id = (int64 *)palloc(sizeof(int64)*so->ef_search);
    so->dis = (double *)palloc(sizeof(double)*so->ef_search);

	scan->opaque = so;

	return scan;
}

/*
 * Start or restart an index scan
 */
void
hnswflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	HnswflatScanOpaque so = (HnswflatScanOpaque) scan->opaque;

#if PG_VERSION_NUM >= 130000
	if (!so->first)
		tuplesort_reset(so->sortstate);
#endif

	so->first = true;

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
bool
hnswflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
	HnswflatScanOpaque so = (HnswflatScanOpaque) scan->opaque;

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		Datum		value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan hnswflat index without order");

		/* No items will match if null */
		if (scan->orderByData->sk_flags & SK_ISNULL)
			return false;

		value = scan->orderByData->sk_argument;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		if (so->normprocinfo != NULL)
		{
			/* No items will match if normalization fails */
			if (!HnswflatNormValue(so->normprocinfo, so->collation, &value, NULL))
				return false;
		}

		HnswflatBench("InmemorySearch", InmemorySearch(scan, value));
        HnswflatBench("GetScanItems", GetScanItems(scan));

		so->first = false;

		/* Clean up if we allocated a new value */
		if (value != scan->orderByData->sk_argument)
			pfree(DatumGetPointer(value));
	}

	if (tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL))
	{
		ItemPointer tid = (ItemPointer) DatumGetPointer(slot_getattr(so->slot, 2, &so->isnull));
		BlockNumber indexblkno = DatumGetInt32(slot_getattr(so->slot, 3, &so->isnull));

#if PG_VERSION_NUM >= 120000
		scan->xs_heaptid = *tid;
#else
		scan->xs_ctup.t_self = *tid;
#endif

		if (BufferIsValid(so->buf))
			ReleaseBuffer(so->buf);

		/*
		 * An index scan must maintain a pin on the index page holding the
		 * item last returned by amgettuple
		 *
		 * https://www.postgresql.org/docs/current/index-locking.html
		 */
		so->buf = ReadBuffer(scan->indexRelation, indexblkno);

		scan->xs_recheckorderby = false;
		return true;
	}

	return false;
}

/*
 * End a scan and release resources
 */
void
hnswflatendscan(IndexScanDesc scan)
{
	HnswflatScanOpaque so = (HnswflatScanOpaque) scan->opaque;

	/* Release pin */
	if (BufferIsValid(so->buf))
		ReleaseBuffer(so->buf);

	tuplesort_end(so->sortstate);

	pfree(so);
	scan->opaque = NULL;
}
