#include "postgres.h"

#include "hnswflat.h"
#include "storage/bufmgr.h"
#include "vector.h"

/*
 * Allocate a vector array
 */
VectorArray
VectorArrayInit(int maxlen, int dimensions)
{
	VectorArray res = palloc(sizeof(VectorArrayData));

	res->length = 0;
	res->maxlen = maxlen;
	res->dim = dimensions;
	res->items = palloc_extended(maxlen * VECTOR_SIZE(dimensions), MCXT_ALLOC_ZERO | MCXT_ALLOC_HUGE);
	return res;
}

/*
 * Free a vector array
 */
void
VectorArrayFree(VectorArray arr)
{
	pfree(arr->items);
	pfree(arr);
}

/*
 * Print vector array - useful for debugging
 */
void
PrintVectorArray(char *msg, VectorArray arr)
{
	int			i;

	for (i = 0; i < arr->length; i++)
		PrintVector(msg, VectorArrayGet(arr, i));
}

/*
 * Get base neighbor number in the index
 */
int
HnswflatGetBnn(Relation index)
{
	HnswflatOptions *opts = (HnswflatOptions *) index->rd_options;

	if (opts)
		return opts->base_nb_num;

	return HNSWFLAT_DEFAULT_BNN;
}

/*
 * Get efConstruction in the index
 */
int
HnswflatGetEfb(Relation index)
{
	HnswflatOptions *opts = (HnswflatOptions *) index->rd_options;

	if (opts)
		return opts->ef_build;

	return HNSWFLAT_DEFAULT_EFB;
}

/*
 * Get ef in the index
 */
int
HnswflatGetEfs(Relation index)
{
	HnswflatOptions *opts = (HnswflatOptions *) index->rd_options;

	if (opts)
		return opts->ef_search;

	return HNSWFLAT_DEFAULT_EFS;
}

/*
 * Get proc
 */
FmgrInfo *
HnswflatOptionalProcInfo(Relation rel, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(rel, 1, procnum)))
		return NULL;

	return index_getprocinfo(rel, 1, procnum);
}

/*
 * Divide by the norm
 *
 * Returns false if value should not be indexed
 *
 * The caller needs to free the pointer stored in value
 * if it's different than the original value
 */
bool
HnswflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector * result)
{
	Vector	   *v;
	int			i;
	double		norm;

	norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

	if (norm > 0)
	{
		v = DatumGetVector(*value);

		if (result == NULL)
			result = InitVector(v->dim);

		for (i = 0; i < v->dim; i++)
			result->x[i] = v->x[i] / norm;

		*value = PointerGetDatum(result);

		return true;
	}

	return false;
}

/*
 * New buffer
 */
Buffer
HnswflatNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * Init page
 */
void
HnswflatInitPage(Buffer buf, Page page)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(HnswflatPageOpaqueData));
	HnswflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	HnswflatPageGetOpaque(page)->page_id = HNSWFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void
HnswflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	HnswflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
HnswflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void
HnswflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
	/* Get new buffer */
	Buffer		newbuf = HnswflatNewBuffer(index, forkNum);
	Page		newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Update the previous buffer */
	HnswflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Init new page */
	HnswflatInitPage(newbuf, newpage);

	/* Commit */
	MarkBufferDirty(*buf);
	MarkBufferDirty(newbuf);
	GenericXLogFinish(*state);

	/* Unlock */
	UnlockReleaseBuffer(*buf);

	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
	*buf = newbuf;
}
