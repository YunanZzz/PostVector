#include "postgres.h"

#include <float.h>

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "hnswflat.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#if PG_VERSION_NUM >= 120000
#include "commands/progress.h"
#endif

static relopt_kind hnswflat_relopt_kind;

/*
 * Initialize index options and variables
 */
void
_PG_init(void)
{
	hnswflat_relopt_kind = add_reloption_kind();
	add_int_reloption(hnswflat_relopt_kind, "base_nb_num", "Max number of neighbors for each layer",
					  HNSWFLAT_DEFAULT_BNN, 5, HNSWFLAT_MAX_BNN
#if PG_VERSION_NUM >= 130000
					  ,AccessExclusiveLock
#endif
		);

    add_int_reloption(hnswflat_relopt_kind, "ef_build", "efConstruction for HNSW",
					  HNSWFLAT_DEFAULT_EFB, 10, HNSWFLAT_MAX_EFB
#if PG_VERSION_NUM >= 130000
					  ,AccessExclusiveLock
#endif
		);

    add_int_reloption(hnswflat_relopt_kind, "ef_search", "ef for HNSW",
					  HNSWFLAT_DEFAULT_EFS, 10, HNSWFLAT_MAX_EFS
#if PG_VERSION_NUM >= 130000
					  ,AccessExclusiveLock
#endif
		);
}

/*
 * Get the name of index build phase
 */
#if PG_VERSION_NUM >= 120000
static char *
hnswflatbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_HNSWFLAT_PHASE_LOAD:
			return "loading tuples";
		default:
			return NULL;
	}
}
#endif

/*
 * Estimate the cost of an index scan
 */
static void
hnswflatcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
    IndexOptInfo *index = path->indexinfo;
#if PG_VERSION_NUM < 120000
    List *qinfos;
#endif
    GenericCosts costs;

#if PG_VERSION_NUM < 120000
    // Do preliminary analysis of indexquals
    qinfos = deconstruct_indexquals(path);
#endif

    MemSet(&costs, 0, sizeof(costs));

    // We have to visit all index tuples anyway
    costs.numIndexTuples = index->tuples;
    
    // Use generic estimate
#if PG_VERSION_NUM >= 120000
    genericcostestimate(root, path, loop_count, &costs);
#else 
    genericcostestimate(root, path, loop_count, qinfos, &costs);
#endif

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
hnswflatoptions(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
      {"base_nb_num", RELOPT_TYPE_INT, offsetof(HnswflatOptions, base_nb_num)},
      {"ef_build", RELOPT_TYPE_INT, offsetof(HnswflatOptions, ef_build)},
      {"ef_search", RELOPT_TYPE_INT, offsetof(HnswflatOptions, ef_search)},
  };

#if PG_VERSION_NUM >= 130000
	return (bytea *) build_reloptions(reloptions, validate,
									  hnswflat_relopt_kind,
									  sizeof(HnswflatOptions),
									  tab, lengthof(tab));
#else
	relopt_value *options;
	int			numoptions;
	HnswflatOptions *rdopts;

	options = parseRelOptions(reloptions, validate, hnswflat_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(HnswflatOptions), options, numoptions);
	fillRelOptions((void *) rdopts, sizeof(HnswflatOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	return (bytea *) rdopts;
#endif
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
hnswflatvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswflathandler);
Datum
hnswflathandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 4;
#if PG_VERSION_NUM >= 130000
	amroutine->amoptsprocnum = 0;
#endif
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
#if PG_VERSION_NUM >= 130000
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
#endif
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = hnswflatbuild;
	amroutine->ambuildempty = hnswflatbuildempty;
	amroutine->aminsert = hnswflatinsert;
	amroutine->ambulkdelete = NULL;
	amroutine->amvacuumcleanup = hnswflatvacuumcleanup;
	amroutine->amcanreturn = NULL;	/* tuple not included in heapsort */
	amroutine->amcostestimate = hnswflatcostestimate;
	amroutine->amoptions = hnswflatoptions;
	amroutine->amproperty = NULL;	/* TODO AMPROP_DISTANCE_ORDERABLE */
#if PG_VERSION_NUM >= 120000
	amroutine->ambuildphasename = hnswflatbuildphasename;
#endif
	amroutine->amvalidate = hnswflatvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = hnswflatbeginscan;
	amroutine->amrescan = hnswflatrescan;
	amroutine->amgettuple = hnswflatgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = hnswflatendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}
