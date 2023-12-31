#include "postgres.h"

#include "commands/vacuum.h"
#include "hnswflat.h"
#include "storage/bufmgr.h"

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *
hnswflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (info->analyze_only)
		return stats;

	/* stats is NULL if ambulkdelete not called */
	/* OK to return NULL if index not changed */
	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}
