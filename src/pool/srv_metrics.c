/**
 * (C) Copyright 2021-2024 Intel Corporation.
 *
 * SPDX-License-Identifier: BSD-2-Clause-Patent
 */

#define D_LOGFAC DD_FAC(pool)

#include "srv_internal.h"
#include <abt.h>
#include <daos/metrics.h>
#include <gurt/telemetry_producer.h>

/**
 * Initializes the pool metrics
 */
void *
ds_pool_metrics_alloc(const char *path, int tgt_id)
{
	struct pool_metrics	*metrics;
	struct d_tm_node_t	*started;
	int			 rc;

	D_ASSERT(tgt_id < 0);

	D_ALLOC_PTR(metrics);
	if (metrics == NULL)
		return NULL;

	rc = d_tm_add_metric(&started, D_TM_TIMESTAMP,
			     "Last time the pool started", NULL,
			     "%s/started_at", path);
	if (rc != 0) /* Probably a bad sign, but not fatal */
		D_WARN("Failed to create started_timestamp metric: "DF_RC"\n", DP_RC(rc));
	else
		d_tm_record_timestamp(started);

	rc = d_tm_add_metric(&metrics->evict_total, D_TM_COUNTER,
			     "Total number of pool handle evict operations", "ops",
			     "%s/ops/pool_evict", path);
	if (rc != 0)
		D_WARN("Failed to create evict hdl counter: "DF_RC"\n", DP_RC(rc));

	rc = d_tm_add_metric(&metrics->connect_total, D_TM_COUNTER,
			     "Total number of processed pool connect operations", "ops",
			     "%s/ops/pool_connect", path);
	if (rc != 0)
		D_WARN("Failed to create pool connect counter: "DF_RC"\n", DP_RC(rc));

	rc = d_tm_add_metric(&metrics->disconnect_total, D_TM_COUNTER,
			     "Total number of processed pool disconnect operations", "ops",
			     "%s/ops/pool_disconnect", path);
	if (rc != 0)
		D_WARN("Failed to create pool disconnect counter: "DF_RC"\n", DP_RC(rc));

	rc = d_tm_add_metric(&metrics->query_total, D_TM_COUNTER,
			     "Total number of processed pool query operations", "ops",
			     "%s/ops/pool_query", path);
	if (rc != 0)
		D_WARN("Failed to create pool query counter: "DF_RC"\n", DP_RC(rc));

	rc = d_tm_add_metric(&metrics->query_space_total, D_TM_COUNTER,
			     "Total number of processed pool query (with space) operations", "ops",
			     "%s/ops/pool_query_space", path);
	if (rc != 0)
		D_WARN("Failed to create pool query space counter: "DF_RC"\n", DP_RC(rc));

	rc = d_tm_add_metric(&metrics->service_leader, D_TM_GAUGE, "Pool service leader rank", NULL,
			     "%s/svc/leader", path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool service leader metric");

	rc = d_tm_add_metric(&metrics->map_version, D_TM_COUNTER, "Pool map version", NULL,
			     "%s/svc/map_version", path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool map version metric");

	rc = d_tm_add_metric(&metrics->open_handles, D_TM_GAUGE, "Pool handles held by clients",
			     NULL, "%s/svc/open_pool_handles", path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool handle metric");

	rc = d_tm_add_metric(&metrics->total_ranks, D_TM_GAUGE, "Pool storage ranks (total)", NULL,
			     "%s/svc/total_ranks", path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool total_ranks metric");

	rc = d_tm_add_metric(&metrics->degraded_ranks, D_TM_GAUGE, "Pool storage ranks (degraded)",
			     NULL, "%s/svc/degraded_ranks", path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool degraded_ranks metric");

	rc = d_tm_add_metric(&metrics->total_targets, D_TM_GAUGE, "Pool storage targets (total)",
			     NULL, "%s/svc/total_targets", path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool total_targets metric");

	rc = d_tm_add_metric(&metrics->draining_targets, D_TM_GAUGE,
			     "Pool storage targets (draining)", NULL, "%s/svc/draining_targets",
			     path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool draining_targets metric");

	rc = d_tm_add_metric(&metrics->disabled_targets, D_TM_GAUGE,
			     "Pool storage targets (disabled)", NULL, "%s/svc/disabled_targets",
			     path);
	if (rc != 0)
		DL_WARN(rc, "Failed to create pool disabled_targets metric");

	return metrics;
}

int
ds_pool_metrics_count(void)
{
	return (sizeof(struct pool_metrics) / sizeof(struct d_tm_node_t *));
}

/**
 * Release the pool metrics
 */
void
ds_pool_metrics_free(void *data)
{
	D_FREE(data);
}

/**
 * Generate the metrics path for a specific pool UUID.
 *
 * \param[in]	pool_uuid	UUID of the pool
 * \param[out]	path		Path to the pool metrics
 * \param[in]	path_len	Length of path array
 */
static void
pool_metrics_gen_path(const uuid_t pool_uuid, char *path, size_t path_len)
{
	snprintf(path, path_len, "pool/"DF_UUIDF, DP_UUID(pool_uuid));
	path[path_len - 1] = '\0';
}

static int
get_pool_dir_size(void)
{
	return dss_module_nr_pool_metrics() * PER_METRIC_BYTES;
}

/**
 * Add metrics for a specific pool.
 *
 * \param[in]	pool	pointer to ds_pool structure
 */
int
ds_pool_metrics_start(struct ds_pool *pool)
{
	int rc;

	pool_metrics_gen_path(pool->sp_uuid, pool->sp_path,
			      sizeof(pool->sp_path));

	/** create new shmem space for per-pool metrics */
	rc = d_tm_add_ephemeral_dir(NULL, get_pool_dir_size(), pool->sp_path);
	if (rc != 0) {
		D_WARN(DF_UUID ": failed to create metrics dir for pool: "
		       DF_RC "\n", DP_UUID(pool->sp_uuid), DP_RC(rc));
		return rc;
	}

	/* initialize metrics on the system xstream for each module */
	rc = dss_module_init_metrics(DAOS_SYS_TAG, pool->sp_metrics,
				     pool->sp_path, -1);
	if (rc != 0) {
		D_WARN(DF_UUID ": failed to initialize module metrics: "
		       DF_RC"\n", DP_UUID(pool->sp_uuid), DP_RC(rc));
		ds_pool_metrics_stop(pool);
		return rc;
	}

	D_INFO(DF_UUID ": created metrics for pool\n", DP_UUID(pool->sp_uuid));

	return 0;
}

/**
 * Destroy metrics for a specific pool.
 *
 * \param[in]	pool	pointer to ds_pool structure
 */
void
ds_pool_metrics_stop(struct ds_pool *pool)
{
	int rc;

	dss_module_fini_metrics(DAOS_SYS_TAG, pool->sp_metrics);

	rc = d_tm_del_ephemeral_dir(pool->sp_path);
	if (rc != 0) {
		D_WARN(DF_UUID ": failed to remove pool metrics dir for pool: "
		       DF_RC"\n", DP_UUID(pool->sp_uuid), DP_RC(rc));
		return;
	}

	D_INFO(DF_UUID ": destroyed ds_pool metrics\n", DP_UUID(pool->sp_uuid));
}
