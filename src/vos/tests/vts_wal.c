/**
 * (C) Copyright 2022 Intel Corporation.
 *
 * SPDX-License-Identifier: BSD-2-Clause-Patent
 */
/**
 * This file is part of vos/tests/
 *
 * vos/tests/vts_wal.c
 */
#define D_LOGFAC	DD_FAC(tests)

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vos_internal.h>
#include "vts_io.h"

#define WAL_IO_KEYS		31
#define WAL_IO_MULTI_KEYS	10000
#define WAL_OBJ_KEYS		31

static int type_list[] = {
	0,
	DAOS_OT_AKEY_UINT64,
	DAOS_OT_AKEY_LEXICAL,
	DAOS_OT_DKEY_UINT64,
	DAOS_OT_DKEY_LEXICAL,
	DAOS_OT_MULTI_UINT64,
	DAOS_OT_MULTI_LEXICAL
};

static int num_keys;
static enum daos_otype_t otype;

struct io_test_flag {
	char		*tf_str;
	unsigned int	 tf_bits;
};

static struct io_test_flag io_test_flags[] = {
	{ .tf_str = "default", .tf_bits = 0, },
	{ .tf_str = "ZC", .tf_bits = TF_ZERO_COPY, },
	{ .tf_str = "extent", .tf_bits = TF_REC_EXT, },
	{ .tf_str = "ZC + extent", .tf_bits = TF_ZERO_COPY | TF_REC_EXT, },
	{ .tf_str = NULL, },
};

/* mirror of enum in vos/tests/vts_common.c */
enum {
	TCX_NONE,
	TCX_PO_CREATE_OPEN,
	TCX_CO_CREATE,
	TCX_CO_OPEN,
	TCX_READY,
};

struct wal_test_args {
	char	*wta_clone;
	void	*wta_buf;
	int	 wta_buf_sz;
};

static int
teardown_wal_test(void **state)
{
	struct wal_test_args	*arg = *state;

	if (arg == NULL) {
		print_message("state not set, likely due to group-setup issue\n");
		return 0;
	}

	unlink(arg->wta_clone);
	D_FREE(arg->wta_clone);
	D_FREE(arg->wta_buf);
	D_FREE(arg);
	return 0;
}

static int
setup_wal_test(void **state)
{
	struct wal_test_args	*arg = NULL;
	char			*pool_name;
	int			 rc;

	D_ALLOC(arg, sizeof(struct wal_test_args));
	if (arg == NULL)
		return -1;

	arg->wta_buf_sz = (32UL << 20);	/* 32MB */
	D_ALLOC(arg->wta_buf, arg->wta_buf_sz);
	if (arg->wta_buf == NULL)
		goto error;

	D_ASPRINTF(arg->wta_clone, "%s/pool_clone", vos_path);
	if (arg->wta_clone == NULL)
		goto error;

	rc = vts_pool_fallocate(&pool_name);
	if (rc)
		goto error;

	rc = rename(pool_name, arg->wta_clone);
	if (rc) {
		unlink(pool_name);
		free(pool_name);
		goto error;
	}
	free(pool_name);

	*state = arg;
	return 0;
error:
	D_FREE(arg->wta_clone);
	D_FREE(arg->wta_buf);
	D_FREE(arg);
	return -1;
}

static int
copy_pool_file(struct wal_test_args *arg, const char *src_pool, const char *dst_pool)
{
	struct stat	lstat;
	uint64_t	copy_sz, left;
	int		src_fd, dst_fd, rc;

	rc = stat(src_pool, &lstat);
	if (rc != 0) {
		D_ERROR("Stat source pool:%s failed. %s\n", src_pool, strerror(errno));
		return -1;
	}

	src_fd = open(src_pool, O_RDONLY);
	if (src_fd < 0) {
		D_ERROR("Open source pool:%s failed. %s\n", src_pool, strerror(errno));
		return -1;
	}

	dst_fd = open(dst_pool, O_WRONLY);
	if (dst_fd < 0) {
		D_ERROR("Open dest pool:%s failed. %s\n", dst_pool, strerror(errno));
		close(src_fd);
		return -1;
	}

	left = lstat.st_size;
	while (left > 0) {
		ssize_t	read_sz;

		copy_sz = min(left, arg->wta_buf_sz);

		read_sz = read(src_fd, arg->wta_buf, copy_sz);
		if (read_sz < copy_sz) {
			D_ERROR("Failed to read "DF_U64" bytes from source pool:%s. %s\n",
				copy_sz, src_pool, strerror(errno));
			rc = -1;
			break;
		}

		read_sz = write(dst_fd, arg->wta_buf, copy_sz);
		if (read_sz < copy_sz) {
			D_ERROR("Failed to write "DF_U64" bytes to dest pool:%s. %s\n",
				copy_sz, dst_pool, strerror(errno));
			rc = -1;
			break;
		}
		left -= copy_sz;
	}

	close(src_fd);
	close(dst_fd);
	return rc;
}

static inline int
save_pool(struct wal_test_args *arg, const char *pool_name)
{
	return copy_pool_file(arg, pool_name, arg->wta_clone);
}

static inline int
restore_pool(struct wal_test_args *arg, const char *pool_name)
{
	return copy_pool_file(arg, arg->wta_clone, pool_name);
}

/* Create pool, clear content in tmpfs, open pool by meta blob loading & WAL replay */
static void
wal_tst_01(void **state)
{
	struct wal_test_args	*arg = *state;
	char			*pool_name;
	uuid_t			 pool_id;
	daos_handle_t		 poh;
	int			 rc;

	uuid_generate(pool_id);

	/* Create VOS pool file */
	rc = vts_pool_fallocate(&pool_name);
	assert_int_equal(rc, 0);

	/* Save the empty pool file */
	rc = save_pool(arg, pool_name);
	assert_int_equal(rc, 0);

	/* Create pool: Create meta & WAL blobs, write meta & WAL header */
	rc = vos_pool_create(pool_name, pool_id, 0, 0, 0, NULL);
	assert_int_equal(rc, 0);

	/* Restore pool content from the empty clone */
	rc = restore_pool(arg, pool_name);
	assert_int_equal(rc, 0);

	/* Open pool: Open meta & WAL blobs, load meta & WAL header, replay WAL */
	rc = vos_pool_open(pool_name, pool_id, 0, &poh);
	assert_int_equal(rc, 0);

	/* Close pool: Flush meta & WAL header, close meta & WAL blobs */
	rc = vos_pool_close(poh);
	assert_int_equal(rc, 0);

	/* Destroy pool: Destroy meta & WAL blobs */
	rc = vos_pool_destroy(pool_name, pool_id);
	assert_int_equal(rc, 0);

	free(pool_name);
}

/* Basic I/O tests */
static void
wal_pool_refill(struct vos_test_ctx *tcx)
{
	daos_handle_t	poh, coh;
	int		rc;

	rc = vos_cont_close(tcx->tc_co_hdl);
	assert_rc_equal(rc, 0);
	tcx->tc_step = TCX_CO_CREATE;

	poh = tcx->tc_po_hdl;
	rc = vos_pool_close(poh);
	assert_rc_equal(rc, 0);
	tcx->tc_step = TCX_NONE;

	poh = DAOS_HDL_INVAL;
	rc = vos_pool_open(tcx->tc_po_name, tcx->tc_po_uuid, 0, &poh);
	assert_rc_equal(rc, 0);
	tcx->tc_po_hdl = poh;
	tcx->tc_step = TCX_CO_CREATE;

	rc = vos_cont_open(poh, tcx->tc_co_uuid, &coh);
	assert_rc_equal(rc, 0);
	tcx->tc_co_hdl = coh;
	tcx->tc_step = TCX_READY;
}

static void
wal_args_reset(struct io_test_args *args)
{
	args->oid = gen_oid(otype);
	args->otype = otype;
	if (is_daos_obj_type_set(otype, DAOS_OT_AKEY_UINT64)) {
		args->akey = NULL;
		args->akey_size = sizeof(uint64_t);
	}
	if (is_daos_obj_type_set(otype, DAOS_OT_DKEY_UINT64)) {
		args->dkey = NULL;
		args->dkey_size = sizeof(uint64_t);
	}
	snprintf(args->fname, VTS_BUF_SIZE, "%s/vpool.test_%x",
		 vos_path, otype);
}

static int
setup_wal_io(void **state)
{
	int rc;

	rc = setup_io(state);
	if (rc == -1)
		return rc;

	wal_args_reset((struct io_test_args *)*state);
	return 0;
}

/* refill:true - perform the pool re-load and refill after every key update/punch */
static int
wal_update_and_fetch_dkey(struct io_test_args *arg, daos_epoch_t update_epoch,
			  daos_epoch_t fetch_epoch, char *update_buf, char *fetch_buf,
			  char *akey_buf, char *dkey_buf, bool refill)
{	int			rc = 0;
	d_iov_t			val_iov;
	daos_key_t		dkey;
	daos_key_t		akey;
	daos_recx_t		rex;
	char			verify_buf[UPDATE_BUF_SIZE];
	daos_iod_t		iod;
	d_sg_list_t		sgl;
	unsigned int		recx_size;
	unsigned int		recx_nr;
	bool			update, fetch;

	/* Setup */
	update = refill || (fetch_buf == NULL);
	fetch = (fetch_buf != NULL);

	memset(&iod, 0, sizeof(iod));
	memset(&rex, 0, sizeof(rex));
	memset(&sgl, 0, sizeof(sgl));

	if (arg->ta_flags & TF_REC_EXT) {
		iod.iod_type = DAOS_IOD_ARRAY;
		recx_size = UPDATE_REC_SIZE;
		recx_nr   = UPDATE_BUF_SIZE / UPDATE_REC_SIZE;
	} else {
		iod.iod_type = DAOS_IOD_SINGLE;
		recx_size = UPDATE_BUF_SIZE;
		recx_nr   = 1;
	}
	iod.iod_size	= recx_size;
	rex.rx_nr	= recx_nr;

	sgl.sg_nr = 1;
	sgl.sg_iovs = &val_iov;

	/* Generate a new A/D keys and data */
	if (update) {
		if (arg->ta_flags & TF_OVERWRITE) {
			memcpy(dkey_buf, last_dkey, arg->dkey_size);
			memcpy(akey_buf, last_akey, arg->akey_size);
		} else {
			vts_key_gen(dkey_buf, arg->dkey_size, true, arg);
			memcpy(last_dkey, dkey_buf, arg->dkey_size);

			vts_key_gen(akey_buf, arg->akey_size, false, arg);
			memcpy(last_akey, akey_buf, arg->akey_size);
		}

		dts_buf_render(update_buf, UPDATE_BUF_SIZE);
		d_iov_set(&val_iov, update_buf, UPDATE_BUF_SIZE);
	}

	set_iov(&dkey, dkey_buf,
		is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
	set_iov(&akey, akey_buf,
		is_daos_obj_type_set(arg->otype, DAOS_OT_AKEY_UINT64));

	rex.rx_idx	= hash_key(&dkey, is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
	iod.iod_name	= akey;
	iod.iod_recxs	= &rex;
	iod.iod_nr	= 1;

	if (update) {
		/* Act */
		rc = io_test_obj_update(arg, update_epoch, 0, &dkey, &iod, &sgl,
					NULL, true);
		if (rc)
			goto exit;

		/* Count */
		inc_cntr(arg->ta_flags);

		memset(verify_buf, 0, UPDATE_BUF_SIZE);
		d_iov_set(&val_iov, verify_buf, UPDATE_BUF_SIZE);
		iod.iod_size = DAOS_REC_ANY;

		/* Act again */
		rc = io_test_obj_fetch(arg, fetch_epoch, 0, &dkey, &iod, &sgl, true);
		if (rc)
			goto exit;

		/* Verify initialized data */
		if (arg->ta_flags & TF_REC_EXT)
			assert_int_equal(iod.iod_size, UPDATE_REC_SIZE);
		else
			assert_int_equal(iod.iod_size, UPDATE_BUF_SIZE);
		assert_memory_equal(update_buf, verify_buf, UPDATE_BUF_SIZE);
	}

	/* Refill VOS file from WAL: reopen pool & container */
	if (refill)
		wal_pool_refill(&arg->ctx);

	/* Verify reconstructed data */
	if (fetch) {
		d_iov_set(&val_iov, fetch_buf, UPDATE_BUF_SIZE);
		set_iov(&iod.iod_name, akey_buf,
			is_daos_obj_type_set(arg->otype, DAOS_OT_AKEY_UINT64));
		set_iov(&dkey, dkey_buf,
			is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
		rex.rx_idx = hash_key(&dkey,
				      is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
		iod.iod_size = DAOS_REC_ANY;

		rc = io_test_obj_fetch(arg, fetch_epoch, 0, &dkey, &iod, &sgl, true);
		if (rc) {
			print_error("Failed to fetch reconstructed data: "DF_RC"\n", DP_RC(rc));
			goto exit;
		}

		if (arg->ta_flags & TF_REC_EXT)
			assert_int_equal(iod.iod_size, UPDATE_REC_SIZE);
		else
			assert_int_equal(iod.iod_size, UPDATE_BUF_SIZE);
		assert_memory_equal(update_buf, fetch_buf, UPDATE_BUF_SIZE);
	}
exit:
	return rc;
}

static void
wal_io_multiple_refills(void **state)
{
	struct io_test_args	*arg = *state;
	daos_epoch_t		 epoch;
	char			*update_buf = NULL;
	char			*fetch_buf = NULL;
	char			*akey_buf = NULL;
	char			*dkey_buf = NULL;
	int			 i, j, rc = 0;

	num_keys = WAL_IO_KEYS;

	D_ALLOC_NZ(update_buf, UPDATE_BUF_SIZE);
	assert_rc_equal(!!update_buf, true);
	D_ALLOC(fetch_buf, UPDATE_BUF_SIZE);
	assert_rc_equal(!!fetch_buf, true);
	D_ALLOC_NZ(akey_buf, UPDATE_AKEY_SIZE);
	assert_rc_equal(!!akey_buf, true);
	D_ALLOC_NZ(dkey_buf, UPDATE_DKEY_SIZE);
	assert_rc_equal(!!dkey_buf, true);

	for (i = 0; io_test_flags[i].tf_str != NULL; i++) {
		print_message("\t%d) update/fetch/verify (%s) test, multiple pool refills\n",
			      i, io_test_flags[i].tf_str);

		/* Update/fetch/verify, refill and fetch/verify again */
		epoch = gen_rand_epoch();
		arg->ta_flags = io_test_flags[i].tf_bits;
		for (j = 0; j < num_keys; j++) {
			rc = wal_update_and_fetch_dkey(arg, epoch, epoch,
						       update_buf, fetch_buf, akey_buf, dkey_buf,
						       true); /* refill after each update */
			assert_rc_equal(rc, 0);
		}
	}
	D_FREE(update_buf);
	D_FREE(fetch_buf);
	D_FREE(akey_buf);
	D_FREE(dkey_buf);
}

static void
wal_io_multiple_updates(void **state)
{
	struct io_test_args	*arg = *state;
	daos_epoch_t		 epoch;
	char			*update_buf = NULL;
	char			*fetch_buf = NULL;
	char			*akey_buf = NULL;
	char			*dkey_buf = NULL;
	char			*up, *f, *ak, *dk;
	int			 i, j, rc = 0;

	num_keys = WAL_IO_MULTI_KEYS;

	D_ALLOC_NZ(update_buf, UPDATE_BUF_SIZE * num_keys);
	assert_rc_equal(!!update_buf, true);
	D_ALLOC(fetch_buf, UPDATE_BUF_SIZE * num_keys);
	assert_rc_equal(!!fetch_buf, true);
	D_ALLOC_NZ(akey_buf, UPDATE_AKEY_SIZE * num_keys);
	assert_rc_equal(!!akey_buf, true);
	D_ALLOC_NZ(dkey_buf, UPDATE_DKEY_SIZE * num_keys);
	assert_rc_equal(!!dkey_buf, true);

	for (i = 0; io_test_flags[i].tf_str != NULL; i++) {
		print_message("\t%d) %dK update/fetch/verify (%s), verify after pool refill\n",
			      i, num_keys/1000, io_test_flags[i].tf_str);

		/* Update/fetch/verify */
		up = update_buf;
		ak = akey_buf;
		dk = dkey_buf;
		epoch = gen_rand_epoch();
		arg->ta_flags = io_test_flags[i].tf_bits;
		for (j = 0; j < num_keys; j++) {
			rc = wal_update_and_fetch_dkey(arg, epoch, epoch,
						       up, NULL, ak, dk,
						       false); /* don't refill */
			assert_rc_equal(rc, 0);

			up += UPDATE_BUF_SIZE;
			ak += UPDATE_AKEY_SIZE;
			dk += UPDATE_DKEY_SIZE;
		}

		/* Refill VOS file from WAL: reopen pool & container */
		wal_pool_refill(&arg->ctx);

		/* Fetch/verify */
		up = update_buf;
		f = fetch_buf;
		ak = akey_buf;
		dk = dkey_buf;
		for (j = 0; j < num_keys; j++) {
			rc = wal_update_and_fetch_dkey(arg, epoch, epoch,
						       up, f, ak, dk,
						       false);
			assert_rc_equal(rc, 0);

			up += UPDATE_BUF_SIZE;
			f += UPDATE_BUF_SIZE;
			ak += UPDATE_AKEY_SIZE;
			dk += UPDATE_DKEY_SIZE;
		}
	}
	D_FREE(update_buf);
	D_FREE(fetch_buf);
	D_FREE(akey_buf);
	D_FREE(dkey_buf);
}

static void
update_dkey(void **state, daos_unit_oid_t oid, daos_epoch_t epoch,
	    uint64_t dkey_value, const char *val)
{
	struct io_test_args	*arg = *state;
	daos_iod_t		iod = {0};
	d_sg_list_t		sgl = {0};
	daos_key_t		dkey;
	daos_key_t		akey;
	d_iov_t			val_iov;
	daos_recx_t		recx;
	uint64_t		akey_value = 0;
	int			rc = 0;

	d_iov_set(&dkey, &dkey_value, sizeof(dkey_value));
	d_iov_set(&akey, &akey_value, sizeof(akey_value));

	iod.iod_type = DAOS_IOD_ARRAY;
	iod.iod_name = akey;
	iod.iod_recxs = &recx;
	iod.iod_nr = 1;

	/* Attach buffer to sgl */
	d_iov_set(&val_iov, &val, strnlen(val, 32) + 1);
	sgl.sg_iovs = &val_iov;
	sgl.sg_nr = 1;

	iod.iod_size = 1;
	/* Set up rexs */
	recx.rx_idx = 0;
	recx.rx_nr = val_iov.iov_len;

	rc = vos_obj_update(arg->ctx.tc_co_hdl, oid, epoch++, 0, 0, &dkey, 1,
			    &iod, NULL, &sgl);
	assert_rc_equal(rc, 0);
}

static void
wal_io_query_key_punch_update(void **state)
{
	struct io_test_args	*arg = *state;
	int			rc = 0;
	daos_epoch_t		epoch = 1;
	daos_key_t		dkey = { 0 };
	daos_key_t		akey;
	daos_recx_t		recx_read;
	daos_unit_oid_t		oid;
	uint64_t		dkey_value;
	uint64_t		akey_value = 0;

	d_iov_set(&akey, &akey_value, sizeof(akey_value));

	oid = gen_oid(arg->otype);

	update_dkey(state, oid, epoch++, 0, "World");
	update_dkey(state, oid, epoch++, 12, "Goodbye");

	rc = vos_obj_query_key(arg->ctx.tc_co_hdl, oid,
			       DAOS_GET_MAX | DAOS_GET_DKEY | DAOS_GET_RECX,
			       epoch++, &dkey, &akey, &recx_read, NULL, 0, 0, NULL);
	assert_rc_equal(rc, 0);
	assert_int_equal(recx_read.rx_idx, 0);
	assert_int_equal(recx_read.rx_nr, sizeof("Goodbye"));
	assert_int_equal(*(uint64_t *)dkey.iov_buf, 12);

	/* Now punch the last dkey */
	dkey_value = 12;
	d_iov_set(&dkey, &dkey_value, sizeof(dkey_value));
	rc = vos_obj_punch(arg->ctx.tc_co_hdl, oid, epoch++, 0, 0, &dkey, 0,
			   NULL, NULL);
	assert_rc_equal(rc, 0);

	rc = vos_obj_query_key(arg->ctx.tc_co_hdl, oid,
			       DAOS_GET_MAX | DAOS_GET_DKEY | DAOS_GET_RECX,
			       epoch++, &dkey, &akey, &recx_read, NULL, 0, 0, NULL);
	assert_rc_equal(rc, 0);
	assert_int_equal(recx_read.rx_idx, 0);
	assert_int_equal(recx_read.rx_nr, sizeof("World"));
	assert_int_equal(*(uint64_t *)dkey.iov_buf, 0);

	/* Ok, now update the last one again */
	update_dkey(state, oid, epoch++, 12, "Hello!");

	rc = vos_obj_query_key(arg->ctx.tc_co_hdl, oid,
			       DAOS_GET_MAX | DAOS_GET_DKEY | DAOS_GET_RECX,
			       epoch++, &dkey, &akey, &recx_read, NULL, 0, 0, NULL);
	assert_rc_equal(rc, 0);
	assert_int_equal(recx_read.rx_nr, sizeof("Hello!"));
	assert_int_equal(recx_read.rx_idx, 0);
	assert_int_equal(*(uint64_t *)dkey.iov_buf, 12);

	/* Refill VOS file from WAL: reopen pool & container */
	wal_pool_refill(&arg->ctx);

	/* Verify */
	rc = vos_obj_query_key(arg->ctx.tc_co_hdl, oid,
			       DAOS_GET_MAX | DAOS_GET_DKEY | DAOS_GET_RECX,
			       epoch++, &dkey, &akey, &recx_read, NULL, 0, 0, NULL);
	assert_rc_equal(rc, 0);
	assert_int_equal(recx_read.rx_nr, sizeof("Hello!"));
	assert_int_equal(recx_read.rx_idx, 0);
	assert_int_equal(*(uint64_t *)dkey.iov_buf, 12);
}

#define WAL_UPDATE_BUF_NR_SIZE 4
static uint64_t wal_key;

static inline void
wal_print_buf(char *buf, int val)
{
	char b[12];

	sprintf(b, "%0*d", WAL_UPDATE_BUF_NR_SIZE, val);
	memcpy(buf, b, WAL_UPDATE_BUF_NR_SIZE);
}

static inline void
wal_akey_gen(daos_key_t *akey, struct io_test_args *arg)
{
	char *buf = akey->iov_buf;

	if (is_daos_obj_type_set(arg->otype, DAOS_OT_AKEY_UINT64)) {
		memcpy(buf, &wal_key, sizeof(wal_key));
		akey->iov_len = akey->iov_buf_len = sizeof(wal_key);
	} else {
		akey->iov_len = akey->iov_buf_len =
			snprintf(buf, arg->akey_size,
				 "akey=%0*lu", WAL_UPDATE_BUF_NR_SIZE, wal_key);
	}
	wal_key++;
}

static inline void
wal_dkey_gen(d_iov_t *dkey, struct io_test_args *arg)
{
	char *buf = dkey->iov_buf;

	if (is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64)) {
		memcpy(buf, &wal_key, sizeof(wal_key));
		dkey->iov_len = dkey->iov_buf_len = sizeof(wal_key);
	} else {
		dkey->iov_len = dkey->iov_buf_len =
			snprintf(buf, arg->dkey_size,
				 "dkey=%0*lu", WAL_UPDATE_BUF_NR_SIZE, wal_key);
	}
	wal_key++;
}

static void
wal_objs_update_and_fetch(struct io_test_args *arg, daos_epoch_t epoch)
{
	daos_epoch_t	ep = epoch;
	int		obj_nr, dkey_nr, v_nr;
	int		oidx, didx, aidx, rc;
	d_iov_t		val_iov;
	daos_key_t	dkey;
	daos_recx_t	rex;
	daos_unit_oid_t	oids[num_keys];
	char		dkey_buf[UPDATE_DKEY_SIZE];
	char		akey_buf[UPDATE_AKEY_SIZE];
	char		update_buf[UPDATE_BUF_SIZE];
	char		fetch_buf[UPDATE_BUF_SIZE];
	daos_iod_t	iod;
	d_sg_list_t	sgl;
	bool		overwrite;

	wal_key = 1;
	obj_nr = dkey_nr = v_nr = num_keys;

	memset(&iod, 0, sizeof(iod));
	memset(&rex, 0, sizeof(rex));
	memset(&sgl, 0, sizeof(sgl));

	dts_buf_render(update_buf, UPDATE_BUF_SIZE);
	d_iov_set(&val_iov, update_buf, UPDATE_BUF_SIZE);
	sgl.sg_nr = 1;
	sgl.sg_nr_out = 0;
	sgl.sg_iovs = &val_iov;
	if (arg->ta_flags & TF_REC_EXT) {
		iod.iod_type	= DAOS_IOD_ARRAY;
		iod.iod_size	= UPDATE_REC_SIZE;
		rex.rx_nr	= UPDATE_BUF_SIZE / UPDATE_REC_SIZE;
	} else {
		iod.iod_type	= DAOS_IOD_SINGLE;
		iod.iod_size	= UPDATE_BUF_SIZE;
		rex.rx_nr	= 1;
	}

	iod.iod_recxs		= &rex;
	iod.iod_nr		= 1;
	iod.iod_name.iov_buf	= akey_buf;
	dkey.iov_buf		= dkey_buf;

	overwrite = (arg->ta_flags & TF_OVERWRITE);
	if (overwrite) {
		wal_dkey_gen(&dkey, arg);
		rex.rx_idx = hash_key(&dkey,
				      is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
		wal_akey_gen(&iod.iod_name, arg);
	}

	/* Update KVs */
	for (oidx = 0; oidx < obj_nr; oidx++) {
		arg->oid = oids[oidx] = gen_oid(arg->otype);

		for (didx = 0; didx < dkey_nr; didx++) {
			if (!overwrite) {
				wal_dkey_gen(&dkey, arg);
				rex.rx_idx = hash_key(&dkey,
					is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
			}

			for (aidx = 0; aidx < v_nr; aidx++) {
				wal_print_buf(update_buf, aidx + v_nr * (didx + dkey_nr * oidx));
				if (!overwrite)
					wal_akey_gen(&iod.iod_name, arg);

				rc = io_test_obj_update(arg, ep++, 0, &dkey, &iod, &sgl,
							NULL, true);
				assert_rc_equal(rc, 0);

				/* Count */
				inc_cntr(arg->ta_flags);
			}
		}
		/* Refill VOS file from WAL: reopen pool & container */
		if (oidx == 0)
			wal_pool_refill(&arg->ctx);
	}

	wal_key = 1;
	if (overwrite) {
		wal_dkey_gen(&dkey, arg);
		rex.rx_idx = hash_key(&dkey,
				      is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
		wal_akey_gen(&iod.iod_name, arg);
		ep = epoch;
	}

	d_iov_set(&val_iov, fetch_buf, UPDATE_BUF_SIZE);

	/* Fetch/verify KVs */
	for (oidx = 0; oidx < obj_nr; oidx++) {
		arg->oid = oids[oidx];

		for (didx = 0; didx < dkey_nr; didx++) {
			if (!overwrite) {
				wal_dkey_gen(&dkey, arg);
				rex.rx_idx = hash_key(&dkey,
					is_daos_obj_type_set(arg->otype, DAOS_OT_DKEY_UINT64));
			}

			for (aidx = 0; aidx < v_nr; aidx++) {
				wal_print_buf(update_buf, aidx + v_nr * (didx + dkey_nr * oidx));
				if (!overwrite)
					wal_akey_gen(&iod.iod_name, arg);

				iod.iod_size = DAOS_REC_ANY;

				rc = io_test_obj_fetch(arg, ep++, 0, &dkey, &iod, &sgl, true);
				assert_rc_equal(rc, 0);

				if (arg->ta_flags & TF_REC_EXT)
					assert_int_equal(iod.iod_size, UPDATE_REC_SIZE);
				else
					assert_int_equal(iod.iod_size, UPDATE_BUF_SIZE);
				assert_memory_equal(update_buf, fetch_buf, UPDATE_BUF_SIZE);
			}
		}
	}
}

static void
wal_io_multiple_objects(void **state)
{
	struct io_test_args	*arg = *state;
	daos_epoch_t		 epoch;
	int i;

	num_keys = WAL_OBJ_KEYS;

	for (i = 0; io_test_flags[i].tf_str != NULL; i++) {
		print_message("\t%d) multiple objects update (%s) test\n",
			      i, io_test_flags[i].tf_str);

		epoch = gen_rand_epoch();
		arg->ta_flags = io_test_flags[i].tf_bits;

		/* Update KVs in num_keys objects, refill pool and fetch/verify all values */
		wal_objs_update_and_fetch(arg, epoch);
	}
}

static void
wal_io_multiple_objects_ovwr(void **state)
{
	struct io_test_args	*arg = *state;
	daos_epoch_t		 epoch;
	int i;

	num_keys = WAL_OBJ_KEYS;

	for (i = 0; io_test_flags[i].tf_str != NULL; i++) {
		print_message("\t%d) multiple objects overwrite (%s) test\n",
			      i, io_test_flags[i].tf_str);

		epoch = gen_rand_epoch();
		arg->ta_flags = io_test_flags[i].tf_bits;
		arg->ta_flags |= TF_OVERWRITE;

		/*
		 * Update same key value in num_keys objects,
		 * refill pool and fetch/verify the values
		 **/
		wal_objs_update_and_fetch(arg, epoch);
	}
}

static const struct CMUnitTest wal_tests[] = {
	{ "WAL01: Basic pool operations",
	  wal_tst_01, NULL, NULL },
};

static const struct CMUnitTest wal_io_tests[] = {
	{ "WAL10: Update/fetch/verify test",
	  wal_io_multiple_refills, NULL, NULL },
	{ "WAL11: 10K update/fetch/verify test",
	  wal_io_multiple_updates, NULL, NULL },
	{ "WAL12: Objects Update(overwrite)/fetch test",
	  wal_io_multiple_objects_ovwr, NULL, NULL },
	{ "WAL13: Objects Update/fetch test",
	  wal_io_multiple_objects, NULL, NULL },
};

static const struct CMUnitTest wal_io_int_tests[] = {
	{ "WAL20: Key query punch with subsequent update",
	  wal_io_query_key_punch_update, NULL, NULL },
};

int
run_wal_tests(const char *cfg)
{
	char		 test_name[DTS_CFG_MAX];
	const char	*akey = "hashed";
	const char	*dkey = "hashed";
	int		 i, rc;
	unsigned int	 val = 0;

	dts_create_config(test_name, "WAL Pool tests %s", cfg);
	D_PRINT("Running %s\n", test_name);

	d_getenv_int("DAOS_MD_ON_SSD", &val);
	if (val == 0) {
		print_message("MD_ON_SSD isn't enabled, skip test\n");
		return 0;
	}

	rc = cmocka_run_group_tests_name(test_name, wal_tests, setup_wal_test,
					   teardown_wal_test);

	for (i = 0; i < (sizeof(type_list) / sizeof(int)); i++) {
		otype = type_list[i];
		if (is_daos_obj_type_set(otype, DAOS_OT_DKEY_UINT64))
			dkey = "uint";
		if (is_daos_obj_type_set(otype, DAOS_OT_DKEY_LEXICAL))
			dkey = "lex";
		if (is_daos_obj_type_set(otype, DAOS_OT_AKEY_UINT64))
			akey = "uint";
		if (is_daos_obj_type_set(otype, DAOS_OT_AKEY_LEXICAL))
			akey = "lex";
		dts_create_config(test_name,
				  "WAL# Basic I/O tests dkey=%-6s akey=%s %s",
				  dkey, akey, cfg);
		test_name[3] = '1';
		D_PRINT("Running %s\n", test_name);
		rc += cmocka_run_group_tests_name(test_name, wal_io_tests,
						  setup_wal_io, teardown_io);
		if (otype == DAOS_OT_MULTI_UINT64) {
			test_name[3] = '2';
			D_PRINT("Running %s\n", test_name);
			rc += cmocka_run_group_tests_name(test_name, wal_io_int_tests,
							  setup_wal_io, teardown_io);
		}
	}
	return rc;
}
