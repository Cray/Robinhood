/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 *  vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2016 Seagate Technology LLC
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the CeCILL License.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license (http://www.cecill.info) and that you
 * accept its terms.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "Basic.h"

#include <pthread.h>
#include <sys/time.h>

#include "chglog_reader.h"
#include "entry_proc_hash.h"
#include "Memory.h"
#include "entry_processor.h"
#include "rbh_logs.h"
#include "chglog_postproc.h"
#include "rbh_modules.h"

#include <lustre/lustreapi.h>

#if HAVE_DECL_CLF_RENAME
    #define PROCESS_LOG_ONE_REC CLF_RENAME
#elif HAVE_CHANGELOG_EXTEND_REC
    #define PROCESS_LOG_ONE_REC CLF_EXT_VERSION
#else
    #define PROCESS_LOG_ONE_REC 0
#endif
#define TEST_LUSTRE_FID_OID 400000
#define TEST_LUSTRE_FID_SEQ 1

extern chglog_reader_config_t cl_reader_config;

static struct reader_thr_info_t thread_info;

static entry_id_t basic_fid = {
    .f_seq = TEST_LUSTRE_FID_SEQ,
    .f_oid = TEST_LUSTRE_FID_OID,
    .f_ver = 0
};

static entry_id_t zero_fid = { .f_seq = 0, .f_oid = 0, .f_ver = 0};

#define CHANGELOG_RECORD(_rt, _idx, _tfid, _pfid, _name,                   \
                         _sfid, _spfid, _sname)                            \
{                                                                          \
    .rec_type = (_rt), .idx = (_idx), .tfid = (_tfid), .pfid = (_pfid),    \
    .name = (_name), .sfid = (_sfid), .spfid = (_spfid), .sname = (_sname) \
}

#define SHORT_RECORD(_rt, _idx, _tfid, _pfid, _name)           \
    CHANGELOG_RECORD((_rt), (_idx), (_tfid), (_pfid), (_name), \
                     zero_fid, zero_fid, NULL)

#define RENAME_RECORD(_idx, _pfid, _name, _sfid, _spfid, _sname)    \
    CHANGELOG_RECORD(CL_RENAME, (_idx), zero_fid, (_pfid), (_name), \
                     (_sfid), (_spfid), (_sname))

struct changelog_record_descr {
    uint32_t    rec_type;
    uint64_t    idx;
    entry_id_t  tfid;
    entry_id_t  pfid;
    const char *name;
    entry_id_t  sfid;
    entry_id_t  spfid;
    const char *sname;
};

int cl_rec_alloc_fill(CL_REC_TYPE **rec, struct changelog_record_descr *crd);

void changelog_test_init(void);
void changelog_test_fini(void);

#define UNIT_TEST(test_name)  \
    void test_name(void); \
    void test_name(void)

#define UNIT_TEST_INFO(test_name) \
{#test_name, (test_name)}

UNIT_TEST(process_log_rec_invalid_test)
{
    CL_REC_TYPE rec;
    int         rc;

    rec.cr_type = -1;
    rc = process_log_rec(&thread_info, &rec);
    CU_ASSERT_EQUAL(rc, EINVAL);

    rec.cr_type = CL_LAST;
    rc = process_log_rec(&thread_info, &rec);
    CU_ASSERT_EQUAL(rc, EINVAL);
}

UNIT_TEST(process_log_rec_ignore_test)
{
    CL_REC_TYPE              rec;
    int                      rc;

    thread_info.suppressed_records = 0;
    thread_info.interesting_records = 0;

    /* Robinhood always ignores CL_MARK records. See definition of
     * record_filters in src/chglog_reader/chglog_reader.c
     **/
    rec.cr_type = CL_MARK;
    rc = process_log_rec(&thread_info, &rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 1);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 0);
}

static int process_log_rec_record_count(void)
{
    int                  n_records = 0;
    int                  i;
    struct id_hash_slot *slot;
    entry_proc_op_t     *op;

    for (i = 0; i < thread_info.id_hash->hash_size; ++i)
    {
        slot = thread_info.id_hash->slot + i;
        rh_list_for_each_entry(op, &slot->list, id_hash_list)
            ++n_records;
    }

    return n_records;
}

static void process_log_rec_queue_cleanup(void)
{
    entry_proc_op_t *op;

    while(!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue,
                                 entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);

        EntryProcessor_Release(op);

        thread_info.op_queue_count --;
    }
}

#if defined(HAVE_CHANGELOG_EXTEND_REC) || defined(HAVE_FLEX_CL)

UNIT_TEST(process_log_rec_rename_one_zero_tfid_test)
{
#define TEST_SHORT_NAME "one_zero_tfid_test"
    CL_REC_TYPE             *rec;
    int                      rc;
    int                      n_records;
    const char               source_name[]= TEST_SHORT_NAME "_source";
    const char               target_name[]= TEST_SHORT_NAME "_target";
    size_t                   record_size;

    record_size = sizeof(CL_REC_TYPE) +
#if defined(HAVE_FLEX_CL)
                  sizeof(struct changelog_ext_rename) +
#endif
                  strlen(source_name) + strlen(target_name) + 2;
    rec = (CL_REC_TYPE*)MemAlloc(record_size);
    memset(rec, 0, record_size);
    CU_ASSERT_NOT_EQUAL_FATAL(rec, NULL);

    rec->cr_type = CL_RENAME;
    rec->cr_flags = PROCESS_LOG_ONE_REC;
    rec->cr_tfid.f_oid = 0;
    rec->cr_tfid.f_seq = 0;

    sprintf(rh_get_cl_cr_name(rec), "%s", target_name);
    sprintf((char*)changelog_rec_sname(rec), "%s", source_name);

    rec->cr_namelen = strlen(target_name) + strlen(source_name) + 1;

    rc = process_log_rec(&thread_info, rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 1);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);

    MemFree(rec);

#undef TEST_SHORT_NAME
}

UNIT_TEST(process_log_rec_rename_non_zero_tfid_test)
{
#define TEST_SHORT_NAME "non_zero_tfid_test"

    CL_REC_TYPE             *rec;
    int                      rc;
    int                      n_records;
    const char               source_name[]= TEST_SHORT_NAME "_source";
    const char               target_name[]= TEST_SHORT_NAME "_target";
    size_t                   record_size;

    record_size = sizeof(CL_REC_TYPE) +
#if defined(HAVE_FLEX_CL)
                  sizeof(struct changelog_ext_rename) +
#endif
                  strlen(source_name) + strlen(target_name) + 2;
    rec = (CL_REC_TYPE*)MemAlloc(record_size);
    memset(rec, 0, record_size);
    CU_ASSERT_NOT_EQUAL_FATAL(rec, NULL);

    rec->cr_type = CL_RENAME;
    rec->cr_flags = PROCESS_LOG_ONE_REC;
    rec->cr_tfid.f_oid = TEST_LUSTRE_FID_OID;
    rec->cr_tfid.f_seq = TEST_LUSTRE_FID_SEQ;

    sprintf(rh_get_cl_cr_name(rec), "%s", target_name);
    sprintf((char*)changelog_rec_sname(rec), "%s", source_name);

    rec->cr_namelen = strlen(target_name) + strlen(source_name) + 1;

    rc = process_log_rec(&thread_info, rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 1);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 3);

    MemFree(rec);

#undef TEST_SHORT_NAME
}
#endif

UNIT_TEST(process_log_rec_rename_ext_rec_one_zero_tfid_test)
{
#define TEST_SHORT_NAME "ext_rec_one_zero_tfid_test"
    CL_REC_TYPE             *rec;
    CL_REC_TYPE             *ext_rec;
    int                      rc;
    int                      n_records;

#if defined(HAVE_FLEX_CL)
    rec = (CL_REC_TYPE*)MemAlloc(sizeof(CL_REC_TYPE) +
                                 sizeof(struct changelog_ext_rename));
#else
    rec = (CL_REC_TYPE*)MemAlloc(sizeof(CL_REC_TYPE));
#endif
    CU_ASSERT_NOT_EQUAL_FATAL(rec, NULL);

    rec->cr_type = CL_RENAME;
    rec->cr_flags = 0;
    rec->cr_tfid.f_oid = 0;
    rec->cr_tfid.f_seq = 0;

    /* We don't need source and target file names for this test. */
    rec->cr_namelen = 0;

    rc = process_log_rec(&thread_info, rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 1);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 0);

    ext_rec = (CL_REC_TYPE*)MemAlloc(sizeof(CL_REC_TYPE));
    CU_ASSERT_NOT_EQUAL_FATAL(ext_rec, NULL);
    memset(ext_rec, 0, sizeof(*ext_rec));

    ext_rec->cr_type = CL_EXT;
    ext_rec->cr_tfid.f_oid = 0;
    ext_rec->cr_tfid.f_seq = 0;
    ext_rec->cr_namelen = 0;

    rc = process_log_rec(&thread_info, ext_rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 2);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);

    MemFree(rec);
    MemFree(ext_rec);

#undef TEST_SHORT_NAME
}

UNIT_TEST(process_log_rec_rename_ext_rec_non_zero_tfid_test)
{
#define TEST_SHORT_NAME "ext_rec_non_zero_tfid_test"
    CL_REC_TYPE             *rec;
    CL_REC_TYPE             *ext_rec;
    int                      rc;
    int                      n_records;
    const char               source_name[]= TEST_SHORT_NAME "_source";
    const char               target_name[]= TEST_SHORT_NAME "_target";
    size_t                   record_size;

#if defined(HAVE_FLEX_CL)
    rec = (CL_REC_TYPE*)malloc(sizeof(CL_REC_TYPE) +
                               sizeof(struct changelog_ext_rename));
#else
    rec = (CL_REC_TYPE*)malloc(sizeof(CL_REC_TYPE));
#endif
    CU_ASSERT_NOT_EQUAL_FATAL(rec, NULL);

    rec->cr_type = CL_RENAME;
    rec->cr_flags = 0;
    rec->cr_tfid.f_oid = TEST_LUSTRE_FID_OID;
    rec->cr_tfid.f_seq = TEST_LUSTRE_FID_SEQ;

    /* Have no need to have source and target file names here. */
    rec->cr_namelen = 0;

    rc = process_log_rec(&thread_info, rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 1);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 0);

    record_size = sizeof(CL_REC_TYPE) +
#if defined(HAVE_FLEX_CL)
                  sizeof(struct changelog_ext_rename) +
#endif
                  strlen(source_name) + strlen(target_name) + 2;
    ext_rec = (CL_REC_TYPE*)MemAlloc(record_size);
    memset(ext_rec, 0, record_size);
    CU_ASSERT_NOT_EQUAL_FATAL(ext_rec, NULL);

    ext_rec->cr_type = CL_EXT;
    ext_rec->cr_flags = PROCESS_LOG_ONE_REC;
    ext_rec->cr_tfid.f_oid = TEST_LUSTRE_FID_OID;
    ext_rec->cr_tfid.f_seq = TEST_LUSTRE_FID_SEQ;

    sprintf(rh_get_cl_cr_name(ext_rec), "%s", target_name);
    sprintf((char*)changelog_rec_sname(ext_rec), "%s", source_name);

    ext_rec->cr_namelen = 0;

    rc = process_log_rec(&thread_info, ext_rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 2);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 3);

    MemFree(rec);
    MemFree(ext_rec);

#undef TEST_SHORT_NAME
}

UNIT_TEST(process_log_rec_unlink)
{
#define TEST_SHORT_NAME "unlink"
    CL_REC_TYPE *rec;
    int          rc;
    int          n_records;
    const char   target_name[] = TEST_SHORT_NAME "_target";
    size_t       record_size;

    record_size = sizeof(CL_REC_TYPE) +
                  strlen(target_name) + 1;
    rec = (CL_REC_TYPE*)MemAlloc(record_size);
    CU_ASSERT_NOT_EQUAL_FATAL(rec, NULL);
    memset(rec, 0, record_size);

    rec->cr_type = CL_UNLINK;
    rec->cr_tfid.f_oid = TEST_LUSTRE_FID_OID;
    rec->cr_tfid.f_seq = TEST_LUSTRE_FID_SEQ;

    sprintf(rh_get_cl_cr_name(rec), "%s", target_name);
    rec->cr_namelen = strlen(target_name);

    rc = process_log_rec(&thread_info, rec);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, 1);

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 1);

    MemFree(rec);

#undef TEST_SHORT_NAME
}

/** @{
 * The following test cases cover all functionality of
 * @ref unlink_compact_wrapper():
 * Starting record | Final record
 * ----------------|-------------
 * CL_CREATE       | CL_UNLINK
 * CL_CREATE       | CL_RENAME
 * CL_MKDIR        | CL_RENAME
 * CL_MKDIR        | CL_RMDIR
 * CL_EXT          | CL_UNLINK
 * CL_EXT          | CL_RMDIR
 * CL_EXT          | CL_RENAME
 * CL_HARDLINK     | CL_UNLINK
 * CL_HARDLINK     | CL_RENAME
 * CL_SOFTLINK     | CL_UNLINK
 * CL_SOFTLINK     | CL_RENAME
 *
 * @Note
 * * CL_EXT record is to be created as result of CL_RENAME record handling.
 * * Each CL_UNLINK final record to be tested in two variants: last and interim.
 * * Each starting and final records are to be separated with something like
 * CL_MKDIR, CL_CREATE, and so on with different target FID.
 */

int cl_rec_alloc_fill(CL_REC_TYPE **rec, struct changelog_record_descr *crd)
{
    size_t                       record_size;
#if defined(HAVE_FLEX_CL)
    struct changelog_ext_rename *ext_rec;
#endif

    if (   rec == NULL
        || crd->rec_type >= CL_LAST
        || crd->name == NULL
        || (crd->rec_type == CL_RENAME && crd->sname == NULL)) {
        return EINVAL;
    }

    record_size = sizeof(CL_REC_TYPE) + strlen(crd->name) + 1;
    if (crd->rec_type == CL_RENAME) {
#if defined(HAVE_FLEX_CL)
        record_size += sizeof(struct changelog_ext_rename) + strlen(crd->sname);
        ++record_size; /* Trailing zero of source name. */
#elif defined(HAVE_CHANGELOG_EXTEND_REC)
        record_size += strlen(crd->sname) + 1;
#else
#error Unsupported Lustre version (prior to 2.3).
#endif
    }

    *rec = MemAlloc(record_size);
    if (*rec == NULL) {
        return ENOMEM;
    }

    memset(*rec, 0, record_size);
    (*rec)->cr_type = crd->rec_type;
    (*rec)->cr_pfid = crd->pfid;
    (*rec)->cr_index = crd->idx;
    if (crd->rec_type != CL_RENAME) {
        (*rec)->cr_tfid = crd->tfid;
    } else {
        (*rec)->cr_flags = PROCESS_LOG_ONE_REC;
#if defined(HAVE_FLEX_CL)
        ext_rec = changelog_rec_rename(*rec);
        ext_rec->cr_sfid = crd->sfid;
        ext_rec->cr_spfid = crd->spfid;
#else
        (*rec)->cr_sfid = crd->sfid;
        (*rec)->cr_spfid = crd->spfid;
#endif
    }
    sprintf(rh_get_cl_cr_name(*rec), "%s", crd->name);
    /* We have to set source name after target name */
    if ((*rec)->cr_type == CL_RENAME) {
        sprintf((char*)changelog_rec_sname(*rec), "%s", crd->sname);
        (*rec)->cr_namelen = strlen(crd->sname);
    }
    (*rec)->cr_namelen += strlen(crd->name) + 1;

    return 0;
}

static bool unlink_compact_wrapper(struct rh_list_head *op_queue,
                                   unsigned int *op_queue_count);
static bool unlink_compact_wrapper(struct rh_list_head *op_queue,
                                   unsigned int *op_queue_count)
{
    cpp_instance_t *cppi = cpp_by_name("collapse");

    CU_ASSERT_FALSE(   cppi == NULL
                    || cppi->cpp == NULL
                    || cppi->cpp->action == NULL);
    if (cppi == NULL || cppi->cpp == NULL || cppi->cpp->action == NULL)
        return false;

    return cppi->cpp->action(op_queue, op_queue_count,
                             cppi->cpp->instance_data);
}

UNIT_TEST(unlink_compact_create_unlink_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name_0"),
        SHORT_RECORD(CL_HARDLINK, 0, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_UNLINK, 0, basic_fid, basic_fid, "target_name_0")
    };

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 1);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 1);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type,
                    CL_HARDLINK);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
#undef TEST_SHORT_NAME
}

UNIT_TEST(unlink_compact_create_unlink_final_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name_0"),
        SHORT_RECORD(CL_HARDLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_UNLINK, 2, basic_fid, basic_fid, "target_name_0")
    };

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        if (i == 2) {
            rec[2]->cr_flags |= CLF_UNLINK_LAST;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 1);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 1);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_UNLINK);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
#undef N_TEST_RECORDS
#undef TEST_SHORT_NAME
}

UNIT_TEST(unlink_compact_create_rename_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name_0"),
        SHORT_RECORD(CL_HARDLINK, 1, basic_fid, basic_fid, "target_name_1"),
        RENAME_RECORD(2, basic_fid, "target_name_new",
                      basic_fid, basic_fid, "target_name_0")
    };

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type,
                    CL_HARDLINK);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type,
                    CL_EXT);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
#undef TEST_SHORT_NAME
}

UNIT_TEST(unlink_compact_mkdir_rename_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                   *rec[N_TEST_RECORDS];
    int                            rc;
    int                            n_records;
    int                            i;
    entry_proc_op_t               *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_MKDIR, 0, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_CREATE, 1, basic_fid, basic_fid, "target_name"),
        RENAME_RECORD(2, basic_fid, "target_name_new",
                      basic_fid, basic_fid, "target_name")
    };

    record_inputs[1].tfid.f_oid += 1;

    for (i = 0; i < N_TEST_RECORDS - 1; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS + 1);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);
    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_EXT);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_mkdir_rmdir_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                   *rec[N_TEST_RECORDS];
    int                            rc;
    int                            n_records;
    int                            i;
    entry_proc_op_t               *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_MKDIR, 0, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_CREATE, 1, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_RMDIR, 2, basic_fid, basic_fid, "target_name"),
    };

    record_inputs[1].tfid.f_oid += 1;
    record_inputs[1].pfid.f_oid += 1;

    for (i = 0; i < N_TEST_RECORDS - 1; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 1);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 1);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_ext_unlink_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        RENAME_RECORD(0, basic_fid, "target_name_new",
                      basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_CREATE, 3, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_UNLINK, 4, basic_fid, basic_fid, "target_name_new")
    };

    record_inputs[1].tfid.f_oid += 1;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS + 1);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_RENAME);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_ext_rmdir_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        RENAME_RECORD(0, basic_fid, "target_name_new",
                      basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_CREATE, 1, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_RMDIR, 2, basic_fid, basic_fid, "target_name_new")
    };

    record_inputs[1].tfid.f_oid += 1;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS + 1);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_RENAME);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_ext_rename_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        RENAME_RECORD(0, basic_fid, "target_name_new",
                      basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_CREATE, 1, basic_fid, basic_fid, "target_name"),
        RENAME_RECORD(2, basic_fid, "target_name_new2",
                      basic_fid, basic_fid, "target_name_new")
    };

    record_inputs[1].tfid.f_oid += 1;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS + 2);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 3);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 3);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_RENAME);
    op = rh_list_entry(op->list.next, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_EXT);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_hardlink_unlink_test)
{
#define N_TEST_RECORDS 4
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_HARDLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_CREATE, 2, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_UNLINK, 3, basic_fid, basic_fid, "target_name_1"),
    };

    record_inputs[2].tfid.f_oid += 2;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_hardlink_unlink_final_test)
{
#define N_TEST_RECORDS 4
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_HARDLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_CREATE, 2, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_UNLINK, 3, basic_fid, basic_fid, "target_name_1"),
    };

    record_inputs[2].tfid.f_oid += 2;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        if (i == 3) {
            rec[3]->cr_flags |= CLF_UNLINK_LAST;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_UNLINK);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_hardlink_rename_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_HARDLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_CREATE, 2, basic_fid, basic_fid, "target_name"),
        RENAME_RECORD(3, basic_fid, "target_name_new2",
                      basic_fid, basic_fid, "target_name_1")
    };

    record_inputs[1].tfid.f_oid += 2;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS + 1);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_EXT);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_softlink_unlink_test)
{
#define N_TEST_RECORDS 4
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_SOFTLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_HARDLINK, 2, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_UNLINK, 3, basic_fid, basic_fid, "target_name_1"),
    };

    record_inputs[0].tfid.f_oid += 1;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_HARDLINK);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_softlink_unlink_final_test)
{
#define N_TEST_RECORDS 4
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_CREATE, 0, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_SOFTLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_HARDLINK, 2, basic_fid, basic_fid, "target_name"),
        SHORT_RECORD(CL_UNLINK, 3, basic_fid, basic_fid, "target_name_1"),
    };

    record_inputs[0].tfid.f_oid += 2;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        if (i == 3) {
            rec[3]->cr_flags |= CLF_UNLINK_LAST;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_UNLINK);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

UNIT_TEST(unlink_compact_softlink_rename_test)
{
#define N_TEST_RECORDS 3
    CL_REC_TYPE                  *rec[N_TEST_RECORDS];
    int                           rc;
    int                           n_records;
    int                           i;
    entry_proc_op_t              *op;
    struct changelog_record_descr record_inputs[N_TEST_RECORDS] = {
        SHORT_RECORD(CL_SOFTLINK, 1, basic_fid, basic_fid, "target_name_1"),
        SHORT_RECORD(CL_CREATE, 2, basic_fid, basic_fid, "target_name"),
        RENAME_RECORD(3, basic_fid, "target_name_new2",
                      basic_fid, basic_fid, "target_name_1")
    };

    record_inputs[1].tfid.f_oid += 2;

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rec[i] = NULL;
    }

    for (i = 0; i < N_TEST_RECORDS; ++i) {
        rc = cl_rec_alloc_fill(rec + i, record_inputs + i);
        CU_ASSERT_EQUAL(rc, 0);
        if (rc != 0) {
            /* Fatal failure, can't proceed with test. */
            goto test_done;
        }
        rc = process_log_rec(&thread_info, rec[i]);
        CU_ASSERT_EQUAL(rc, 0);
    }

    CU_ASSERT_EQUAL(thread_info.op_queue_count, N_TEST_RECORDS + 1);

    CU_ASSERT_EQUAL(thread_info.suppressed_records, 0);
    CU_ASSERT_EQUAL(thread_info.interesting_records, N_TEST_RECORDS);

    CU_ASSERT_TRUE(unlink_compact_wrapper(&thread_info.op_queue,
                                  &thread_info.op_queue_count));

    n_records = process_log_rec_record_count();
    CU_ASSERT_EQUAL(n_records, 2);
    CU_ASSERT_EQUAL(thread_info.op_queue_count, 2);

    op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_CREATE);
    op = rh_list_last_entry(&thread_info.op_queue, entry_proc_op_t, list);
    CU_ASSERT_PTR_NOT_NULL(op);
    CU_ASSERT_EQUAL(op->extra_info.log_record.p_log_rec->cr_type, CL_EXT);

test_done:
    for (i = 0; i < N_TEST_RECORDS; ++i)
        if (rec[i] != NULL)
            MemFree(rec[i]);
    while (!rh_list_empty(&thread_info.op_queue)) {
        op = rh_list_first_entry(&thread_info.op_queue, entry_proc_op_t, list);
        rh_list_del(&op->list);
        rh_list_del(&op->id_hash_list);
    }
#undef N_TEST_RECORDS
}

/** @} */

void changelog_test_init(void)
{
    log_config.debug_level = 0;
    memset(&thread_info, 0, sizeof(thread_info));
    rh_list_init(&thread_info.op_queue);
    thread_info.id_hash = id_hash_init(ID_CHGLOG_HASH_SIZE, false);
}

void changelog_test_fini(void)
{
    process_log_rec_queue_cleanup();
    MemFree(thread_info.id_hash);
    memset(&thread_info, 0, sizeof(thread_info));
}

int changelog_suite_init(void);
int changelog_suite_init(void)
{
    cl_reader_config.mdt_count = 1;
    cl_reader_config.mdt_def = MemCalloc(1, sizeof(mdt_def_t));
    if (cl_reader_config.mdt_def == NULL)
        return ENOMEM;

    strcpy(cl_reader_config.mdt_def[0].mdt_name, "fake_mdt");

    (void)create_cpp_instance("collapse");
    if (cpp_by_name("collapse") == NULL)
        return 1;

    return 0;
}

int changelog_suite_fini(void);
int changelog_suite_fini(void)
{
    MemFree(cl_reader_config.mdt_def);
    cl_reader_config.mdt_count = 0;

    module_unload_all();

    return 0;
}

CU_TestInfo changelog_suite[] = {
    UNIT_TEST_INFO(process_log_rec_invalid_test),
    UNIT_TEST_INFO(process_log_rec_ignore_test),
#if defined(HAVE_CHANGELOG_EXTEND_REC) || defined(HAVE_FLEX_CL)
    UNIT_TEST_INFO(process_log_rec_rename_one_zero_tfid_test),
    UNIT_TEST_INFO(process_log_rec_rename_non_zero_tfid_test),
#endif
    UNIT_TEST_INFO(process_log_rec_rename_ext_rec_one_zero_tfid_test),
    UNIT_TEST_INFO(process_log_rec_rename_ext_rec_non_zero_tfid_test),
    UNIT_TEST_INFO(process_log_rec_unlink),
    UNIT_TEST_INFO(unlink_compact_create_unlink_test),
    UNIT_TEST_INFO(unlink_compact_create_unlink_final_test),
    UNIT_TEST_INFO(unlink_compact_create_rename_test),
    UNIT_TEST_INFO(unlink_compact_mkdir_rename_test),
    UNIT_TEST_INFO(unlink_compact_mkdir_rmdir_test),
    UNIT_TEST_INFO(unlink_compact_ext_unlink_test),
    UNIT_TEST_INFO(unlink_compact_ext_rmdir_test),
    UNIT_TEST_INFO(unlink_compact_ext_rename_test),
    UNIT_TEST_INFO(unlink_compact_hardlink_unlink_test),
    UNIT_TEST_INFO(unlink_compact_hardlink_unlink_final_test),
    UNIT_TEST_INFO(unlink_compact_hardlink_rename_test),
    UNIT_TEST_INFO(unlink_compact_softlink_unlink_test),
    UNIT_TEST_INFO(unlink_compact_softlink_unlink_final_test),
    UNIT_TEST_INFO(unlink_compact_softlink_rename_test),
    CU_TEST_INFO_NULL
};
