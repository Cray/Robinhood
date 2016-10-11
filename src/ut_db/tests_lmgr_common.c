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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

#include "tests_lmgr_common.h"

#include "chglog_reader.h"
#include "fs_scan_main.h"
#include "listmgr_common.h"
#include "Memory.h"

/* Next macros are from rbh_daemon.c */
#define ACTION_MASK_HANDLE_EVENTS       0x00000004
#define ACTION_MASK_RUN_POLICIES        0x00000008
#define ACTION_MASK_SCAN                0x00000001

lmgr_t mgr;
bool   list_manager_initialised = false;
size_t n_test_records = 0;

extern chglog_reader_config_t cl_reader_config;

/* Copied from rbh_daemon.c */
static int action2parsing_mask( int act_mask )
{
    /* build config parsing mask */
    int parse_mask = 0;
    if (act_mask & ACTION_MASK_SCAN)
        parse_mask |= MODULE_MASK_FS_SCAN | MODULE_MASK_ENTRY_PROCESSOR;
    if (act_mask & ACTION_MASK_RUN_POLICIES)
        parse_mask |= MODULE_MASK_POLICY_RUN;
#ifdef HAVE_CHANGELOGS
    if ( act_mask & ACTION_MASK_HANDLE_EVENTS )
        parse_mask |= MODULE_MASK_EVENT_HDLR | MODULE_MASK_ENTRY_PROCESSOR;
#endif

    return parse_mask;
}

void fake_fs_scan_call(void);
void fake_fs_scan_call(void)
{
	CL_REC_TYPE rec;

    process_log_rec(NULL, &rec);
    FSScan_DumpStats();
}

int suite_setup(void)
{
    int  rc;
    int  action_mask = ACTION_MASK_HANDLE_EVENTS | ACTION_MASK_RUN_POLICIES;
    char err_msg[4096];

    rc = rbh_cfg_load(action2parsing_mask(action_mask), config_file_name,
                      err_msg);
    if (rc != 0)
        return rc;

    rc = smi_init_all(0);

    return rc;
}

int test_setup(void)
{
    int          rc;
    struct stat  stat_info;
    int          dump_fd;
    ssize_t      bytes_read;
    char        *dump;
    MYSQL_RES   *sql_result;

    rc = stat(dump_file, &stat_info);
    if (rc != 0) {
        rc = errno;
        return rc;
    }

    dump = MemAlloc(stat_info.st_size + 1);
    if (dump == NULL) {
        rc = ENOMEM;
        return rc;
    }

    dump_fd = open(dump_file, O_RDONLY);
    if (dump_fd == -1) {
        rc = errno;
        return rc;
    }

    bytes_read = read(dump_fd, dump, stat_info.st_size);
    if (bytes_read != stat_info.st_size)
        return ENODATA;

    dump[stat_info.st_size] = '\000';
    close(dump_fd);

    if (!list_manager_initialised) {
        rc = ListMgr_Init(false);
        if (rc != 0)
            return rc;

        list_manager_initialised = true;
    }

    rc = ListMgr_InitAccess(&mgr);
    if (rc != 0)
        return rc;

    mysql_set_server_option(&mgr.conn, MYSQL_OPTION_MULTI_STATEMENTS_ON);
    mysql_real_query(&mgr.conn, "START TRANSACTION",
                     strlen("START TRANSACTION"));
    sql_result = mysql_store_result(&mgr.conn);
    if (sql_result != NULL)
        mysql_free_result(sql_result);

    rc = mysql_real_query(&mgr.conn, dump, stat_info.st_size);
    if (rc != 0)
        return rc;

    do {
        sql_result = mysql_store_result(&mgr.conn);
        if (sql_result)
            mysql_free_result(sql_result);
    } while (mysql_next_result(&mgr.conn) == 0);

    mysql_real_query(&mgr.conn, "COMMIT", strlen("COMMIT"));
    sql_result = mysql_store_result(&mgr.conn);
    if (sql_result != NULL)
        mysql_free_result(sql_result);

    MemFree(dump);

    mysql_set_server_option(&mgr.conn, MYSQL_OPTION_MULTI_STATEMENTS_OFF);
    ListMgr_CloseAccess(&mgr);

    rc = ListMgr_InitAccess(&mgr);

    return rc;
}

int test_teardown(void)
{
    return ListMgr_CloseAccess(&mgr);
}

/** Fisher-Yates shuffle. */
void shuffle(entry_id_t *ids, size_t n);
void shuffle(entry_id_t *ids, size_t n)
{
    struct timeval tv;
    size_t         src_idx;
    size_t         dst_idx;
    entry_id_t     tmp_id;

    gettimeofday(&tv, NULL);
    srand48(tv.tv_usec);

    if (n > 1) {
        for (src_idx = n - 1; src_idx > 0; --src_idx) {
            dst_idx = (size_t)(drand48() * (src_idx + 1));
            tmp_id = ids[src_idx];
            ids[src_idx] = ids[dst_idx];
            ids[dst_idx] = tmp_id;
        }
    }
}

int get_fids(entry_id_t **fids, size_t *n_ids, bool file_only);
int get_fids(entry_id_t **fids, size_t *n_ids, bool file_only)
{
    lmgr_t     mgr;
    int        rc;
    MYSQL_RES *sql_result = NULL;
    MYSQL_ROW  row = NULL;
    size_t     idx;

#define SELECT_ALL_FIDS "select id from "MAIN_TABLE
#define SELECT_FILE_FIDS "select id from "MAIN_TABLE" where type='file'"

    if (n_ids == NULL)
        return EINVAL;

    if (fids == NULL)
        return EINVAL;

    *fids = NULL;
    *n_ids = 0;

    rc = ListMgr_InitAccess(&mgr);
    if (rc != 0)
        return rc;

    rc = mysql_real_query(&mgr.conn,
                          file_only ? SELECT_FILE_FIDS : SELECT_ALL_FIDS,
                          strlen(file_only ? SELECT_FILE_FIDS :
                                 SELECT_ALL_FIDS));
    if (rc != 0)
        goto done;
#undef SELECT_FILE_FIDS
#undef SELECT_ALL_FIDS

    sql_result = mysql_store_result(&mgr.conn);
    if (sql_result == NULL) {
        rc = ENODATA;
        goto done;
    }

    *n_ids = mysql_num_rows(sql_result);
    if (*n_ids == 0) {
        rc = ENODATA;
        goto done;
    }

    *fids = calloc(*n_ids, sizeof(**fids));
    if (*fids == NULL) {
        rc = ENOMEM;
        goto done;
    }

    for (idx = 0; idx < *n_ids; ++idx) {
        row = mysql_fetch_row(sql_result);
        if (*row == NULL) {
            rc = ENODATA;
            goto done;
        }
        pk2entry_id(&mgr, row[0], *fids + idx);
    }

    rc = 0;

done:
    if (rc != 0 && *fids != NULL) {
        free(*fids);
        *fids = NULL;
    }
    if (sql_result != NULL)
        mysql_free_result(sql_result);
    ListMgr_CloseAccess(&mgr);

    return rc;
}

static entry_id_t *fid_array = NULL;

int get_fids_shuffled(void)
{
    int rc;

    if ((rc = get_fids(&fid_array, &n_test_records, false)) != 0)
        return rc;

    shuffle(fid_array, n_test_records);
    return 0;
}

void *get_next_fid(void)
{
    static size_t next_idx = 0;
    void *result = NULL;

    if (fid_array != NULL && n_test_records != 0) {
        result = fid_array + next_idx % n_test_records;
        ++next_idx;
    }

    return result;
}

void free_fids(void)
{
    if (fid_array != NULL) {
        free(fid_array);
        fid_array = NULL;
        n_test_records = 0;
    }
}

int chmod_test(void *data, void **result)
{
    struct two_attrsets_data *attr_sets;
    int                       rc;

    if (data == NULL)
        return EINVAL;

    attr_sets = malloc(sizeof(*attr_sets));
    if (result != NULL)
        *result = attr_sets;

    ATTR_SET_INIT_ST(&attr_sets->first);
    ATTR_SET_INIT_ST(&attr_sets->second);

    ATTR_MASK_SET(&attr_sets->first, size);
    ATTR_MASK_SET(&attr_sets->first, type);
    ATTR_MASK_SET(&attr_sets->first, link);
    ATTR_MASK_SET(&attr_sets->first, path_update);
    ATTR_MASK_SET(&attr_sets->first, fullpath);
    ATTR_MASK_STATUS_SET(&attr_sets->first, 0);

    rc = ListMgr_Get(&mgr, data, &attr_sets->first);
    if (rc != 0)
        goto done;

    ATTR_MASK_SET(&attr_sets->second, owner);
    ATTR_MASK_SET(&attr_sets->second, gr_name);
    ATTR_MASK_SET(&attr_sets->second, blocks);
    ATTR_MASK_SET(&attr_sets->second, last_access);
    ATTR_MASK_SET(&attr_sets->second, last_mod);
    ATTR_MASK_SET(&attr_sets->second, mode);
    ATTR_MASK_SET(&attr_sets->second, nlink);
    ATTR_MASK_SET(&attr_sets->second, md_update);
    ATTR_MASK_SET(&attr_sets->second, fileclass);
    ATTR_MASK_SET(&attr_sets->second, class_update);

    strcpy(ATTR(&attr_sets->second, owner), "root");
    strcpy(ATTR(&attr_sets->second, gr_name), "root");
    ATTR(&attr_sets->second, blocks) = 10;
    ATTR(&attr_sets->second, last_access) = time(NULL);
    ATTR(&attr_sets->second, last_mod) = ATTR(&attr_sets->second,
                                                 last_access) + 1;
    ATTR(&attr_sets->second, mode) = 932;
    ATTR(&attr_sets->second, nlink) = 1;
    ATTR(&attr_sets->second, md_update) = ATTR(&attr_sets->second,
                                                  last_mod) + 1;
    strcpy(ATTR(&attr_sets->second, fileclass), "test_file_class");
    ATTR(&attr_sets->second, class_update) = ATTR(&attr_sets->second,
                                                     md_update) + 1;

    rc = ListMgr_Update(&mgr, data, &attr_sets->second);
    if (rc != 0)
        goto done;

done:
    if (result == NULL) {
        ListMgr_FreeAttrs(&attr_sets->first);
        ListMgr_FreeAttrs(&attr_sets->second);
        free(attr_sets);
    }

    return rc;
}

const char *get_cl_last_committed_name(void)
{
    static char varname[MAX_VAR_LEN] = "";

    if (varname[0] == '\000')
        snprintf(varname, sizeof(varname), "%s_%s", CL_LAST_COMMITTED,
                cl_reader_config.mdt_def[0].mdt_name);

    return varname;
}

int lhsm_archive_test(void *data, void**result)
{
    struct lhsm_archive_test_data *attr_sets;
    attr_set_t                     attrs_get;
    int                            rc;
    sm_instance_t                 *sm_lhsm;
    static int                     changelog_last_commit = 0;
    char                           print_buffer[20];

    if (data == NULL)
        return EINVAL;

    attr_sets = malloc(sizeof(*attr_sets));
    if (result != NULL)
        *result = attr_sets;

    sm_lhsm = LHSM_SMI;
    if (sm_lhsm == NULL) {
        rc = EINVAL;
        goto done;
    }

    /* Initialise all attribute sets. */
    ATTR_SET_INIT_ST(&attr_sets->attrs);
    ATTR_SET_INIT_ST(&attr_sets->updated1_attrs);
    ATTR_SET_INIT_ST(&attr_sets->updated2_attrs);
    ATTR_SET_INIT_ST(&attr_sets->updated3_attrs);
    ATTR_SET_INIT_ST(&attrs_get);

    ATTR_MASK_SET(&attr_sets->attrs, size);
    ATTR_MASK_SET(&attr_sets->attrs, type);
    ATTR_MASK_SET(&attr_sets->attrs, path_update);
    ATTR_MASK_SET(&attr_sets->attrs, fullpath);
    ATTR_MASK_STATUS_SET(&attr_sets->attrs, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, data, &attr_sets->attrs);
    if (rc != 0)
        goto done;

    /** Prepare SQL statement like the following:
     * \code{.unparsed}
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=8,
     * last_access=1472812758, last_mod=1472812758, type='file', mode=420,
     * nlink=1, md_update=1472812895, fileclass='++', class_update=1472812895,
     * lhsm_status='synchro', lhsm_archid=1, lhsm_norels=0, lhsm_noarch=0 WHERE
     * id='0x200000401:0x1:0x0'
     * \endcode
     */
    ATTR_MASK_SET(&attr_sets->updated1_attrs, owner);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, gr_name);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, blocks);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, last_access);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, last_mod);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, type);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, mode);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, nlink);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, md_update);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, fileclass);
    ATTR_MASK_SET(&attr_sets->updated1_attrs, class_update);
    ATTR_MASK_STATUS_SET(&attr_sets->updated1_attrs, sm_lhsm->smi_index);
    ATTR_MASK_INFO_SET(&attr_sets->updated1_attrs, sm_lhsm, ATTR_ARCHIVE_ID);
    ATTR_MASK_INFO_SET(&attr_sets->updated1_attrs, sm_lhsm, ATTR_NO_RELEASE);
    ATTR_MASK_INFO_SET(&attr_sets->updated1_attrs, sm_lhsm, ATTR_NO_ARCHIVE);

    strcpy(ATTR(&attr_sets->updated1_attrs, owner), "2000");
    strcpy(ATTR(&attr_sets->updated1_attrs, gr_name), "root");
    ATTR(&attr_sets->updated1_attrs, blocks) = 0;
    ATTR(&attr_sets->updated1_attrs, last_access) = time(NULL);
    ATTR(&attr_sets->updated1_attrs, last_mod) =
        ATTR(&attr_sets->updated1_attrs, last_access) + 1;
    strcpy(ATTR(&attr_sets->updated1_attrs, type), "file");
    ATTR(&attr_sets->updated1_attrs, mode) = 420;
    ATTR(&attr_sets->updated1_attrs, nlink) = 1;
    ATTR(&attr_sets->updated1_attrs, md_update) =
        ATTR(&attr_sets->updated1_attrs, last_access) + 2;
    strcpy(ATTR(&attr_sets->updated1_attrs, fileclass),
           "system_test_file_class");
    ATTR(&attr_sets->updated1_attrs, class_update) =
        ATTR(&attr_sets->updated1_attrs, last_access) + 3;
    sm_status_ensure_alloc(&attr_sets->updated1_attrs.attr_values.sm_status);
    sm_info_ensure_alloc(&attr_sets->updated1_attrs.attr_values.sm_info);
    /* The next line uses direct literal - as in 'lhsm' status manager. */
    STATUS_ATTR(&attr_sets->updated1_attrs, sm_lhsm->smi_index) = "synchro";
    SMI_INFO(&attr_sets->updated1_attrs, sm_lhsm, ATTR_ARCHIVE_ID) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated1_attrs, sm_lhsm, ATTR_ARCHIVE_ID) = 1;
    SMI_INFO(&attr_sets->updated1_attrs, sm_lhsm, ATTR_NO_RELEASE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated1_attrs, sm_lhsm, ATTR_NO_RELEASE) = 0;
    SMI_INFO(&attr_sets->updated1_attrs, sm_lhsm, ATTR_NO_ARCHIVE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated1_attrs, sm_lhsm, ATTR_NO_ARCHIVE) = 0;

    rc = ListMgr_Update(&mgr, data, &attr_sets->updated1_attrs);
    if (rc != 0)
        goto done;

    snprintf(print_buffer, sizeof(print_buffer), "%i", changelog_last_commit);
    rc = ListMgr_SetVar(&mgr, get_cl_last_committed_name(),
                        print_buffer);
    ++changelog_last_commit;
    if (rc != 0)
        goto done;

    ATTR_MASK_SET(&attrs_get, size);
    ATTR_MASK_SET(&attrs_get, type);
    ATTR_MASK_SET(&attrs_get, path_update);
    ATTR_MASK_SET(&attrs_get, fullpath);
    ATTR_MASK_STATUS_SET(&attrs_get, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, data, &attrs_get);
    if (rc != 0)
        goto done;

    /* Preparing SQL UPDATE like:
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=8,
     * last_access=1472812758, last_mod=1472812758, type='file', mode=420,
     * nlink=1, md_update=1472812897, fileclass='++', class_update=1472812897,
     * lhsm_archid=1, lhsm_norels=0, lhsm_noarch=0 WHERE
     * id='0x200000401:0x1:0x0'
     */
    ATTR_MASK_SET(&attr_sets->updated2_attrs, owner);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, gr_name);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, blocks);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, last_access);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, last_mod);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, type);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, mode);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, nlink);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, md_update);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, fileclass);
    ATTR_MASK_SET(&attr_sets->updated2_attrs, class_update);
    ATTR_MASK_INFO_SET(&attr_sets->updated2_attrs, sm_lhsm, ATTR_ARCHIVE_ID);
    ATTR_MASK_INFO_SET(&attr_sets->updated2_attrs, sm_lhsm, ATTR_NO_RELEASE);
    ATTR_MASK_INFO_SET(&attr_sets->updated2_attrs, sm_lhsm, ATTR_NO_ARCHIVE);

    strcpy(ATTR(&attr_sets->updated2_attrs, owner), "2000");
    strcpy(ATTR(&attr_sets->updated2_attrs, gr_name), "root");
    ATTR(&attr_sets->updated2_attrs, blocks) = 0;
    ATTR(&attr_sets->updated2_attrs, last_access) = time(NULL);
    ATTR(&attr_sets->updated2_attrs, last_mod) =
        ATTR(&attr_sets->updated2_attrs, last_access) + 1;
    strcpy(ATTR(&attr_sets->updated2_attrs, type), "file");
    ATTR(&attr_sets->updated2_attrs, mode) = 420;
    ATTR(&attr_sets->updated2_attrs, nlink) = 1;
    ATTR(&attr_sets->updated2_attrs, md_update) =
        ATTR(&attr_sets->updated2_attrs, last_access) + 2;
    strcpy(ATTR(&attr_sets->updated2_attrs, fileclass),
           "system_test_file_class");
    ATTR(&attr_sets->updated2_attrs, class_update) =
        ATTR(&attr_sets->updated2_attrs, last_access) + 3;
    sm_status_ensure_alloc(&attr_sets->updated2_attrs.attr_values.sm_status);
    sm_info_ensure_alloc(&attr_sets->updated2_attrs.attr_values.sm_info);
    SMI_INFO(&attr_sets->updated2_attrs, sm_lhsm, ATTR_ARCHIVE_ID) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated2_attrs, sm_lhsm, ATTR_ARCHIVE_ID) = 1;
    SMI_INFO(&attr_sets->updated2_attrs, sm_lhsm, ATTR_NO_RELEASE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated2_attrs, sm_lhsm, ATTR_NO_RELEASE) = 0;
    SMI_INFO(&attr_sets->updated2_attrs, sm_lhsm, ATTR_NO_ARCHIVE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated2_attrs, sm_lhsm, ATTR_NO_ARCHIVE) = 0;

    rc = ListMgr_Update(&mgr, data, &attr_sets->updated2_attrs);
    if (rc != 0)
        goto done;

    ListMgr_FreeAttrs(&attrs_get);
    ATTR_SET_INIT_ST(&attrs_get);
    ATTR_MASK_SET(&attrs_get, size);
    ATTR_MASK_SET(&attrs_get, type);
    ATTR_MASK_SET(&attrs_get, path_update);
    ATTR_MASK_SET(&attrs_get, fullpath);
    ATTR_MASK_STATUS_SET(&attrs_get, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, data, &attrs_get);
    if (rc != 0)
        goto done;
    /* Do not free attrs_get: we'll definitely do it later. */

    /* Preparing UPDATE SQL statement like:
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=8,
     * last_access=1472812758, last_mod=1472812758, type='file', mode=420,
     * nlink=1, md_update=1472812897, fileclass='++', class_update=1472812897,
     * lhsm_lstarc=1472812891 WHERE id='0x200000401:0x1:0x0'
     */
    ATTR_MASK_SET(&attr_sets->updated3_attrs, owner);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, gr_name);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, blocks);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, last_access);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, last_mod);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, type);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, mode);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, nlink);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, md_update);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, fileclass);
    ATTR_MASK_SET(&attr_sets->updated3_attrs, class_update);
    ATTR_MASK_INFO_SET(&attr_sets->updated3_attrs, sm_lhsm, ATTR_LAST_ARCHIVE);

    strcpy(ATTR(&attr_sets->updated3_attrs, owner), "2000");
    strcpy(ATTR(&attr_sets->updated3_attrs, gr_name), "root");
    ATTR(&attr_sets->updated3_attrs, blocks) = 0;
    ATTR(&attr_sets->updated3_attrs, last_access) = time(NULL);
    ATTR(&attr_sets->updated3_attrs, last_mod) =
        ATTR(&attr_sets->updated3_attrs, last_access) + 1;
    strcpy(ATTR(&attr_sets->updated3_attrs, type), "file");
    ATTR(&attr_sets->updated3_attrs, mode) = 420;
    ATTR(&attr_sets->updated3_attrs, nlink) = 1;
    ATTR(&attr_sets->updated3_attrs, md_update) =
        ATTR(&attr_sets->updated3_attrs, last_access) + 2;
    strcpy(ATTR(&attr_sets->updated3_attrs, fileclass),
           "system_test_file_class");
    ATTR(&attr_sets->updated3_attrs, class_update) =
        ATTR(&attr_sets->updated3_attrs, last_access) + 3;
    sm_status_ensure_alloc(&attr_sets->updated3_attrs.attr_values.sm_status);
    sm_info_ensure_alloc(&attr_sets->updated3_attrs.attr_values.sm_info);
    SMI_INFO(&attr_sets->updated3_attrs, sm_lhsm, ATTR_LAST_ARCHIVE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->updated3_attrs, sm_lhsm, ATTR_LAST_ARCHIVE) =
        ATTR(&attr_sets->updated3_attrs, last_access) + 4;

    rc = ListMgr_Update(&mgr, data, &attr_sets->updated3_attrs);
    if (rc != 0)
        goto done;

done:
    if (result == NULL) {
        ListMgr_FreeAttrs(&attr_sets->attrs);
        ListMgr_FreeAttrs(&attr_sets->updated1_attrs);
        ListMgr_FreeAttrs(&attr_sets->updated2_attrs);
        ListMgr_FreeAttrs(&attr_sets->updated3_attrs);
        free(attr_sets);
    }
    ListMgr_FreeAttrs(&attrs_get);

    return rc;
}

static bool entry_id_gt(entry_id_t *first, entry_id_t *second);
static bool entry_id_gt(entry_id_t *first, entry_id_t *second)
{
    return (first->f_seq > second->f_seq)
        || (   (first->f_seq == second->f_seq)
            && (   (first->f_oid > second->f_oid)
                || ((first->f_oid == second->f_oid)
                    && (first->f_ver > second->f_ver))));
}

int get_max_fid(entry_id_t *id)
{
    int        rc;
    const char max_fid_req[] = "select id from "MAIN_TABLE;
    MYSQL_RES *sql_result;
    MYSQL_ROW  row;
    entry_id_t fid = {0, 0, 0};

    if (id == NULL)
        return EINVAL;
    memset(id, 0, sizeof(*id));

    rc = mysql_real_query(&mgr.conn, max_fid_req, strlen(max_fid_req));
    if (rc != 0)
        return rc;

    sql_result = mysql_store_result(&mgr.conn);
    if (sql_result == NULL)
        return ENODATA;

    while ((row = mysql_fetch_row(sql_result)) != NULL) {
        pk2entry_id(&mgr, row[0], &fid);
        if (entry_id_gt(&fid, id) > 0)
            memcpy(id, &fid, sizeof(fid));
    }

    mysql_free_result(sql_result);

    return 0;
}

int inc_fid(entry_id_t *id)
{
    if (id == NULL)
        return EINVAL;
    if (id->f_oid != UINT32_MAX) {
        ++id->f_oid;
    } else if (id->f_seq == ULLONG_MAX) {
        return E2BIG;
    } else {
        ++id->f_seq;
        id->f_oid = 0;
    }
    return 0;
}

int mkdir_test(void *data, void **result)
{
    struct two_attrsets_data *attr_sets;
    int                       rc;
    sm_instance_t            *sm_lhsm;
    char                      print_buffer[20];
    struct dir_test_data     *test_data = data;

    if (test_data == NULL)
        return EINVAL;

    attr_sets = malloc(sizeof(*attr_sets));
    if (result != NULL)
        *result = attr_sets;

    sm_lhsm = LHSM_SMI;
    if (sm_lhsm == NULL) {
        rc = EINVAL;
        goto done;
    }

    ATTR_SET_INIT_ST(&attr_sets->first);
    ATTR_SET_INIT_ST(&attr_sets->second);

    /* Prepare SELECT SQL statement like:
     * SELECT size, this_path(parent_id, name) FROM ENTRIES LEFT JOIN NAMES ON
     * ENTRIES.id=NAMES.id WHERE ENTRIES.id='0x200000401:0x5:0x0'
     */
    ATTR_MASK_SET(&attr_sets->first, size);
    ATTR_MASK_SET(&attr_sets->first, fullpath);

    rc = ListMgr_Get(&mgr, &test_data->dir_fid, &attr_sets->first);
    if (rc != ENOENT)
        goto done;

    /* Prepare INSERT SQL statements like:
     * INSERT INTO ENTRIES(id, owner, gr_name, size, blocks, creation_time,
     * last_access, last_mod, type, mode, nlink, md_update, fileclass,
     * class_update) VALUES ('0x200000401:0x5:0x0', 'root', 'root', 4096, 8,
     * 1472813482, 1472813482, 1472813482, 'dir', 493, 2, 1472813488, '++',
     * 1472813488)
     *
     * INSERT INTO NAMES(id, parent_id, name, path_update, pkn) VALUES
     * ('0x200000401:0x5:0x0', '0x200000007:0x1:0x0', 'directory', 1472813488,
     * sha1(CONCAT(parent_id, '/', name))) ON DUPLICATE KEY UPDATE
     * id=VALUES(id), parent_id=VALUES(parent_id), name=VALUES(name),
     * path_update=VALUES(path_update)
     */
    ATTR_MASK_SET(&attr_sets->second, owner);
    ATTR_MASK_SET(&attr_sets->second, gr_name);
    ATTR_MASK_SET(&attr_sets->second, size);
    ATTR_MASK_SET(&attr_sets->second, blocks);
    ATTR_MASK_SET(&attr_sets->second, creation_time);
    ATTR_MASK_SET(&attr_sets->second, last_access);
    ATTR_MASK_SET(&attr_sets->second, last_mod);
    ATTR_MASK_SET(&attr_sets->second, type);
    ATTR_MASK_SET(&attr_sets->second, mode);
    ATTR_MASK_SET(&attr_sets->second, nlink);
    ATTR_MASK_SET(&attr_sets->second, md_update);
    ATTR_MASK_SET(&attr_sets->second, fileclass);
    ATTR_MASK_SET(&attr_sets->second, class_update);
    ATTR_MASK_SET(&attr_sets->second, parent_id);
    ATTR_MASK_SET(&attr_sets->second, name);
    ATTR_MASK_SET(&attr_sets->second, path_update);

    strcpy(ATTR(&attr_sets->second, owner), "root");
    strcpy(ATTR(&attr_sets->second, gr_name), "root");
    ATTR(&attr_sets->second, size) = 24850;
    ATTR(&attr_sets->second, blocks) = 1;
    ATTR(&attr_sets->second, creation_time) = time(NULL);
    ATTR(&attr_sets->second, last_access) = ATTR(&attr_sets->second,
                                                     creation_time) + 1;
    ATTR(&attr_sets->second, last_mod) = ATTR(&attr_sets->second,
                                                  last_access) + 1;
    strcpy(ATTR(&attr_sets->second, type), "dir");
    ATTR(&attr_sets->second, mode) = 420;
    ATTR(&attr_sets->second, nlink) = 1;
    ATTR(&attr_sets->second, md_update) = ATTR(&attr_sets->second,
                                                  last_mod) + 1;
    strcpy(ATTR(&attr_sets->second, fileclass), "test_dir_class");
    ATTR(&attr_sets->second, class_update) = ATTR(&attr_sets->second,
                                                     md_update) + 1;
    memcpy(&ATTR(&attr_sets->second, parent_id), &test_data->fs_fid,
           sizeof(test_data->fs_fid));
    snprintf(print_buffer, sizeof(print_buffer), "dir%i",
             test_data->dir_number);
    ++test_data->dir_number;
    strcpy(ATTR(&attr_sets->second, name), print_buffer);
    ATTR(&attr_sets->second, path_update) = ATTR(&attr_sets->second,
                                                     class_update) + 1;

    rc = ListMgr_Insert(&mgr, &test_data->dir_fid, &attr_sets->second,
                        false);
    if (rc != 0)
        goto done;

done:
    if (result == NULL) {
        ListMgr_FreeAttrs(&attr_sets->first);
        ListMgr_FreeAttrs(&attr_sets->second);
        free(attr_sets);
    }

    return rc;
}

static struct dir_test_data dir_data;

int mkdir_test_init(void)
{
    int rc;

    if ((rc = get_fids_shuffled()) != 0)
        return rc;

    if ((rc = get_max_fid(&dir_data.fs_fid)) != 0)
        return rc;
    if ((rc = inc_fid(&dir_data.fs_fid)) != 0)
        return rc;

    memcpy(&dir_data.dir_fid, &dir_data.fs_fid, sizeof(dir_data.dir_fid));
    dir_data.dir_number = 0;

    return 0;
}

void *get_next_dir_data(void)
{
    int rc;

    rc = inc_fid(&dir_data.dir_fid);
    if (rc != 0)
        return NULL;

    return &dir_data;
}

int rmdir_test(void *data, void **result)
{
    int                   rc;
    char                  print_buffer[20];
    struct dir_test_data *test_data = data;
    attr_set_t            sel_attrs;
    attr_set_t            del_attrs;

    (void)result; /* This test produces no output. */

    if (test_data == NULL)
        return EINVAL;

    ATTR_SET_INIT_ST(&sel_attrs);
    ATTR_SET_INIT_ST(&del_attrs);

    /* Prepare SQL statement like:
     * SELECT size, this_path(parent_id, name) FROM ENTRIES LEFT JOIN NAMES ON
     * ENTRIES.id=NAMES.id WHERE ENTRIES.id='0x200000401:0x5:0x0'
     */
    ATTR_MASK_SET(&sel_attrs, size);
    ATTR_MASK_SET(&sel_attrs, fullpath);

    rc = ListMgr_Get(&mgr, &test_data->dir_fid, &sel_attrs);
    if (rc != 0)
        goto done;

    /* Prepare SQL set like:
     * DELETE M.*, A.*, I.*, S.* FROM ENTRIES M LEFT JOIN ANNEX_INFO A ON M.id =
     * A.id LEFT JOIN STRIPE_INFO I ON M.id = I.id LEFT JOIN STRIPE_ITEMS S ON
     * M.id = S.id WHERE M.id='0x200000401:0x5:0x0'
     *
     * DELETE FROM NAMES WHERE pkn=sha1(CONCAT('0x200000007:0x1:0x0', '/',
     * 'directory')) AND id='0x200000401:0x5:0x0'
     */
    ATTR_MASK_SET(&del_attrs, parent_id);
    ATTR_MASK_SET(&del_attrs, name);

    memcpy(&ATTR(&del_attrs, parent_id), &test_data->fs_fid,
           sizeof(test_data->fs_fid));
    snprintf(print_buffer, sizeof(print_buffer), "dir%i",
             test_data->dir_number);
    ++test_data->dir_number;
    strcpy(ATTR(&del_attrs, name), print_buffer);

    rc = ListMgr_Remove(&mgr, &test_data->dir_fid, &del_attrs, true);

done:
    ListMgr_FreeAttrs(&sel_attrs);
    ListMgr_FreeAttrs(&del_attrs);

    return rc;
}

int rmdir_test_init(void)
{
    int   rc;
    void *mkdir_data;
    int   i;

    rc = mkdir_test_init();
    if (rc != 0)
        return rc;

    for (i = 0; i < n_test_records; ++i) {
        mkdir_data = get_next_dir_data();
        if (mkdir_data == NULL)
            return ENODATA;

        rc = mkdir_test(mkdir_data, NULL);
        if (rc != 0)
            return rc;
    }

    memcpy(&dir_data.dir_fid, &dir_data.fs_fid, sizeof(dir_data.dir_fid));
    dir_data.dir_number = 0;

    return 0;
}

static entry_id_t first_free_fid;

int lhsm_release_restore_tests_init(void)
{
    int rc;

    rc = get_fids_shuffled();
    if (rc != 0)
        return rc;

    rc = get_max_fid(&first_free_fid);
    if (rc != 0)
        return rc;

    return inc_fid(&first_free_fid);
}

int lhsm_release_test(void *data, void**result)
{
    struct two_attrsets_data *attr_sets;
    attr_set_t                attrs;
    int                       rc;
    sm_instance_t            *sm_lhsm;
    stripe_info_t             stripe_info;

    if (data == NULL)
        return EINVAL;

    attr_sets = malloc(sizeof(*attr_sets));
    if (result != NULL)
        *result = attr_sets;

    sm_lhsm = LHSM_SMI;
    if (sm_lhsm == NULL) {
        rc = EINVAL;
        goto done;
    }

    /* Initialise all attribute sets. */
    ATTR_SET_INIT_ST(&attr_sets->first);
    ATTR_SET_INIT_ST(&attr_sets->second);
    ATTR_SET_INIT_ST(&attrs);

    /* SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x2:0x0'
     */
    ATTR_MASK_SET(&attr_sets->first, size);
    ATTR_MASK_SET(&attr_sets->first, path_update);
    ATTR_MASK_SET(&attr_sets->first, fullpath);
    ATTR_MASK_STATUS_SET(&attr_sets->first, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, data, &attr_sets->first);
    if (rc != 0)
        goto done;

    /** Prepare SQL statement like the following:
     * \code{.unparsed}
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=1,
     * last_access=1475834490, last_mod=1475834490, type='file', mode=420,
     * nlink=1, md_update=1475834606, fileclass='++', class_update=1475834606,
     * lhsm_status='released', lhsm_archid=1, lhsm_norels=0, lhsm_noarch=0 WHERE
     * id='0x200000401:0x2:0x0'
     * \endcode
     */
    ListMgr_FreeAttrs(&attrs);
    ATTR_MASK_SET(&attrs, owner);
    ATTR_MASK_SET(&attrs, gr_name);
    ATTR_MASK_SET(&attrs, blocks);
    ATTR_MASK_SET(&attrs, last_access);
    ATTR_MASK_SET(&attrs, last_mod);
    ATTR_MASK_SET(&attrs, type);
    ATTR_MASK_SET(&attrs, mode);
    ATTR_MASK_SET(&attrs, nlink);
    ATTR_MASK_SET(&attrs, md_update);
    ATTR_MASK_SET(&attrs, fileclass);
    ATTR_MASK_SET(&attrs, class_update);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);
    ATTR_MASK_INFO_SET(&attrs, sm_lhsm, ATTR_ARCHIVE_ID);
    ATTR_MASK_INFO_SET(&attrs, sm_lhsm, ATTR_NO_RELEASE);
    ATTR_MASK_INFO_SET(&attrs, sm_lhsm, ATTR_NO_ARCHIVE);

    strcpy(ATTR(&attrs, owner), "2000");
    strcpy(ATTR(&attrs, gr_name), "root");
    ATTR(&attrs, blocks) = 0;
    ATTR(&attrs, last_access) = time(NULL);
    ATTR(&attrs, last_mod) =
        ATTR(&attrs, last_access) + 1;
    strcpy(ATTR(&attrs, type), "file");
    ATTR(&attrs, mode) = 420;
    ATTR(&attrs, nlink) = 1;
    ATTR(&attrs, md_update) =
        ATTR(&attrs, last_access) + 2;
    strcpy(ATTR(&attrs, fileclass),
           "system_test_file_class");
    ATTR(&attrs, class_update) =
        ATTR(&attrs, last_access) + 3;
    sm_status_ensure_alloc(&attrs.attr_values.sm_status);
    sm_info_ensure_alloc(&attrs.attr_values.sm_info);
    /* The next line uses direct literal - as in 'lhsm' status manager. */
    STATUS_ATTR(&attrs, sm_lhsm->smi_index) = "released";
    SMI_INFO(&attrs, sm_lhsm, ATTR_ARCHIVE_ID) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attrs, sm_lhsm, ATTR_ARCHIVE_ID) = 1;
    SMI_INFO(&attrs, sm_lhsm, ATTR_NO_RELEASE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attrs, sm_lhsm, ATTR_NO_RELEASE) = 0;
    SMI_INFO(&attrs, sm_lhsm, ATTR_NO_ARCHIVE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attrs, sm_lhsm, ATTR_NO_ARCHIVE) = 0;

    rc = ListMgr_Update(&mgr, data, &attrs);
    if (rc != 0)
        goto done;

    /* SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x2:0x0'
     */
    ListMgr_FreeAttrs(&attrs);
    ATTR_MASK_SET(&attrs, size);
    ATTR_MASK_SET(&attrs, path_update);
    ATTR_MASK_SET(&attrs, fullpath);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, data, &attrs);
    if (rc != 0)
        goto done;

    /* SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x3:0x0'
     *                         ^^^
     */
    ListMgr_FreeAttrs(&attrs);
    ATTR_SET_INIT_ST(&attrs);
    ATTR_MASK_SET(&attrs, size);
    ATTR_MASK_SET(&attrs, path_update);
    ATTR_MASK_SET(&attrs, fullpath);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, &first_free_fid, &attrs);
    if (rc != 2) {
        rc = -1;
        goto done;
    }

    /* Preparing SQL UPDATE like:
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=1,
     * last_access=1475834490, last_mod=1475834490, type='file', mode=420,
     * nlink=1, md_update=1475834606, fileclass='++', class_update=1475834606
     * WHERE id='0x200000401:0x2:0x0'
     */
    ListMgr_FreeAttrs(&attrs);
    ATTR_MASK_SET(&attrs, owner);
    ATTR_MASK_SET(&attrs, gr_name);
    ATTR_MASK_SET(&attrs, blocks);
    ATTR_MASK_SET(&attrs, last_access);
    ATTR_MASK_SET(&attrs, last_mod);
    ATTR_MASK_SET(&attrs, type);
    ATTR_MASK_SET(&attrs, mode);
    ATTR_MASK_SET(&attrs, nlink);
    ATTR_MASK_SET(&attrs, md_update);
    ATTR_MASK_SET(&attrs, fileclass);
    ATTR_MASK_SET(&attrs, class_update);

    strcpy(ATTR(&attrs, owner), "2000");
    strcpy(ATTR(&attrs, gr_name), "root");
    ATTR(&attrs, blocks) = 0;
    ATTR(&attrs, last_access) = time(NULL);
    ATTR(&attrs, last_mod) =
        ATTR(&attrs, last_access) + 1;
    strcpy(ATTR(&attrs, type), "file");
    ATTR(&attrs, mode) = 420;
    ATTR(&attrs, nlink) = 1;
    ATTR(&attrs, md_update) =
        ATTR(&attrs, last_access) + 2;
    strcpy(ATTR(&attrs, fileclass),
           "system_test_file_class");
    ATTR(&attrs, class_update) =
        ATTR(&attrs, last_access) + 3;

    rc = ListMgr_Update(&mgr, data, &attrs);
    if (rc != 0)
        goto done;

    ListMgr_FreeAttrs(&attrs);
    ATTR_SET_INIT_ST(&attrs);
    /* SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x2:0x0'
     */
    ATTR_MASK_SET(&attrs, type);
    ATTR_MASK_SET(&attrs, path_update);
    ATTR_MASK_SET(&attrs, fullpath);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, data, &attrs);
    if (rc != 0)
        goto done;

    ListMgr_FreeAttrs(&attrs);
    ATTR_SET_INIT_ST(&attrs);
    /* SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x3:0x0'
     *                         ^^^
     */
    ATTR_MASK_SET(&attrs, type);
    ATTR_MASK_SET(&attrs, path_update);
    ATTR_MASK_SET(&attrs, fullpath);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);

    rc = ListMgr_Get(&mgr, &first_free_fid, &attrs);
    if (rc != 2) {
        rc = -1;
        goto done;
    }

    /* Do not free attrs: we'll definitely do it later. */

    /* Preparing UPDATE SQL statement like:
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=1,
     * last_access=1475834490, last_mod=1475834490, type='file', mode=420,
     * nlink=1, md_update=1475834606, fileclass='++', class_update=1475834606
     * WHERE id='0x200000401:0x2:0x0'
     */
    ATTR_MASK_SET(&attr_sets->second, owner);
    ATTR_MASK_SET(&attr_sets->second, gr_name);
    ATTR_MASK_SET(&attr_sets->second, blocks);
    ATTR_MASK_SET(&attr_sets->second, last_access);
    ATTR_MASK_SET(&attr_sets->second, last_mod);
    ATTR_MASK_SET(&attr_sets->second, type);
    ATTR_MASK_SET(&attr_sets->second, mode);
    ATTR_MASK_SET(&attr_sets->second, nlink);
    ATTR_MASK_SET(&attr_sets->second, md_update);
    ATTR_MASK_SET(&attr_sets->second, fileclass);
    ATTR_MASK_SET(&attr_sets->second, class_update);

    strcpy(ATTR(&attr_sets->second, owner), "2000");
    strcpy(ATTR(&attr_sets->second, gr_name), "root");
    ATTR(&attr_sets->second, blocks) = 0;
    ATTR(&attr_sets->second, last_access) = time(NULL);
    ATTR(&attr_sets->second, last_mod) =
        ATTR(&attr_sets->second, last_access) + 1;
    strcpy(ATTR(&attr_sets->second, type), "file");
    ATTR(&attr_sets->second, mode) = 420;
    ATTR(&attr_sets->second, nlink) = 1;
    ATTR(&attr_sets->second, md_update) =
        ATTR(&attr_sets->second, last_access) + 2;
    strcpy(ATTR(&attr_sets->second, fileclass),
           "system_test_file_class");
    ATTR(&attr_sets->second, class_update) =
        ATTR(&attr_sets->second, last_access) + 3;

    rc = ListMgr_Update(&mgr, data, &attr_sets->second);
    if (rc != 0)
        goto done;

    /* INSERT INTO STRIPE_INFO (id, validator, stripe_count, stripe_size,
     * pool_name) VALUES ('0x200000401:0x2:0x0', 1, 1, 1048576, '') ON DUPLICATE
     * KEY UPDATE validator=VALUES(validator),
     * stripe_count=VALUES(stripe_count), stripe_size=VALUES(stripe_size),
     * pool_name=VALUES(pool_name)
     *
     * DELETE FROM STRIPE_ITEMS WHERE id='0x200000401:0x2:0x0'
     */
    stripe_info.validator = 1;
    stripe_info.stripe_count = 1;
    stripe_info.stripe_size = 1048576;
    stripe_info.pool_name[0] = '\000'; /* Empty */
    rc = ListMgr_SetStripe(&mgr, data, &stripe_info, NULL);
done:
    if (result == NULL) {
        ListMgr_FreeAttrs(&attr_sets->first);
        ListMgr_FreeAttrs(&attr_sets->second);
        free(attr_sets);
    }
    ListMgr_FreeAttrs(&attrs);

    return rc;
}

int lhsm_restore_test(void *data, void**result)
{
    struct two_attrsets_data *attr_sets;
    attr_set_t                attrs;
    int                       rc;
    sm_instance_t            *sm_lhsm;
    stripe_info_t             stripe_info;
    stripe_item_t             stripe_item;
    stripe_items_t            stripe_items;
    int                       i;

    if (data == NULL)
        return EINVAL;

    attr_sets = malloc(sizeof(*attr_sets));
    if (result != NULL)
        *result = attr_sets;

    sm_lhsm = LHSM_SMI;
    if (sm_lhsm == NULL) {
        rc = EINVAL;
        goto done;
    }

    /* Initialise all attribute sets. */
    ATTR_SET_INIT_ST(&attr_sets->first);
    ATTR_SET_INIT_ST(&attr_sets->second);
    ATTR_SET_INIT_ST(&attrs);

    /* Requesting information for data FID - not present in DB.
     * SELECT size, type, lhsm_status, link, path_update, this_path(parent_id,
     * name) FROM ENTRIES LEFT JOIN ANNEX_INFO ON ENTRIES.id=ANNEX_INFO.id LEFT
     * JOIN NAMES ON ENTRIES.id=NAMES.id WHERE ENTRIES.id='0x200000401:0x4:0x0'
     */
    ATTR_MASK_SET(&attrs, size);
    ATTR_MASK_SET(&attrs, type);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);
    ATTR_MASK_SET(&attrs, link);
    ATTR_MASK_SET(&attrs, path_update);
    ATTR_MASK_SET(&attrs, fullpath);

    rc = ListMgr_Get(&mgr, &first_free_fid, &attrs);
    if (rc != 2) {
        rc = -1;
        goto done;
    }

    /* Request information on actual FID - present in DB.
     * SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x2:0x0'
     */
    ListMgr_FreeAttrs(&attrs);
    ATTR_SET_INIT_ST(&attrs);
    ATTR_MASK_SET(&attrs, size);
    ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);
    ATTR_MASK_SET(&attrs, path_update);
    ATTR_MASK_SET(&attrs, fullpath);

    rc = ListMgr_Get(&mgr, data, &attrs);
    if (rc != 0)
        goto done;

    /* Select data for DFID (5 times by log) - not present in DB.
     * SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x4:0x0'
     */
    for (i = 0; i < 5; ++i) {
        ListMgr_FreeAttrs(&attrs);
        ATTR_SET_INIT_ST(&attrs);
        ATTR_MASK_SET(&attrs, size);
        ATTR_MASK_STATUS_SET(&attrs, sm_lhsm->smi_index);
        ATTR_MASK_SET(&attrs, path_update);
        ATTR_MASK_SET(&attrs, fullpath);

        rc = ListMgr_Get(&mgr, &first_free_fid, &attrs);
        if (rc != 2) {
            rc = -1;
            goto done;
        }
    }

    /*
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=8,
     * last_access=1475834490, last_mod=1475834490, type='file', mode=420,
     * nlink=1, md_update=1475834650, fileclass='++', class_update=1475834650
     * WHERE id='0x200000401:0x2:0x0'
     */
    ListMgr_FreeAttrs(&attrs);
    ATTR_MASK_SET(&attrs, owner);
    ATTR_MASK_SET(&attrs, gr_name);
    ATTR_MASK_SET(&attrs, blocks);
    ATTR_MASK_SET(&attrs, last_access);
    ATTR_MASK_SET(&attrs, last_mod);
    ATTR_MASK_SET(&attrs, type);
    ATTR_MASK_SET(&attrs, mode);
    ATTR_MASK_SET(&attrs, nlink);
    ATTR_MASK_SET(&attrs, md_update);
    ATTR_MASK_SET(&attrs, fileclass);
    ATTR_MASK_SET(&attrs, class_update);

    strcpy(ATTR(&attrs, owner), "2000");
    strcpy(ATTR(&attrs, gr_name), "root");
    ATTR(&attrs, blocks) = 0;
    ATTR(&attrs, last_access) = time(NULL);
    ATTR(&attrs, last_mod) =
        ATTR(&attrs, last_access) + 1;
    strcpy(ATTR(&attrs, type), "file");
    ATTR(&attrs, mode) = 420;
    ATTR(&attrs, nlink) = 1;
    ATTR(&attrs, md_update) =
        ATTR(&attrs, last_access) + 2;
    strcpy(ATTR(&attrs, fileclass),
           "system_test_file_class");
    ATTR(&attrs, class_update) =
        ATTR(&attrs, last_access) + 3;

    rc = ListMgr_Update(&mgr, data, &attrs);
    if (rc != 0)
        goto done;

    /*
     * INSERT INTO STRIPE_INFO (id, validator, stripe_count, stripe_size,
     * pool_name) VALUES ('0x200000401:0x2:0x0', 2, 1, 1048576, '') ON DUPLICATE
     * KEY UPDATE validator=VALUES(validator),
     * stripe_count=VALUES(stripe_count), stripe_size=VALUES(stripe_size),
     * pool_name=VALUES(pool_name)
     *
     * DELETE FROM STRIPE_ITEMS WHERE id='0x200000401:0x2:0x0'
     *
     * INSERT INTO STRIPE_ITEMS (id, stripe_index, ostidx, details) VALUES
     * ('0x200000401:0x2:0x0', 0, 0,
     * x'0000000002000000000000000000000000000000')
     */
    stripe_info.validator = 1;
    stripe_info.stripe_count = 1;
    stripe_info.stripe_size = 1048576;
    stripe_info.pool_name[0] = '\000'; /* Empty */
    stripe_items.count = 1;
    stripe_items.stripe = &stripe_item;
    stripe_item.ost_idx = 0;
    stripe_item.ost_gen = 0;
    stripe_item.obj_id = 2;
    stripe_item.obj_seq = 0;
    rc = ListMgr_SetStripe(&mgr, data, &stripe_info, &stripe_items);
    if (rc != 0)
        goto done;

    /* About real FID.
     * SELECT size, lhsm_status, path_update, this_path(parent_id, name) FROM
     * ENTRIES LEFT JOIN NAMES ON ENTRIES.id=NAMES.id WHERE
     * ENTRIES.id='0x200000401:0x2:0x0'
     */
    ATTR_MASK_SET(&attr_sets->first, size);
    ATTR_MASK_STATUS_SET(&attr_sets->first, sm_lhsm->smi_index);
    ATTR_MASK_SET(&attr_sets->first, path_update);
    ATTR_MASK_SET(&attr_sets->first, fullpath);

    rc = ListMgr_Get(&mgr, data, &attr_sets->first);
    if (rc != 0)
        goto done;

    /* About real FID.
     * UPDATE ENTRIES SET owner='root', gr_name='root', blocks=8,
     * last_access=1475834490, last_mod=1475834490, type='file', mode=420,
     * nlink=1, md_update=1475834650, fileclass='++', class_update=1475834650,
     * lhsm_status='synchro', lhsm_lstrst=1475834641 WHERE
     * id='0x200000401:0x2:0x0'
     */
    ATTR_MASK_SET(&attr_sets->second, owner);
    ATTR_MASK_SET(&attr_sets->second, gr_name);
    ATTR_MASK_SET(&attr_sets->second, blocks);
    ATTR_MASK_SET(&attr_sets->second, last_access);
    ATTR_MASK_SET(&attr_sets->second, last_mod);
    ATTR_MASK_SET(&attr_sets->second, type);
    ATTR_MASK_SET(&attr_sets->second, mode);
    ATTR_MASK_SET(&attr_sets->second, nlink);
    ATTR_MASK_SET(&attr_sets->second, md_update);
    ATTR_MASK_SET(&attr_sets->second, fileclass);
    ATTR_MASK_SET(&attr_sets->second, class_update);
    ATTR_MASK_STATUS_SET(&attr_sets->second, sm_lhsm->smi_index);
    ATTR_MASK_INFO_SET(&attr_sets->second, sm_lhsm, ATTR_LAST_RESTORE);

    strcpy(ATTR(&attr_sets->second, owner), "2000");
    strcpy(ATTR(&attr_sets->second, gr_name), "root");
    ATTR(&attr_sets->second, blocks) = 0;
    ATTR(&attr_sets->second, last_access) = time(NULL);
    ATTR(&attr_sets->second, last_mod) =
        ATTR(&attr_sets->second, last_access) + 1;
    strcpy(ATTR(&attr_sets->second, type), "file");
    ATTR(&attr_sets->second, mode) = 420;
    ATTR(&attr_sets->second, nlink) = 1;
    ATTR(&attr_sets->second, md_update) =
        ATTR(&attr_sets->second, last_access) + 2;
    strcpy(ATTR(&attr_sets->second, fileclass),
           "system_test_file_class");
    ATTR(&attr_sets->second, class_update) =
        ATTR(&attr_sets->second, last_access) + 3;
    sm_status_ensure_alloc(&attr_sets->second.attr_values.sm_status);
    sm_info_ensure_alloc(&attr_sets->second.attr_values.sm_info);
    /* The next line uses direct literal - as in 'lhsm' status manager. */
    STATUS_ATTR(&attr_sets->second, sm_lhsm->smi_index) = "synchro";
    SMI_INFO(&attr_sets->second, sm_lhsm, ATTR_LAST_RESTORE) =
        MemAlloc(sizeof(int));
    *(int*)SMI_INFO(&attr_sets->second, sm_lhsm, ATTR_LAST_RESTORE) =
        ATTR(&attr_sets->second, last_access) + 4;

    rc = ListMgr_Update(&mgr, data, &attr_sets->second);

done:
    if (result == NULL) {
        ListMgr_FreeAttrs(&attr_sets->first);
        ListMgr_FreeAttrs(&attr_sets->second);
        free(attr_sets);
    }
    ListMgr_FreeAttrs(&attrs);

    return rc;
}
