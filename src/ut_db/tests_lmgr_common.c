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

int get_fids_shuffled(void);
int get_fids_shuffled(void) {
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
    struct chmod_test_data *attr_sets;
    int                     rc;

    if (data == NULL)
        return EINVAL;

    attr_sets = malloc(sizeof(*attr_sets));
    if (result != NULL)
        *result = attr_sets;

    ATTR_MASK_INIT(&attr_sets->attrs);
    ATTR_MASK_SET(&attr_sets->attrs, size);
    ATTR_MASK_SET(&attr_sets->attrs, type);
    ATTR_MASK_SET(&attr_sets->attrs, link);
    ATTR_MASK_SET(&attr_sets->attrs, path_update);
    ATTR_MASK_SET(&attr_sets->attrs, fullpath);
    ATTR_MASK_STATUS_SET(&attr_sets->attrs, 0);

    ATTR_MASK_INIT(&attr_sets->upd_attrs);

    rc = ListMgr_Get(&mgr, data, &attr_sets->attrs);
    if (rc != 0)
        goto done;

    ATTR_MASK_SET(&attr_sets->upd_attrs, owner);
    ATTR_MASK_SET(&attr_sets->upd_attrs, gr_name);
    ATTR_MASK_SET(&attr_sets->upd_attrs, blocks);
    ATTR_MASK_SET(&attr_sets->upd_attrs, last_access);
    ATTR_MASK_SET(&attr_sets->upd_attrs, last_mod);
    ATTR_MASK_SET(&attr_sets->upd_attrs, mode);
    ATTR_MASK_SET(&attr_sets->upd_attrs, nlink);
    ATTR_MASK_SET(&attr_sets->upd_attrs, md_update);
    ATTR_MASK_SET(&attr_sets->upd_attrs, fileclass);
    ATTR_MASK_SET(&attr_sets->upd_attrs, class_update);

    strcpy(ATTR(&attr_sets->upd_attrs, owner), "root");
    strcpy(ATTR(&attr_sets->upd_attrs, gr_name), "root");
    ATTR(&attr_sets->upd_attrs, blocks) = 10;
    ATTR(&attr_sets->upd_attrs, last_access) = time(NULL);
    ATTR(&attr_sets->upd_attrs, last_mod) = ATTR(&attr_sets->upd_attrs,
                                                 last_access) + 1;
    ATTR(&attr_sets->upd_attrs, mode) = 932;
    ATTR(&attr_sets->upd_attrs, nlink) = 1;
    ATTR(&attr_sets->upd_attrs, md_update) = ATTR(&attr_sets->upd_attrs,
                                                  last_mod) + 1;
    strcpy(ATTR(&attr_sets->upd_attrs, fileclass), "test_file_class");
    ATTR(&attr_sets->upd_attrs, class_update) = ATTR(&attr_sets->upd_attrs,
                                                     md_update) + 1;

    rc = ListMgr_Update(&mgr, data, &attr_sets->upd_attrs);
    if (rc != 0)
        goto done;

done:
    if (result == NULL)
        free(attr_sets);

    return rc;
}
