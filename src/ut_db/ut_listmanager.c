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

#include <errno.h>

#include "Basic.h"

#include "database.h"
#include "status_manager.h"
#include "listmgr_common.h"

#include "tests_lmgr_common.h"

extern char *config_file_name;
extern char *dump_file;

int list_manager_suite_init(void);
void list_manager_test_init(void);
void list_manager_test_fini(void);

void list_manager_simple_test(void);
void list_manager_chmod_test(void);

#define UNIT_TEST_INFO(test_name) \
{#test_name, (test_name)}

void list_manager_simple_test(void)
{
    int        rc;
    const char min_file_fid_req[] = "select "MAIN_TABLE".id, "
        DNAMES_TABLE".name from "MAIN_TABLE" JOIN "DNAMES_TABLE" on "
        MAIN_TABLE".id="DNAMES_TABLE".id where "MAIN_TABLE".id=("
        "SELECT MIN(id) from "MAIN_TABLE" where type='file')";
    MYSQL_ROW  row;
    MYSQL_RES *sql_result;
    entry_id_t id;
    attr_set_t attrs;
    char      *fid;
    char      *name;

    rc = mysql_real_query(&mgr.conn, min_file_fid_req,
                          strlen(min_file_fid_req));
    CU_ASSERT_EQUAL(rc, 0);
    sql_result = mysql_store_result(&mgr.conn);
    CU_ASSERT_NOT_EQUAL(sql_result, NULL);
    row = mysql_fetch_row(sql_result);
    fid = strdup(row[0]);
    name = strdup(row[1]);
    mysql_free_result(sql_result);
    pk2entry_id(&mgr, fid, &id);
    free(fid);

    ATTR_MASK_INIT(&attrs);
    attrs.attr_mask.std = POSIX_ATTR_MASK;
    attr_mask_set_index(&attrs.attr_mask, ATTR_INDEX_name);

    rc = ListMgr_Get(&mgr, &id, &attrs);
    CU_ASSERT_EQUAL(rc, 0);
    CU_ASSERT_STRING_EQUAL(name, attrs.attr_values.name);
    ListMgr_FreeAttrs(&attrs);
    free(name);
}

int get_min_file_fid(entry_id_t *id);
int get_min_file_fid(entry_id_t *id)
{
    int        rc;
    const char min_file_fid_req[] = "select MIN(id) min_id from "MAIN_TABLE
        " where type='file'";
    MYSQL_RES *sql_result;
    MYSQL_ROW  row;
    char      *fid;

    rc = mysql_real_query(&mgr.conn, min_file_fid_req,
                          strlen(min_file_fid_req));
    if (rc != 0)
        return rc;

    sql_result = mysql_store_result(&mgr.conn);
    if (sql_result == NULL)
        return ENODATA;

    row = mysql_fetch_row(sql_result);
    if (*row == NULL)
        return ENODATA;

    fid = strdup(row[0]);
    mysql_free_result(sql_result);
    pk2entry_id(&mgr, fid, id);
    free(fid);

    return 0;
}

void list_manager_chmod_test(void)
{
    int        rc;
    entry_id_t id;
    struct chmod_test_data *results;
    attr_set_t changed_attrs;

    rc = get_min_file_fid(&id);
    CU_ASSERT_EQUAL(rc, 0);

    rc = chmod_test(&id, (void**)&results);
    CU_ASSERT_EQUAL(rc , 0);

    ATTR_MASK_INIT(&changed_attrs);
    ATTR_MASK_SET(&changed_attrs, size);
    ATTR_MASK_SET(&changed_attrs, type);
    ATTR_MASK_SET(&changed_attrs, link);
    ATTR_MASK_SET(&changed_attrs, path_update);
    ATTR_MASK_SET(&changed_attrs, fullpath);
    ATTR_MASK_STATUS_SET(&changed_attrs, 0);
    ATTR_MASK_SET(&changed_attrs, owner);
    ATTR_MASK_SET(&changed_attrs, gr_name);
    ATTR_MASK_SET(&changed_attrs, blocks);
    ATTR_MASK_SET(&changed_attrs, last_access);
    ATTR_MASK_SET(&changed_attrs, last_mod);
    ATTR_MASK_SET(&changed_attrs, mode);
    ATTR_MASK_SET(&changed_attrs, nlink);
    ATTR_MASK_SET(&changed_attrs, fileclass);
    rc = ListMgr_Get(&mgr, &id, &changed_attrs);
    CU_ASSERT_EQUAL(rc, 0);

    CU_ASSERT_EQUAL(ATTR(&changed_attrs, size), ATTR(&results->attrs, size));
    CU_ASSERT_STRING_EQUAL(ATTR(&changed_attrs, type),
                           ATTR(&results->attrs, type));
    CU_ASSERT_STRING_EQUAL(ATTR(&changed_attrs, link),
                           ATTR(&results->attrs, link));
    CU_ASSERT_EQUAL(ATTR(&changed_attrs, path_update),
                    ATTR(&results->attrs, path_update));
    CU_ASSERT_STRING_EQUAL(ATTR(&changed_attrs, fullpath),
                           ATTR(&results->attrs, fullpath));
    CU_ASSERT_STRING_EQUAL(ATTR(&changed_attrs, owner),
                           ATTR(&results->upd_attrs, owner));
    CU_ASSERT_STRING_EQUAL(ATTR(&changed_attrs, gr_name),
                           ATTR(&results->upd_attrs, gr_name));
    CU_ASSERT_EQUAL(ATTR(&changed_attrs, blocks),
                    ATTR(&results->upd_attrs, blocks));
    CU_ASSERT_EQUAL(ATTR(&changed_attrs, last_access),
                    ATTR(&results->upd_attrs, last_access));
    CU_ASSERT_EQUAL(ATTR(&changed_attrs, last_mod),
                    ATTR(&results->upd_attrs, last_mod));
    CU_ASSERT_EQUAL(ATTR(&changed_attrs, mode),
                    ATTR(&results->upd_attrs, mode));
    CU_ASSERT_EQUAL(ATTR(&changed_attrs, nlink),
                    ATTR(&results->upd_attrs, nlink));
    CU_ASSERT_STRING_EQUAL(ATTR(&changed_attrs, fileclass),
                           ATTR(&results->upd_attrs, fileclass));

    ListMgr_FreeAttrs(&changed_attrs);
    free(results);
}

int list_manager_suite_init(void)
{
    return suite_setup();
}

void list_manager_test_init(void)
{
    int          rc;

    rc = test_setup();
    CU_ASSERT_EQUAL_FATAL(rc, 0);
}

void list_manager_test_fini(void)
{
    int rc;

    rc = test_teardown();
    CU_ASSERT_EQUAL(rc, 0);
}

CU_TestInfo list_manager_suite[] = {
    UNIT_TEST_INFO(list_manager_simple_test),
    UNIT_TEST_INFO(list_manager_chmod_test),
    CU_TEST_INFO_NULL
};
