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

#ifndef TESTS_LMGR_COMMON_H
#define TESTS_LMGR_COMMON_H

#include <stdlib.h>

#include "list_mgr.h"
#include "status_manager.h"

/* Copied from src/modules/lhsm.c */
enum lhsm_info_e
{
    ATTR_ARCHIVE_ID = 0,
    ATTR_NO_RELEASE,
    ATTR_NO_ARCHIVE,
    ATTR_LAST_ARCHIVE,
    ATTR_LAST_RESTORE
};

/* Copied from src/modules/lhsm.c */
/** set of managed status */
typedef enum {
  STATUS_NEW,                   /* file has no HSM flags (just created) */
  STATUS_MODIFIED,              /* file must be archived */
  STATUS_RESTORE_RUNNING,       /* file is being retrieved */
  STATUS_ARCHIVE_RUNNING,       /* file is being archived */
  STATUS_SYNCHRO,               /* file has been synchronized in HSM,
                                   file can be purged */
  STATUS_RELEASED,              /* file is released (nothing to do).
                                   XXX should not be in DB? */

  STATUS_COUNT,                 /* number of possible file status */
} hsm_status_t;

/** Path to robinhood configuration file. */
extern char *config_file_name;
/** Path to robinhood DB dump file.
 * @attention Must correspond to configuration file.
 */
extern char *dump_file;
/** Number of records in main table of robinhood DB. */
extern size_t n_records;
extern        lmgr_t mgr;
extern bool   list_manager_initialised;

int suite_setup(void);
int test_setup(void);
int test_teardown(void);

/** Test data generator function. */
typedef int (*generate_test_data_fcn)(void);

/** Return pointer to next test data record. */
typedef void* (*get_test_data_fcn)(void);

/** Free resources allocated for test data. */
typedef void (*free_test_data_fcn)(void);

/** Runs single test action. */
typedef int (*test_action)(void *data, void **result);

struct st_test_info {
    generate_test_data_fcn generate;
    get_test_data_fcn      get;
    free_test_data_fcn     free;
    test_action            test;
    /* Time spent in tests. */
    struct timespec        ts;
    const char            *test_name;
    bool                   failed;
};

int chmod_test(void *data, void **result);
int lhsm_archive_test(void *data, void**result);
int mkdir_test(void *data, void **result);
int rmdir_test(void *data, void **result);

int get_fids_shuffled(void);
void free_fids(void);
void *get_next_fid(void);

int get_max_fid(entry_id_t *id);
int inc_fid(entry_id_t *id);

struct dir_test_data {
    /** Lustre uses FID that is not present in @ref MAIN_TABLE as FS FID. */
    entry_id_t fs_fid;
    entry_id_t dir_fid;
    int        dir_number;
};

int mkdir_test_init(void);
int rmdir_test_init(void);
void *get_next_dir_data(void);

#define LHSM_SMI smi_by_name("lhsm")

/** Get ChangelogLastCommit full variable name according to loaded
 * configuration.
 */
const char *get_cl_last_committed_name(void);

/** Initialise attribute set.
 * \attr _a pointer to attr_set_t.
 */
#define ATTR_SET_INIT_ST(_a)            \
do {                                    \
    ATTR_MASK_INIT(_a);                 \
    (_a)->attr_values.sm_status = NULL; \
    (_a)->attr_values.sm_info = NULL;    \
} while (false)

struct chmod_test_data {
    attr_set_t attrs;
    attr_set_t upd_attrs;
};

struct lhsm_archive_test_data {
    attr_set_t attrs;
    attr_set_t updated1_attrs;
    attr_set_t updated2_attrs;
    attr_set_t updated3_attrs;
};

struct mkdir_test_data {
    attr_set_t sel_attrs;
    attr_set_t ins_attrs;
};

#endif
