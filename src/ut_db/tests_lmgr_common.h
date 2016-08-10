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
};

int chmod_test(void *data, void **result);
void free_fids(void);
void *get_next_fid(void);

struct chmod_test_data {
    attr_set_t attrs;
    attr_set_t upd_attrs;
};

#endif
