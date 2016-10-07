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

#ifndef ST_TESTS_H
#define ST_TESTS_H

#include "tests_lmgr_common.h"

#define NULL_TEST_INFO { .generate = NULL, .get = NULL, .free = NULL, \
    .test = NULL }
#define TEST_INFO(_generate, _get, _free, _test, _name) \
{ \
    .generate = _generate, .get = _get, .free = _free, .test = _test, \
    .ts = {0, 0}, .test_name = _name, .failed = false }

struct st_test_info test_infos[] = {
    TEST_INFO(get_fids_shuffled, get_next_fid, free_fids, lhsm_archive_test,
              "HSM ARCHIVE SQL sequence"),
    TEST_INFO(lhsm_release_test_init, get_next_fid, free_fids,
              lhsm_release_test, "HSM RELEASE SQL sequence"),
    TEST_INFO(get_fids_shuffled, get_next_fid, free_fids, chmod_test,
              "CHMOD SQL sequence"),
    TEST_INFO(mkdir_test_init, get_next_dir_data, free_fids, mkdir_test,
              "MKDIR SQL sequence"),
    TEST_INFO(rmdir_test_init, get_next_dir_data, free_fids, rmdir_test,
              "RMDIR SQL sequence"),
    NULL_TEST_INFO
};

#endif
