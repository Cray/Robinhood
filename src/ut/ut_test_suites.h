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

#ifndef UT_TEST_SUITES_H
#define UT_TEST_SUITES_H

/**
 * @file ut_test_suites.h
 * Declare test suites and list them for test suite registry.
 * @attention To be included in main unit test runner file only!
 */

extern CU_TestInfo changelog_suite[];

int changelog_suite_init(void);
int changelog_suite_fini(void);
void changelog_test_init(void);
void changelog_test_fini(void);

/**
 * Test suites list.
 */
CU_SuiteInfo suites[] = {
    {"changelog_suite", changelog_suite_init, changelog_suite_fini,
        changelog_test_init, changelog_test_fini,
        changelog_suite},
	CU_SUITE_INFO_NULL,
};

#endif
