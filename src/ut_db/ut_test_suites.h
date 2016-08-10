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

extern CU_TestInfo list_manager_suite[];

int list_manager_suite_init(void);
void list_manager_test_init(void);
void list_manager_test_fini(void);

/**
 * Test suites list.
 */
CU_SuiteInfo suites[] = {
	{"list_manager_suite", list_manager_suite_init, NULL,
        list_manager_test_init, list_manager_test_fini,
        list_manager_suite},
	CU_SUITE_INFO_NULL,
};

#endif
