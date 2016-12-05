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

#include "chglog_postproc.h"
#include "rbh_modules.h"

#include <stdlib.h>

#define UNIT_TEST(test_name)  \
    void test_name(void); \
    void test_name(void)

#define UNIT_TEST_INFO(test_name) \
{#test_name, (test_name)}

int cpp_suite_init(void);
void cpp_test_fini(void);

extern cpp_instance_t *cpp_inst;

static int cleanup(void);

int cpp_suite_init(void)
{
    return cleanup();
}

static int cleanup(void)
{
	int rc;

	free(cpp_inst);
	cpp_inst = NULL;
	cpp_inst_count = 0;

	rc = module_unload_all();
    return rc;
}

void cpp_test_fini(void)
{
	int rc;

	rc = cleanup();
	CU_ASSERT_EQUAL(rc, 0);
}

UNIT_TEST(test_create_cpp_instance_fail_cases)
{
    cpp_instance_t *cpp_instance;

    cpp_instance = create_cpp_instance("no_such_cpp_module");
    CU_ASSERT_PTR_NULL(cpp_instance);

    cpp_instance = create_cpp_instance("lhsm");
    CU_ASSERT_PTR_NULL(cpp_instance);
}

UNIT_TEST(test_create_cpp_collapse_module)
{
    cpp_instance_t *cpp_instance;
    cpp_instance_t *cpp_instance_by_name;

    cpp_instance = create_cpp_instance("collapse");
    CU_ASSERT_PTR_NOT_NULL(cpp_instance);

    cpp_instance_by_name = cpp_by_name("collapse");
    CU_ASSERT_PTR_NOT_NULL(cpp_instance_by_name);
    CU_ASSERT_EQUAL(cpp_instance, cpp_instance_by_name);

    CU_ASSERT_PTR_NOT_NULL(cpp_instance->cpp);
    CU_ASSERT_PTR_NOT_NULL(cpp_instance->cpp->action);
}

CU_TestInfo chglog_postproc_suite[] = {
    UNIT_TEST_INFO(test_create_cpp_instance_fail_cases),
    UNIT_TEST_INFO(test_create_cpp_collapse_module),
    CU_TEST_INFO_NULL
};
