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

#include <stdlib.h>
#include <stdint.h>
#include <syslog.h>

#include "Basic.h"
#include "ut_test_suites.h"

/**
 * Populate internal CUnit DB with test suites.
 */
void add_tests(void);

int main(int argc, char *argv[])
{
    int      rc;
    unsigned n_failures;

    /* Tests setup */
    if (CU_initialize_registry() != CUE_SUCCESS) {
        printf("Failed to initialise Test registry.\n");
        return -1;
    }

    add_tests();
    CU_basic_set_mode(CU_BRM_NORMAL);
    CU_set_error_action(CUEA_IGNORE);
    CU_basic_run_tests();
    n_failures = CU_get_number_of_tests_failed();
    rc = ((n_failures != 0) ? -2 : 0);

    CU_cleanup_registry();

    return rc;
}

void add_tests()
{
    if (CU_register_suites(suites) != CUE_SUCCESS) {
        printf("Suite registration failed: %s\n", CU_get_error_msg());
        exit(-1);
    }

    return;
}
