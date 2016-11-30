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
#include <getopt.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "list_mgr.h"
#include "database.h"

#include "Basic.h"
#include "ut_test_suites.h"

/* For linker to find required global variable. */
mod_cfg_funcs_t fs_scan_cfg_hdlr;
mod_cfg_funcs_t updt_params_hdlr;
mod_cfg_funcs_t policies_cfg_hdlr;
mod_cfg_funcs_t policy_run_cfg_hdlr;
mod_cfg_funcs_t entry_proc_cfg_hdlr;

global_config_t global_config = {
    .fs_path = "/fake/lustre/path"
};

char *dump_file = NULL;
char *config_file_name = NULL;

/** Parse command line and fill test context. */
int cmdline_parse(int argc, char *argv[]);

/** Print usage digest. */
void usage(char *);

/**
 * Populate internal CUnit DB with test suites.
 */
void add_tests(void);

int main(int argc, char *argv[])
{
    int      rc;
    unsigned n_failures;

    if ((rc = cmdline_parse(argc, argv)) != 0)
        return rc;

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

int cmdline_parse(int argc, char *argv[])
{
    char          c;
    struct stat   stat_info;
    int           error;
    struct option longopt[] = {
        {"help", no_argument, NULL, 'h'},
        {"config-file", required_argument, NULL, 'f'},
        {0, 0, 0, 0}
    };

    if (argc <= 1) {
        fprintf(stderr, "No DB dump file name provided.\n");
        return EINVAL;
    }

    optind = 0;
    while ((c = getopt_long(argc, argv, "hf:", longopt, NULL)) !=
           -1) {
        switch (c) {
        case 'h':
            usage(argv[0]);
            exit(0);
        case 'f':
            config_file_name = optarg;
            break;
        default:
            fprintf(stderr, "Error: %s: '%s'\n", strerror(EINVAL),
                    argv[optind - 1]);
            usage(argv[0]);
            return EINVAL;
        }
    }

    if (argc != optind + 1) {
        fprintf(stderr, "One and oly one non-option argument allowed, %i "
                "provided.\n", optind - argc + 1);
        return EINVAL;
    }

    dump_file  = argv[optind];

    if (dump_file == NULL) {
        fprintf(stderr, "Error: no DB dump file provided.\n");
        return ENOENT;
    }

    if (stat(dump_file, &stat_info) == -1) {
        error = errno;
        fprintf(stderr, "Failed to stat(2) DB dump file '%s': %s\n", dump_file,
                strerror(error));
        return error;
    }

    if (!S_ISREG(stat_info.st_mode)) {
        fprintf(stderr, "Error: DB dump file '%s' is not a regular file.\n",
                dump_file);
        return EINVAL;
    }

    return 0;
}

void usage(char *command)
{
    printf(
"Usage:\n"
"%1$s --help\n"
"\tPrint this help message.\n"
"%1$s [OPTIONS] </path/to/dump/file>\n"
"\t Run database-enabled List Manager tests.\n"
"Options:\n"
"\t-f, --config-file </path/to/file>\n"
"\t\tPath to configuration file corresponding to database dump.\n"
"\t</path/to/dump/file>\n"
"\t\tTest DB dump file.\n",
    basename(command));
}
