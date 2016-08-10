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
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>

#include "st_tests.h"

char *dump_file = NULL;
char *config_file_name = NULL;
extern size_t n_test_records;

int cmdline_parse(int argc, char *argv[]);
void usage(char *command);

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

int main(int argc, char *argv[])
{
    int                  rc;
    size_t               i;
    struct st_test_info *test_info = test_infos;
    void                *data;
    struct timespec      start;
    struct timespec      finish;

    if ((rc = cmdline_parse(argc, argv)) != 0)
        return rc;

    if ((rc = suite_setup()) != 0)
        return rc;

    while (test_info->test != NULL) {
        data = NULL;
        if ((rc = test_setup()) != 0)
            return rc;

        if (test_info->generate != NULL && (rc = test_info->generate()) != 0)
                return rc;

        rc = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);
        for (i = 0; i < n_test_records; ++i) {
            if (test_info->get != NULL && (data = test_info->get()) == NULL)
                return ENODATA;
            if ((rc = test_info->test(data, NULL)) != 0)
                return rc;
        }
        rc = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &finish);
        test_info->ts.tv_sec = finish.tv_sec - start.tv_sec;
        test_info->ts.tv_nsec = finish.tv_nsec - start.tv_nsec;
        if (test_info->ts.tv_nsec < 0) {
#define NSEC_PER_SEC 1000000000l
            test_info->ts.tv_nsec = NSEC_PER_SEC + test_info->ts.tv_nsec;
#undef NSEC_PER_SEC
            --test_info->ts.tv_sec;
        }

        if (test_info->free != NULL)
            test_info->free();

        if ((rc = test_teardown()) != 0)
            return rc;
        ++test_info;
    }

    printf("\nPrinting testing statistics:\n");
    for (test_info = test_infos, i = 0; test_info->test != NULL;
         ++test_info, ++i) {
        printf("Test #%li", i + 1);
        if (test_info->test_name != NULL)
            printf(" (%s)", test_info->test_name);
        printf(" took %li sec %li nsec.\n", test_info->ts.tv_sec,
               test_info->ts.tv_nsec);
    }

    return 0;
}
