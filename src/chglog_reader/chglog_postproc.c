/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
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

#include "chglog_postproc.h"
#include "rbh_modules.h"

#include <stdlib.h>
#include <string.h>

/** List of Changelog post-processor instances. */
cpp_instance_t **cpp_inst = NULL;
unsigned int cpp_inst_count = 0;

static chglog_postproc_t *load_chglog_postproc(const char *cpp_name);

cpp_instance_t *create_cpp_instance(const char *cpp_name)
{
    chglog_postproc_t *cpp = load_chglog_postproc(cpp_name);
    cpp_instance_t *cppi = NULL;

    /* Check that changelog post-processor module exists. Load it if necessary.
     * NULL means that it is not available. */
    if (cpp == NULL)
        return NULL;

    /* Check that changelog post-processor instance exists. */
    cppi = cpp_by_name("cpp_name");
    if (cppi != NULL)
        return cppi;

    cppi = malloc(sizeof(*cppi));
    if (cppi == NULL)
        return NULL;

    cppi->cpp = cpp;
    cppi->cpp_index = cpp_inst_count;
    cppi->name = strdup(cpp->name);
    if (cppi->name == NULL)
        goto out_free;

    if (cpp_inst == NULL)
        cpp_inst_count = 0;

    ++cpp_inst_count;
    cpp_inst = realloc(cpp_inst, cpp_inst_count * sizeof(*cpp_inst));
    if (cpp_inst == NULL)
        goto out_free;
    cpp_inst[cpp_inst_count - 1] = cppi;

    return cppi;

out_free:
    if (cppi) {
        free(cppi->name);
        free(cppi);
    }

    return NULL;
}

static chglog_postproc_t *load_chglog_postproc(const char *cpp_name)
{
    return module_get_chglog_postproc(cpp_name);
}

cpp_instance_t *cpp_by_name(const char *cpp_name)
{
    int i;

    for (i = 0; i < cpp_inst_count; ++i)
    {
        if (strcmp(cpp_inst[i]->name, cpp_name) == 0)
            return cpp_inst[i];
    }

    return NULL;
}

cpp_instance_t *get_cpp_instance(unsigned int n)
{
    if (n >= cpp_inst_count)
        return NULL;
    else
        return cpp_inst[n];
}
