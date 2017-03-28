/* -*- mode: c; c-basic-offset: 4; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=4:tabstop=4:
 */
/*
 * Copyright (C) 2008, 2009 CEA/DAM
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

#include "global_config.h"
#include "rbh_cfg_helpers.h"
#include "rbh_misc.h"
#include "rbh_logs.h"
#include <errno.h>

#include <limits.h>
#include <stdlib.h>

#define GLOBAL_CONFIG_BLOCK "General"

/* exported variable available to all modules */
global_config_t global_config;

static int read_mount_points(config_item_t config_blck, const char *block_name,
                             char ***mount_points, int *n_mount_points,
                             char *msg_out);
/* name 2 fskey value */
static inline fs_key_t name2fskey(const char *name)
{
    if (!strcasecmp(name, "fsname"))
        return FSKEY_FSNAME;
    else if (!strcasecmp(name, "fsid"))
        return FSKEY_FSID;
    else if (!strcasecmp(name, "devid"))
        return FSKEY_DEVID;
    else
        return FSKEY_ERROR;
}

static void global_cfg_set_default(void *module_config)
{
    global_config_t *conf = (global_config_t *)module_config;

    rh_strncpy(conf->fs_path, "", RBH_PATH_MAX);
#ifdef _LUSTRE
    rh_strncpy(conf->fs_type, "lustre", FILENAME_MAX);
#else
    rh_strncpy(conf->fs_type, "", FILENAME_MAX);
#endif
    conf->fs_mount_points = NULL;
    conf->fs_mpoints_num = 0;
    conf->stay_in_fs = true;
    conf->check_mounted = true;
    conf->last_access_only_atime = false;
    conf->uid_gid_as_numbers = false;
    conf->fs_key = FSKEY_FSNAME;

#if defined(_LUSTRE) && defined(_MDS_STAT_SUPPORT)
    conf->direct_mds_stat = false;
#endif
}

static void global_cfg_write_default(FILE *output)
{
    print_begin_block(output, 0, GLOBAL_CONFIG_BLOCK, NULL);
    print_line(output, 1, "fs_path       :  [MANDATORY]");
#ifdef _LUSTRE
    print_line(output, 1, "fs_type       :  lustre");
#else
    print_line(output, 1, "fs_type       :  [MANDATORY]");
#endif
#ifdef _LUSTRE
    print_line(output, 1, "mountpoint    :  [OPTIONAL, MULTIPLE]");
#else
    print_line(output, 1, "mountpoint    :  [NOT SUPPRTED]");
#endif
    print_line(output, 1, "fs_key        :  fsname");
    print_line(output, 1, "stay_in_fs    :  yes");
    print_line(output, 1, "check_mounted :  yes");
    print_line(output, 1, "last_access_only_atime :  no");
    print_line(output, 1, "uid_gid_as_numbers     :  no");

#if defined(_LUSTRE) && defined(_MDS_STAT_SUPPORT)
    print_line(output, 1, "direct_mds_stat :   no");
#endif
    print_end_block(output, 0);
}

static int global_cfg_read(config_file_t config, void *module_config,
                           char *msg_out)
{
    global_config_t *conf = (global_config_t *)module_config;
    config_item_t    general_block;
    int              rc;

    static const char * const allowed_params[] = {
        "fs_path", "fs_type", "mount_point", "stay_in_fs", "check_mounted",
        "direct_mds_stat", "fs_key", "last_access_only_atime",
        "uid_gid_as_numbers", NULL
    };
    const cfg_param_t cfg_params[] = {
        {"fs_path", PT_STRING, PFLG_MANDATORY | PFLG_ABSOLUTE_PATH |
         PFLG_REMOVE_FINAL_SLASH | PFLG_NO_WILDCARDS, conf->fs_path,
         sizeof(conf->fs_path)}
        ,
        {"fs_type", PT_STRING,
#ifndef _LUSTRE
         PFLG_MANDATORY |
#endif
         PFLG_NO_WILDCARDS, conf->fs_type, sizeof(conf->fs_type)}
        ,
        {"stay_in_fs", PT_BOOL, 0, &conf->stay_in_fs, 0}
        ,
        {"check_mounted", PT_BOOL, 0, &conf->check_mounted, 0}
        ,
        {"last_access_only_atime", PT_BOOL, 0, &conf->last_access_only_atime, 0}
        ,
        {"uid_gid_as_numbers", PT_BOOL, 0, &conf->uid_gid_as_numbers, 0}
        ,
#if defined(_LUSTRE) && defined(_MDS_STAT_SUPPORT)
        {"direct_mds_stat", PT_BOOL, 0, &conf->direct_mds_stat, 0}
        ,
#endif
        END_OF_PARAMS
    };

    /* get GENERAL block */
    rc = get_cfg_block(config, GLOBAL_CONFIG_BLOCK, &general_block, msg_out);
    if (rc)
        return rc;

    /* retrieve std parameters */
    rc = read_scalar_params(general_block, GLOBAL_CONFIG_BLOCK, cfg_params,
                            msg_out);
    if (rc)
        return rc;

#ifdef _LUSTRE
    if (strcmp(conf->fs_type, "lustre")) {
        strcpy(msg_out,
               "This robinhood version has been built for Lustre filesystem support only");
        return EINVAL;
    }
#endif

    /* fs_key param */
    char tmpstr[128];
    rc = GetStringParam(general_block, GLOBAL_CONFIG_BLOCK, "fs_key",
                        PFLG_NO_WILDCARDS, tmpstr, sizeof(tmpstr), NULL, NULL,
                        msg_out);
    if ((rc != 0) && (rc != ENOENT))
        return rc;
    else if (rc == 0) {
        conf->fs_key = name2fskey(tmpstr);
        if (conf->fs_key == FSKEY_ERROR) {
            sprintf(msg_out,
                    "Invalid type for fs_key: '%s' ('fsname', 'devid' or 'fsid' expected)",
                    tmpstr);
            return EINVAL;
        }
    }

#ifdef _LUSTRE
    /* Fill alternative mount points array */
    conf->fs_mount_points = (char**)malloc(sizeof(char*));
    if (conf->fs_mount_points == NULL) {
        sprintf(msg_out, "Not enough memory for mount points array");
        return ENOMEM;
    }
    conf->fs_mount_points[0] = strdup(conf->fs_path);
    if (conf->fs_mount_points[0] == NULL) {
        rc = errno;
        sprintf(msg_out, "Failed to copy main mount point: %s",
                strerror(rc));
        free(conf->fs_mount_points);
        conf->fs_mount_points = NULL;
        return rc;
    }
    conf->fs_mpoints_num = 1;
    rc = read_mount_points(general_block, GLOBAL_CONFIG_BLOCK,
                           &conf->fs_mount_points, &conf->fs_mpoints_num,
                           msg_out);
    /* TODO add result processing */
#endif

    /* check unknown parameters */
    CheckUnknownParameters(general_block, GLOBAL_CONFIG_BLOCK, allowed_params);

    return 0;
}

#define critical_err_check(_ptr_, _blkname_) do { if (!(_ptr_)) {\
          sprintf(msg_out, "Internal error reading %s block in config file",\
                  (_blkname_)); \
          return EFAULT; \
        }\
    } while (0)

static int read_mount_points(config_item_t config_blck, const char *block_name,
                             char ***mount_points, int *n_mount_points,
                             char *msg_out)
{
    char *mount_point_path;
    unsigned int item_index;
    config_item_t curr_item;
    char *item_name;  /* Temporary reference */
    char *item_value; /* Temporary reference */
    int   have_extra_args; /* Temporary value, formal */
    int   rc;
    char **tmp_mount_points;

    for (item_index = 0; item_index < rh_config_GetNbItems(config_blck);
         ++item_index) {

        curr_item = rh_config_GetItemByIndex(config_blck, item_index);
        critical_err_check(curr_item, GLOBAL_CONFIG_BLOCK);

        if (rh_config_ItemType(curr_item) != CONFIG_ITEM_VAR)
            continue;

        rc = rh_config_GetKeyValue(curr_item, &item_name, &item_value,
                                   &have_extra_args);

        if (rc != 0 || strcmp(item_name, "mount_point") != 0)
            continue;

        mount_point_path = realpath(item_value, NULL);
        if (mount_point_path == NULL) {
            rc = errno;
            strcpy(msg_out,
                   "Not enough memory to store absolute path for mount point");
            return rc;
        }
        tmp_mount_points = (char**)realloc(*mount_points, (*n_mount_points + 1) * sizeof(char*));
        if (tmp_mount_points == NULL) {
            rc =errno;
            strcpy(msg_out,
                   "Not enough memory to add absolute path for mount point");
            free(mount_point_path);
            return rc;
        }
        tmp_mount_points[*n_mount_points] = mount_point_path;
        ++*n_mount_points;
        *mount_points = tmp_mount_points;
    }

    return 0;
}

static int global_cfg_set(void *module_config, bool reload)
{
    global_config_t *conf = (global_config_t *) module_config;
    bool             mpoints_differ = false;
    int              mpoint_idx;

    if (!reload) {
        /* copy the whole structure content */
        global_config = *conf;
        return 0;
    }

    if (strcmp(conf->fs_path, global_config.fs_path))
        DisplayLog(LVL_MAJOR, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK
                   "::fs_path changed in config file, but cannot be modified dynamically");
    if (strcmp(conf->fs_type, global_config.fs_type))
        DisplayLog(LVL_MAJOR, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK
                   "::fs_type changed in config file, but cannot be modified dynamically");

    mpoints_differ = (conf->fs_mpoints_num != global_config.fs_mpoints_num);
    for (mpoint_idx = 0; mpoint_idx < conf->fs_mpoints_num && !mpoints_differ;
         ++mpoint_idx) {
        mpoints_differ = (strcmp(conf->fs_mount_points[mpoint_idx],
                                 global_config.fs_mount_points[mpoint_idx]) !=
                          0);
    }
    if (mpoints_differ)
        DisplayLog(LVL_MAJOR, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK
                   "::mount_point list changed in config file, but cannot be modified dynamically");

    if (global_config.stay_in_fs != conf->stay_in_fs) {
        DisplayLog(LVL_EVENT, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK "::stay_in_fs updated: %s->%s",
                   bool2str(global_config.stay_in_fs),
                   bool2str(conf->stay_in_fs));
        global_config.stay_in_fs = conf->stay_in_fs;
    }

    if (global_config.check_mounted != conf->check_mounted) {
        DisplayLog(LVL_EVENT, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK "::check_mounted updated: %s->%s",
                   bool2str(global_config.check_mounted),
                   bool2str(conf->check_mounted));
        global_config.check_mounted = conf->check_mounted;
    }

    if (global_config.last_access_only_atime != conf->last_access_only_atime) {
        DisplayLog(LVL_EVENT, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK
                   "::last_access_only_atime updated: %s->%s",
                   bool2str(global_config.last_access_only_atime),
                   bool2str(conf->last_access_only_atime));
        global_config.last_access_only_atime = conf->last_access_only_atime;
    }

    if (global_config.uid_gid_as_numbers != conf->uid_gid_as_numbers) {
        DisplayLog(LVL_MAJOR, "GlobalConfig",
                   GLOBAL_CONFIG_BLOCK
                   "::uid_gid_as_numbers changed in config file, but cannot be modified dynamically");
    }
    if (global_config.uid_gid_as_numbers)
        DisplayLog(LVL_VERB, "GlobalConfig", "UID and GID stored as numbers");

#if defined(_LUSTRE) && defined(_MDS_STAT_SUPPORT)
    if (conf->direct_mds_stat != global_config.direct_mds_stat) {
        DisplayLog(LVL_EVENT, "FS_Scan_Config",
                   GLOBAL_CONFIG_BLOCK "::direct_mds_stat updated: %u->%u",
                   global_config.direct_mds_stat, conf->direct_mds_stat);
        global_config.direct_mds_stat = conf->direct_mds_stat;
    }
#endif

    return 0;
}

static void global_cfg_write_template(FILE *output)
{
    print_begin_block(output, 0, GLOBAL_CONFIG_BLOCK, NULL);

#ifdef _HAVE_FID
    print_line(output, 1, "# filesystem to be monitored");
    print_line(output, 1, "fs_path = \"/mnt/lustre\" ;");
    fprintf(output, "\n");
#else
    print_line(output, 1, "# filesystem to be monitored");
    print_line(output, 1, "fs_path = \"/tmp\" ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# filesystem type (as returned by 'df' or 'mount' commands)");
    print_line(output, 1, "fs_type = \"ext3\" ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# filesystem property used as FS key: fsname, devid or fsid (fsid NOT recommended)");
    print_line(output, 1, "fs_key = fsname ;");
    fprintf(output, "\n");
#endif
#ifdef _HAVE_FID
    print_line(output, 1, "# alternative mount point");
    print_line(output, 1, "mount_point = \"/mnt/lustre1\" ;");
    print_line(output, 1, "# another alternative mount point");
    print_line(output, 1, "mount_point = \"/mnt/lustre2\" ;");
    print_line(output, 0, "");
#endif
    print_line(output, 1,
               "# check that objects are in the same device as 'fs_path',");
    print_line(output, 1, "# so it will not traverse mount points");
    print_line(output, 1, "stay_in_fs = yes ;");
    fprintf(output, "\n");
    print_line(output, 1, "# check that the filesystem is mounted");
    print_line(output, 1, "check_mounted = yes ;");
    fprintf(output, "\n");
    print_line(output, 1,
               "# Set the last_access time by only the atime variable, and not MAX(atime,mtime)");
    print_line(output, 1,
               "# There are no guarantees that all filesystems will correctly store atime");
    print_line(output, 1, "last_access_only_atime = no ;");
    print_line(output, 1, "uid_gid_as_numbers = no ;");

#if defined(_LUSTRE) && defined(_MDS_STAT_SUPPORT)
    fprintf(output, "\n");
    print_line(output, 1,
               "# File info is asked directly to MDS on Lustre filesystems");
    print_line(output, 1, "# (scan faster, but size information is missing)");
    print_line(output, 1, "direct_mds_stat        =    no ;");
#endif
    print_end_block(output, 0);
}

static void *global_cfg_new(void)
{
    return calloc(1, sizeof(global_config_t));
}

static void global_cfg_free(void *cfg)
{
    if (cfg != NULL)
        free(cfg);
}

/** structure with config handling functions */
mod_cfg_funcs_t global_cfg_hdlr = {
    .module_name = "global",
    .new = global_cfg_new,
    .free = global_cfg_free,
    .set_default = global_cfg_set_default,
    .read = global_cfg_read,
    .set_config = global_cfg_set,
    .write_default = global_cfg_write_default,
    .write_template = global_cfg_write_template
};
