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

#include <lustre/lustre_user.h>
#include <lustre/lustreapi.h>
#include <errno.h>

#include "lustre_extended_types.h"

void llapi_msg_set_level(int);
int llapi_changelog_fini(void**);
int llapi_changelog_clear(const char*, const char*, long long);
int llapi_changelog_free(CL_REC_TYPE**);
int llapi_changelog_recv(void*, CL_REC_TYPE**);
/*
 * NOTE! CLF_VERSION was move to 'enum changelog_send_flag' when
 * 'enum changelog_send_flag' was added to project
*/
#ifndef CLF_VERSION
int llapi_changelog_start(void**, enum changelog_send_flag, const char*,
              long long);
#else
int llapi_changelog_start(void**, int, const char*, long long);
#endif
llapi_log_callback_t llapi_error_callback_set(llapi_log_callback_t);
llapi_log_callback_t llapi_info_callback_set(llapi_log_callback_t);
int llapi_file_get_stripe(const char*, struct lov_user_md*);
int llapi_file_create_pool(const char*, unsigned long long, int, int, int,
               char*);
int llapi_file_create(const char*, unsigned long long, int, int, int);
int llapi_fid2path(const char*, const char*, char*, int, long long*, int*);
int llapi_path2fid(const char*, lustre_fid*);
int llapi_fd2fid(const int, lustre_fid*);
int llapi_obd_statfs(char*, __u32, __u32, struct obd_statfs*, struct obd_uuid*);
int llapi_get_poolmembers(const char*, char**, int, char*, int);

void llapi_msg_set_level(int level)
{
    (void)level;
    return;
}

int llapi_changelog_fini(void **priv)
{
    (void)priv;
    return -ENOSYS;
}

int llapi_changelog_clear(const char *mdtname, const char *idstr,
                          long long endrec)
{
    (void)mdtname;
    (void)idstr;
    (void)endrec;
    return -ENOSYS;
}

int llapi_changelog_free(CL_REC_TYPE **rech)
{
    (void)rech;
    return -ENOSYS;
}

int llapi_changelog_recv(void *priv, CL_REC_TYPE **rech)
{
    (void)priv;
    (void)rech;
    return -ENOSYS;
}

#ifndef CLF_VERSION
int llapi_changelog_start(void **priv, enum changelog_send_flag flags,
              const char *device, long long startrec)
#else
int llapi_changelog_start(void **priv, int flags, const char *device,
              long long startrec)
#endif
{
    (void)priv;
    (void)flags;
    (void)device;
    (void)startrec;
    return -ENOSYS;
}

llapi_log_callback_t llapi_error_callback_set(llapi_log_callback_t cb)
{
    (void)cb;
    return NULL;
}

llapi_log_callback_t llapi_info_callback_set(llapi_log_callback_t cb)
{
    (void)cb;
    return NULL;
}

int llapi_file_get_stripe(const char *path, struct lov_user_md *lum)
{
    (void)path;
    (void)lum;
    return -ENOSYS;
}

int llapi_file_create_pool(const char *name, unsigned long long stripe_size,
                           int stripe_offset, int stripe_count,
                           int stripe_pattern, char *pool_name)
{
    (void)name;
    (void)stripe_size;
    (void)stripe_offset;
    (void)stripe_count;
    (void)stripe_pattern;
    (void)pool_name;
    return -ENOSYS;
}

int llapi_file_create(const char *name, unsigned long long stripe_size,
                      int stripe_offset, int stripe_count, int stripe_pattern)
{
    (void)name;
    (void)stripe_size;
    (void)stripe_offset;
    (void)stripe_count;
    (void)stripe_pattern;
    return -ENOSYS;
}

int llapi_fid2path(const char *device, const char *fidstr, char *buf,
           int buflen, long long *recno, int *linkno)
{
    (void)device;
    (void)fidstr;
    (void)buf;
    (void)buflen;
    (void)recno;
    (void)linkno;
    return -ENOSYS;
}

int llapi_path2fid(const char *path, lustre_fid *fid)
{
    (void)path;
    (void)fid;
    return -ENOSYS;
}

int llapi_fd2fid(const int fd, lustre_fid *fid)
{
    (void)fd;
    (void)fid;
    return -ENOSYS;
}

int llapi_obd_statfs(char *path, __u32 type, __u32 index,
                     struct obd_statfs *stat_buf,
                     struct obd_uuid *uuid_buf)
{
    (void)path;
    (void)type;
    (void)index;
    (void)stat_buf;
    (void)uuid_buf;
    return -ENOSYS;
}

int llapi_get_poolmembers(const char *poolname, char **members,
                          int list_size, char *buffer, int buffer_size)
{
    (void)poolname;
    (void)members;
    (void)list_size;
    (void)buffer;
    (void)buffer_size;
    return -ENOSYS;
}
