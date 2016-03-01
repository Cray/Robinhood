umask 022

# Select mock config for the kernel we are building with
PROJECT=${JP_NEO_RELEASE}
MOCK_CONFIG=mock_${PROJECT}
SCM_URL=${JP_SCM_URL:-http://seagate.com}
VERSION=${JP_VERSION:-3.0}
NEO_ID=${JP_NEO_ID:-o.1.0}
SOURCE=${REPO}-${VERSION}.tar.gz
_PWD=$(pwd)
WORKSPACE=${WORKSPACE:-$_PWD}
BUILD_NUMBER=${BUILD_NUMBER:-3}

if [ -x /build/bin/spec_update.sh ] ; then
  SPECUPDATE=/build/bin/spec_update.sh
else
  SPECUPDATE=${WORKSPACE}/jenkins/spec_update.sh
fi

RPMVER=${VERSION}.${NEO_ID}
if [ -d ${WORKSPACE}/.git ] ; then
    SCM_VER=$(git rev-list --max-count=1 HEAD | cut -c1-6)
fi
if [ -d ${WORKSPACE}/.svn ] ; then
    SCM_VER=$(svn info ${repo_dir} 2>/dev/null | grep "Revision:" | cut -f2 -d" ")
fi

if [ -n "${SCM_VER}" ] ; then
   RPMREL=${BUILD_NUMBER}.${SCM_VER}
else
   RPMREL=${BUILD_NUMBER}
fi


# Create output directory
RPMDIR=${WORKSPACE}/RPMBUILD
rm -rf ${RPMDIR}
mkdir -p ${RPMDIR}/SOURCE
mkdir -p ${RPMDIR}/SPECS

# Create tarball.  We will share one for all spec files.
cd ./$(git rev-parse --show-cdup) &&
  git archive --format=tar --prefix=${JP_REPO}/ HEAD . | gzip > RPMBUILD/SOURCE/${SOURCE}

# Initialize chroot.
mock -r ${MOCK_CONFIG} --resultdir ${RPMDIR} --init

# Create versioned spec file and rebuild package for each spec we find.
ls jenkins/*.spec | while read SPECFILE; do
  PACKAGE=$(basename ${SPECFILE})
  PACKAGE=${PACKAGE%%.spec}

  # spec_update expects this name
  ln -s ${SOURCE} RPMBUILD/SOURCE/${PACKAGE}.tar.gz

  sh -x ${SPECUPDATE} ${PACKAGE} ${RPMVER} ${SCM_URL} ${WORKSPACE} ${RPMDIR} ${RPMREL} ${WORKSPACE}/${SPECFILE}

  mock --buildsrpm -r ${MOCK_CONFIG} --spec ${RPMDIR}/SPECS/${PACKAGE}.spec --sources ${RPMDIR}/SOURCE --resultdir ${RPMDIR} --no-clean --no-cleanup-after -v

  mock --rebuild -r ${MOCK_CONFIG} --no-clean --no-cleanup-after -v --rebuild ${RPMDIR}/${PACKAGE}*.src.rpm --resultdir ${RPMDIR}

done


echo "Complete Build"
