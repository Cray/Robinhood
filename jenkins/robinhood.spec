#xyr build defines
%define _xyr_package_name     robinhood
%define _xyr_package_source   robinhood.tgz
%define _xyr_package_version  1.0
%define _xyr_build_number     2
%define _xyr_pkg_url          http://es-gerrit:8080/robinhood
%define _xyr_svn_version      0
#xyr end defines

Name:		%_xyr_package_name
Version:	%_xyr_package_version
Release:	%_xyr_build_number
Summary:	Robinhood - Policy engine and accounting tool for large filesystems

Group:		Applications/System
License:	CeCILL-C
URL:		http://seagate.com
Source0:	%_xyr_package_source
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
# You probably want to change this.
BuildArch:	noarch


%description
This is a dummy sample package.  The standard robinhood spec file is of course auto-generated
so it will have to be created and merged.



%prep
# %setup -q


%build
find .

%install

%clean


%files


%pre

if [ "$1" = "1" ] ; then
 # nothing for now
 true
fi

true

%post
# notice "harmless" stuff is not protected by 1=1 so we
# can add things.


true

%changelog
* Thu May 12 2015 David Adair <dadair@freyr.xyus.xyratex.com>
- Dummy example package.


