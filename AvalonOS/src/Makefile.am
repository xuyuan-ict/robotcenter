# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

# Add 'subdir-objects' to build objects in respective subdirectories
# which is necessary for dealing with naming conflicts, i.e,,
# master/http.cpp and slave/http.cpp.
AUTOMAKE_OPTIONS = subdir-objects

include ../third_party/versions.am

# TODO(charles): Move these into an included automakefile and have
# them include $(top_builddir) as appropriate.
BOOST = third_party/boost-$(BOOST_VERSION)
DISTRIBUTE = third_party/distribute-$(DISTRIBUTE_VERSION)
PROTOBUF = third_party/protobuf-$(PROTOBUF_VERSION)
GLOG = third_party/glog-$(GLOG_VERSION)
GMOCK = third_party/gmock-$(GMOCK_VERSION)
GTEST = $(GMOCK)/gtest
LEVELDB = third_party/leveldb
ZOOKEEPER = third_party/zookeeper-$(ZOOKEEPER_VERSION)/src/c
LIBPROCESS = third_party/libprocess

# Unfortunatley, 'pkglibexecdir' and 'pkglocalstatedir' are not set
# before automake 1.11, so we need to set them manually (until we in
# the future assume an automake version).
pkglibexecdir = $(libexecdir)/$(PACKAGE)
pkglocalstatedir = $(localstatedir)/$(PACKAGE)

# Protocol buffer compiler.
PROTOC = ../$(PROTOBUF)/src/protoc
PROTOCFLAGS = -I$(top_srcdir)/include/avalon -I$(srcdir)

# Initialize variables here so we can use += operator everywhere else.
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
sbin_PROGRAMS =
bin_PROGRAMS =
pkglibexec_PROGRAMS =
dist_pkglibexec_SCRIPTS =
nobase_dist_pkgdata_DATA =
nodist_sbin_SCRIPTS =
nobase_pkglocalstate_DATA =
check_PROGRAMS =
dist_check_SCRIPTS =
check_SCRIPTS =
BUILT_SOURCES =
CLEANFILES =
EXTRA_DIST =
PHONY_TARGETS =

# Add compiler and linker flags for pthreads.
AM_CXXFLAGS = $(PTHREAD_CFLAGS)
AM_LIBS = $(PTHREAD_LIBS)

# Setup CPPFLAGS that are used for most source files.
AVALON_CPPFLAGS = $(AM_CPPFLAGS)
AVALON_CPPFLAGS += -Wall 
AVALON_CPPFLAGS += -DLIBDIR=\"$(libdir)\"
AVALON_CPPFLAGS += -DPKGLIBEXECDIR=\"$(pkglibexecdir)\"
AVALON_CPPFLAGS += -DPKGDATADIR=\"$(pkgdatadir)\"
AVALON_CPPFLAGS += -I$(top_srcdir)/include
AVALON_CPPFLAGS += -I$(top_srcdir)/$(LIBPROCESS)/include
AVALON_CPPFLAGS += -I../include
AVALON_CPPFLAGS += -I../$(BOOST)
AVALON_CPPFLAGS += -I../$(PROTOBUF)/src
AVALON_CPPFLAGS += -I../$(GLOG)/src



# README: we build the Avalon library out of a collection of
# convenience libraries (that is, libraries that do not get installed
# but we can use as building blocks to vary compile flags as necessary
# and then aggregate into final archives): libavalon_no_third_party.la
# libbuild.la, liblog.la, libjava.la.

# First, let's define necessary protocol buffer files.

AVALON_PROTO = $(top_srcdir)/include/avalon/avalon.proto

CXX_PROTOS = avalon.pb.cc avalon.pb.h

MESSAGES_PROTOS = messages/messages.pb.cc messages/messages.pb.h

BUILT_SOURCES += $(MESSAGES_PROTOS)
CLEANFILES += $(MESSAGES_PROTOS)

LOG_PROTOS = messages/log.pb.cc messages/log.pb.h

BUILT_SOURCES += $(LOG_PROTOS)
CLEANFILES += $(LOG_PROTOS)

STATE_PROTOS = messages/state.pb.cc messages/state.pb.h

BUILT_SOURCES += $(STATE_PROTOS)
CLEANFILES += $(STATE_PROTOS)


# Targets for generating protocol buffer code.
%.pb.cc %.pb.h: $(top_srcdir)/include/avalon/%.proto
	@$(MKDIR_P) $(@D)
	@$(PROTOC) $(PROTOCFLAGS) --cpp_out=. $^

%.pb.cc %.pb.h: %.proto
	@$(MKDIR_P) $(@D)
	@$(PROTOC) $(PROTOCFLAGS) --cpp_out=. $^


# We even use a convenience library for most of Avalon so that we can
# exclude third party libraries so setuptools/distribute can build a
# self-contained Python library and statically link in the third party
# libraries themselves.
noinst_LTLIBRARIES += libavalon_no_third_party.la

nodist_libavalon_no_third_party_la_SOURCES = $(CXX_PROTOS) $(MESSAGES_PROTOS)

libavalon_no_third_party_la_SOURCES =					\
	sched/sched.cpp							\
	master/constants.cpp					\
	master/master.cpp						\
	slave/constants.cpp						\
	slave/gc.cpp							\
	slave/slave.cpp							\
	slave/isolation_module.cpp				\
	slave/process_based_isolation_module.cpp			\
	slave/reaper.cpp						\
	launcher/launcher.cpp					\
	exec/exec.cpp							\
	common/lock.cpp							\
	detector/detector.cpp 					\
	configurator/configurator.cpp			\
	common/date_utils.cpp					\
	common/resources.cpp					\
	common/attributes.cpp					\
	common/values.cpp						\
	files/files.cpp							\
	logging/logging.cpp						\
	messages/messages.proto

pkginclude_HEADERS = $(top_srcdir)/include/avalon/scheduler.hpp	\
					 $(top_srcdir)/include/avalon/executor.hpp	\
		     		 $(top_srcdir)/include/avalon/avalon.proto

nodist_pkginclude_HEADERS = ../include/avalon/avalon.hpp avalon.pb.h

if OS_LINUX
  libavalon_no_third_party_la_SOURCES += slave/cgroups_isolation_module.cpp
  libavalon_no_third_party_la_SOURCES += slave/lxc_isolation_module.cpp
  libavalon_no_third_party_la_SOURCES += linux/cgroups.cpp
  libavalon_no_third_party_la_SOURCES += linux/fs.cpp
  libavalon_no_third_party_la_SOURCES += linux/proc.cpp
else
  EXTRA_DIST += slave/cgroups_isolation_module.cpp
  EXTRA_DIST += slave/lxc_isolation_module.cpp
  EXTRA_DIST += linux/cgroups.cpp
  EXTRA_DIST += linux/fs.cpp
  EXTRA_DIST += linux/proc.cpp
endif

libavalon_no_third_party_la_SOURCES += common/attributes.hpp			\
	common/build.hpp common/date_utils.hpp common/factory.hpp			\
	common/protobuf_utils.hpp 											\
	common/lock.hpp common/resources.hpp common/process_utils.hpp		\
	common/type_utils.hpp common/thread.hpp common/units.hpp			\
	common/values.hpp common/robot.hpp launcher/launcher.hpp			\
	configurator/configurator.hpp configurator/option.hpp				\
	detector/detector.hpp 	files/files.hpp								\
	flags/flag.hpp flags/flags.hpp flags/loader.hpp						\
	flags/parse.hpp configurator/configuration.hpp						\
	linux/cgroups.hpp linux/fs.hpp linux/proc.hpp						\
	logging/check_some.hpp logging/flags.hpp logging/logging.hpp		\
	master/allocator.hpp												\
	master/constants.hpp master/flags.hpp								\
	master/master.hpp master/slaves_manager.hpp 						\
	master/hierarchical_allocator_process.hpp							\
	messages/messages.hpp slave/constants.hpp							\
	slave/isolation_module.hpp slave/isolation_module_factory.hpp		\
	slave/process_based_isolation_module.hpp slave/reaper.hpp			\
	slave/cgroups_isolation_module.hpp slave/lxc_isolation_module.hpp	\
	slave/flags.hpp slave/gc.hpp 										\
	slave/paths.hpp	slave/slave.hpp		


libavalon_no_third_party_la_CPPFLAGS = $(AVALON_CPPFLAGS)

libavalon_no_third_party_la_LIBADD = # Initialized to allow += below.

# Convenience library that *always* gets rebuilt to ensure accurate info.
noinst_LTLIBRARIES += libbuild.la
libbuild_la_SOURCES = common/build.cpp
libbuild_la_CPPFLAGS = $(AM_CPPFLAGS)
libbuild_la_CPPFLAGS += -DBUILD_DATE="\"$$(date '+%Y-%m-%d %H:%M:%S')\""
libbuild_la_CPPFLAGS += -DBUILD_TIME="\"$$(date '+%s')\""
libbuild_la_CPPFLAGS += -DBUILD_USER="\"$$USER\""

# We need to escape the build flags properly.
BUILD_FLAGS = $(echo $(AVALON_CPPFLAGS) $(CPPFLAGS) | sed 's/\"/\\\"/g') \
              $(echo $(AM_CFLAGS) $(CFLAGS) | sed 's/\"/\\\"/g') \
              $(echo $(AM_CXXFLAGS) $(CXXFLAGS) | sed 's/\"/\\\"/g')

# TODO(benh): Provide other build flags.

libbuild_la_CPPFLAGS += -DBUILD_FLAGS="\"$$BUILD_FLAGS\""

libavalon_no_third_party_la_LIBADD += libbuild.la


# Convenience library for building the replicated log in order to
# include the leveldb headers.
# noinst_LTLIBRARIES += liblog.la
# liblog_la_SOURCES = log/coordinator.cpp log/replica.cpp
# liblog_la_SOURCES += log/coordinator.hpp log/replica.hpp log/log.hpp	\
#   log/network.hpp messages/log.hpp messages/log.proto
# nodist_liblog_la_SOURCES = $(LOG_PROTOS)
# liblog_la_CPPFLAGS = -I../$(LEVELDB)/include $(AVALON_CPPFLAGS)

# libavalon_no_third_party_la_LIBADD += liblog.la


# The final result!
lib_LTLIBRARIES += libavalon.la

libavalon_la_SOURCES = $(AVALON_PROTO) # Part of the distribution.

libavalon_la_LDFLAGS = -release $(PACKAGE_VERSION) -shared

# Since we just include the convenience library (and no sources), we
# need to tell libtool to build this as a C++ library.
libavalon_la_LIBTOOLFLAGS = --tag=CXX

# Add the convenience library.
libavalon_la_LIBADD = libavalon_no_third_party.la

# For non-convenience libraries we need to link them in to make the shared
# library each time. (Currently, we don't support platforms where this is not
# possible.)
libavalon_la_LIBADD += ../$(PROTOBUF)/src/libprotobuf.la
libavalon_la_LIBADD += ../$(GLOG)/libglog.la

# We need to directly include the leveldb library in order to avoid
# the installed libavalon.la file to include leveldb in
# 'dependency_libs' (via '-L../third_party/leveldb -lleveldb').
libavalon_la_LIBADD += ../$(LEVELDB)/libleveldb.a


libavalon_la_LIBADD += ../$(LIBPROCESS)/libprocess.la


# Binaries.
sbin_PROGRAMS += avalon-master
avalon_master_SOURCES = master/main.cpp
avalon_master_CPPFLAGS = $(AVALON_CPPFLAGS)
avalon_master_LDADD = libavalon.la

sbin_PROGRAMS += avalon-slave
avalon_slave_SOURCES = slave/main.cpp
avalon_slave_CPPFLAGS = $(AVALON_CPPFLAGS)
avalon_slave_LDADD = libavalon.la

pkglibexec_PROGRAMS += avalon-launcher
avalon_launcher_SOURCES = launcher/main.cpp
avalon_launcher_CPPFLAGS = $(AVALON_CPPFLAGS)
avalon_launcher_LDADD = libavalon.la

pkglibexec_PROGRAMS += avalon-executor
avalon_executor_SOURCES = launcher/executor.cpp
avalon_executor_CPPFLAGS = $(AVALON_CPPFLAGS)
avalon_executor_LDADD = libavalon.la

bin_PROGRAMS += avalon-avalon
avalon_avalon_SOURCES = avalon/main.cpp
avalon_avalon_CPPFLAGS = $(AVALON_CPPFLAGS)
avalon_avalon_LDADD = libavalon.la

check_PROGRAMS += test-framework
test_framework_SOURCES = examples/test_framework.cpp
test_framework_CPPFLAGS = $(AVALON_CPPFLAGS)
test_framework_LDADD = libavalon.la

check_PROGRAMS += test-executor
test_executor_SOURCES = examples/test_executor.cpp
test_executor_CPPFLAGS = $(AVALON_CPPFLAGS)
test_executor_LDADD = libavalon.la

# check_PROGRAMS += avalon-tests

# We use a check-local target for now to avoid the parallel test
# runner that ships with newer versions of autotools.
# See the following discussion for the workaround:
# http://lists.gnu.org/archive/html/automake/2013-01/msg00051.html
# check-local: avalon-tests
# 	./avalon-tests

# clean-local: clean-java clean-python

.PHONY: $(PHONY_TARGETS)
