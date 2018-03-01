/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __HIERARCHICAL_ALLOCATOR_PROCESS_HPP__
#define __HIERARCHICAL_ALLOCATOR_PROCESS_HPP__

#include <math.h>
#include <vector>
#include <algorithm>

#include <process/delay.hpp>
#include <process/timeout.hpp>

#include <stout/duration.hpp>
#include <stout/hashmap.hpp>
#include <stout/stopwatch.hpp>

#include "common/resources.hpp"

#include "master/allocator.hpp"
#include "master/master.hpp"

namespace avalon {
namespace internal {
namespace master {


class HierarchicalAllocatorProcess : public AllocatorProcess
{
public:
  HierarchicalAllocatorProcess();

  virtual ~HierarchicalAllocatorProcess();

  process::PID<HierarchicalAllocatorProcess> self();

  void initialize(
      const Flags& flags,
      const process::PID<Master>& _master);

  void frameworkAdded(
      const FrameworkID& frameworkId,
      const FrameworkInfo& frameworkInfo,
      const Resources& used);

  void frameworkRemoved(
      const FrameworkID& frameworkId);

  void slaveAdded(
      const SlaveID& slaveId,
      const SlaveInfo& slaveInfo);

  void slaveRemoved(
      const SlaveID& slaveId);

  void resourcesUnused(
      const FrameworkID& frameworkId,
      const SlaveID& slaveId,
      const Resources& resources);

  void resourcesRecovered(
      const FrameworkID& frameworkId,
      const SlaveID& slaveId,
      const Resources& resources);


protected:
  // Useful typedefs for dispatch/delay/defer to self()/this.
  typedef HierarchicalAllocatorProcess Self;
  typedef HierarchicalAllocatorProcess This;

  // Callback for doing batch allocations.
  void batch();

  // Allocate any allocatable resources.
  void allocate();

  // Allocate resources just from the specified slave.
  void allocate(const SlaveID& slaveId);

  // Allocate resources from the specified slaves.
  void allocate(const hashset<SlaveID>& slaveIds);

  hashmap<SlaveID, Resources> filterByPosition(const FrameworkID& frameworkId,
                      const hashmap<SlaveID, Resources>& available);

  bool initialized;

  Flags flags;
  PID<Master> master;

  // Maps slaves to their allocatable resources.
  hashmap<SlaveID, Resources> allocatable_cres;
  hashmap<SlaveID, Resources> allocatable_dres;
  hashmap<SlaveID, Resources> allocatable_ares;

  hashmap<SlaveID, Position> robotsPosition;
  
  hashmap<FrameworkID, hashmap<SlaveID, double> > scores;

  hashmap<FrameworkID, FrameworkInfo> frameworks;

  // Contains all active slaves.
  hashmap<SlaveID, SlaveInfo> slaves;
};


HierarchicalAllocatorProcess::HierarchicalAllocatorProcess()
  : initialized(false) {}


HierarchicalAllocatorProcess::~HierarchicalAllocatorProcess()
{}


process::PID<HierarchicalAllocatorProcess>
HierarchicalAllocatorProcess::self()
{
  return
    process::PID<HierarchicalAllocatorProcess>(this);
}


void
HierarchicalAllocatorProcess::initialize(
    const Flags& _flags,
    const process::PID<Master>& _master)
{
  flags = _flags;
  master = _master;
  initialized = true;

  VLOG(1) << "Initializing hierarchical allocator process "
          << "with master : " << master;

  delay(flags.allocation_interval, self(), &Self::batch);
}


void
HierarchicalAllocatorProcess::frameworkAdded(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const Resources& used)
{
  CHECK(initialized);

  LOG(INFO) << "Added framework " << frameworkId;

  frameworks[frameworkId] = frameworkInfo;

  allocate();
}


void
HierarchicalAllocatorProcess::frameworkRemoved(
    const FrameworkID& frameworkId)
{
  CHECK(initialized);

  LOG(INFO) << "Removed framework " << frameworkId;

  frameworks.erase(frameworkId);
}


void
HierarchicalAllocatorProcess::slaveAdded(
    const SlaveID& slaveId,
    const SlaveInfo& slaveInfo)
{
  CHECK(initialized);

  CHECK(!slaves.contains(slaveId));

  slaves[slaveId] = slaveInfo;

  // userSorter->add(slaveInfo.resources());

  Resources unused_cres = slaveInfo.cres();
  Resources unused_dres = slaveInfo.dres();
  Resources unused_ares = slaveInfo.ares();
  Position position = slaveInfo.pose().position();

  allocatable_cres[slaveId] = unused_cres;
  allocatable_dres[slaveId] = unused_dres;
  allocatable_ares[slaveId] = unused_ares;
  robotsPosition[slaveId] = position;

  LOG(INFO) << "Added slave " << slaveId << " (" << slaveInfo.hostname()
            << ") available) in " << "[" << position.x() << ", " << 
            ", " << position.y() << ", " << position.z() << "]";
  LOG(INFO)<< "cRes: " << slaveInfo.cres();
  LOG(INFO)<< "dRes: " << slaveInfo.dres();
  LOG(INFO)<< "aRes: " << slaveInfo.ares();

  allocate(slaveId);
}


void
HierarchicalAllocatorProcess::slaveRemoved(
    const SlaveID& slaveId)
{
  CHECK(initialized);

  CHECK(slaves.contains(slaveId));


  slaves.erase(slaveId);

  allocatable_ares.erase(slaveId);
  allocatable_dres.erase(slaveId);
  allocatable_ares.erase(slaveId);
  robotsPosition.erase(slaveId);

  // Note that we DO NOT actually delete any filters associated with
  // this slave, that will occur when the delayed
  // HierarchicalAllocatorProcess::expire gets invoked (or the framework
  // that applied the filters gets removed).

  LOG(INFO) << "Removed slave " << slaveId;
}


void
HierarchicalAllocatorProcess::batch()
{
  CHECK(initialized);
  allocate();
  delay(flags.allocation_interval, self(), &Self::batch);
}


void
HierarchicalAllocatorProcess::allocate()
{
  CHECK(initialized);

  Stopwatch stopwatch;
  stopwatch.start();

  allocate(slaves.keys());

  LOG(INFO) << "Performed allocation for " << slaves.size() << " slaves in "
            << stopwatch.elapsed();
}


void
HierarchicalAllocatorProcess::allocate(
    const SlaveID& slaveId)
{
  CHECK(initialized);

  hashset<SlaveID> slaveIds;
  slaveIds.insert(slaveId);

  Stopwatch stopwatch;
  stopwatch.start();

  allocate(slaveIds);

  LOG(INFO) << "Performed allocation for slave " << slaveId << " in "
            << stopwatch.elapsed();
}


hashmap<SlaveID, Resources>
HierarchicalAllocatorProcess::filterByPosition(
    const FrameworkID& frameworkId,
    const hashmap<SlaveID, Resources>& available)
{
  FrameworkInfo frameworkInfo = frameworks[frameworkId];
  Position fwPosition = frameworkInfo.position();
  hashmap<SlaveID, Resources> offerable;
  uint32_t range = frameworkInfo.range();

  foreachpair (const SlaveID& slaveId, Position rtPosition, robotsPosition) {
    double score = sqrt(pow(rtPosition.x()-fwPosition.x(), 2) + 
                        pow(rtPosition.y()-fwPosition.y(), 2));
    scores[frameworkId][slaveId] = score;
    LOG(INFO) << "Slave " << slaveId << " in Framework " 
          << frameworkId << " score: " << score << " Filter range: " << range;
  }

  foreachkey(const SlaveID& slaveId, scores[frameworkId]) {
    const Resources& cres = allocatable_cres[slaveId];
    const Resources& dres = allocatable_dres[slaveId];
    const Resources& ares = allocatable_ares[slaveId];
    if(scores[frameworkId][slaveId] < range){
      offerable[slaveId] += cres;
      offerable[slaveId] += dres;
      offerable[slaveId] += ares;
      allocatable_cres[slaveId] -= cres;
      allocatable_dres[slaveId] -= dres;
      allocatable_ares[slaveId] -= ares;
    }
  }

  return offerable;
}


void
HierarchicalAllocatorProcess::allocate(
    const hashset<SlaveID>& slaveIds)
{
  CHECK(initialized);

  // // Get out only "available" resources (i.e., resources that are
  // // allocatable and above a certain threshold, see below).
  hashmap<SlaveID, Resources> available_cres;
  hashmap<SlaveID, Resources> available_dres;
  hashmap<SlaveID, Resources> available_ares;

  foreachpair (const SlaveID& slaveId, Resources resources, allocatable_cres) {
    if (!slaveIds.contains(slaveId)) {
      continue;
    }

    resources = resources.allocatable(); // Make sure they're allocatable.

    // TODO(benh): For now, only make offers when there is some cpu
    // and memory left. This is an artifact of the original code that
    // only offered when there was at least 1 cpu "unit" available,
    // and without doing this a framework might get offered resources
    // with only memory available (which it obviously will decline)
    // and then end up waiting the default Filters::refuse_seconds
    // (unless the framework set it to something different).

    Value::Scalar none;
    Value::Scalar cpus = resources.get("cpus", none);
    Value::Scalar mem = resources.get("mem", none);

    if (cpus.value() >= MIN_CPUS && mem.value() > MIN_MEM) {
      VLOG(1) << "Found available resources: " << resources
                << " on slave " << slaveId;
      available_cres[slaveId] = resources;
    }
  }

  foreachpair (const SlaveID& slaveId, Resources resources, allocatable_dres) {
    if (!slaveIds.contains(slaveId)) {
      continue;
    }

    resources = resources.allocatable(); // Make sure they're allocatable.
    available_dres[slaveId] = resources;
  }

  foreachpair (const SlaveID& slaveId, Resources resources, allocatable_ares) {
    if (!slaveIds.contains(slaveId)) {
      continue;
    }

    resources = resources.allocatable(); // Make sure they're allocatable.
    available_ares[slaveId] = resources;
  }

  if (allocatable_cres.size() == 0 && 
      allocatable_dres.size() == 0 && 
      allocatable_ares.size() == 0) {
    VLOG(1) << "No resources available to allocate!";
    return;
  }

  foreachkey (const FrameworkID& frameworkId, frameworks) {
    hashmap<SlaveID, Resources> offerable = 
          filterByPosition(frameworkId, available_dres);


    if (offerable.size() > 0) {
      // foreachkey (const SlaveID& slaveId, offerable) {
      //   // available.erase(slaveId);
      //   LOG(INFO) << "Erase order: " << slaveId;
      // }

      dispatch(master, &Master::offer, frameworkId, offerable, scores);
    }
  }
}


void
HierarchicalAllocatorProcess::resourcesUnused(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& resources)
{
  CHECK(initialized);

  if (resources.allocatable().size() == 0) {
    return;
  }

  VLOG(1) << "Framework " << frameworkId
          << " left " << resources.allocatable()
          << " unused on slave " << slaveId;

  // Update resources allocatable on slave.
  CHECK(allocatable_cres.contains(slaveId));
  allocatable_cres[slaveId] += resources;
}


void
HierarchicalAllocatorProcess::resourcesRecovered(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& resources)
{
  CHECK(initialized);

  if (resources.allocatable().size() == 0) {
    return;
  }

  // Updated resources allocated to framework (if framework still
  // exists, which it might not in the event that we dispatched
  // Master::offer before we received AllocatorProcess::frameworkRemoved
  // or AllocatorProcess::frameworkDeactivated, in which case we will
  // have already recovered all of its resources).
  // if (users.contains(frameworkId) &&
  //     sorters[users[frameworkId]]->contains(frameworkId.value())) {
  //   std::string user = users[frameworkId];
  //   sorters[user]->unallocated(frameworkId.value(), resources);
  //   sorters[user]->remove(resources);
  //   userSorter->unallocated(user, resources);
  // }

  // Update resources allocatable on slave (if slave still exists,
  // which it might not in the event that we dispatched Master::offer
  // before we received Allocator::slaveRemoved).
  if (allocatable_cres.contains(slaveId)) {
    allocatable_cres[slaveId] += resources;

    LOG(INFO) << "Recovered " << resources.allocatable()
              << " (total allocatable_cres: " << allocatable_cres[slaveId] << ")"
              << " on slave " << slaveId
              << " from framework " << frameworkId;
  }
}


} // namespace master {
} // namespace internal {
} // namespace avalon {

#endif // __HIERARCHICAL_ALLOCATOR_PROCESS_HPP__
