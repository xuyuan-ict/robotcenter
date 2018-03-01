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

#ifndef __MASTER_HPP__
#define __MASTER_HPP__

#include <list>
#include <string>
#include <vector>

#include <tr1/functional>

#include <boost/circular_buffer.hpp>

#include <process/http.hpp>
#include <process/process.hpp>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
#include <process/protobuf.hpp>

#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/hashset.hpp>
#include <stout/multihashmap.hpp>
#include <stout/option.hpp>

#include "common/resources.hpp"
#include "common/type_utils.hpp"
#include "common/units.hpp"

#include "flags/flags.hpp"

#include "logging/flags.hpp"

#include "files/files.hpp"

#include "master/constants.hpp"
#include "master/flags.hpp"
// #include "master/http.hpp"

#include "messages/messages.hpp"

namespace avalon {
namespace internal {
namespace master {

using namespace process; // Included to make code easier to read.

// Forward declarations.
class Allocator;
class SlavesManager;
class SlaveObserver;

struct Framework;
struct Slave;


class Master : public ProtobufProcess<Master>
{
public:
  Master(Allocator* allocator, Files* files);
  Master(Allocator* allocator,
         Files* files,
         const flags::Flags<logging::Flags, master::Flags>& flags);

  virtual ~Master();

  void newMasterDetected(const UPID& pid);
  void noMasterDetected();
  void registerFramework(const FrameworkInfo& frameworkInfo);
  void unregisterFramework(const FrameworkID& frameworkId);
  void registerSlave(const SlaveInfo& slaveInfo);
  void unregisterSlave(const SlaveID& slaveId);
  void launchTasks(const FrameworkID& frameworkId,
                   const OfferID& offerId,
                   const std::vector<TaskInfo>& tasks,
                   const Filters& filters);
  void statusUpdate(const StatusUpdate& update, const UPID& pid);
  void activatedSlaveHostnamePort(const std::string& hostname, uint16_t port);
  void deactivatedSlaveHostnamePort(const std::string& hostname, uint16_t port);

  void offer(const FrameworkID& framework,
             const hashmap<SlaveID, Resources>& resources,
             const hashmap<FrameworkID, hashmap<SlaveID, double> > scores);

  SlaveID* resortByPosition(const FrameworkID& frameworkId,
                            const hashmap<SlaveID, Resources>& available,
                            hashmap<FrameworkID, hashmap<SlaveID, double> > scores);

protected:
  virtual void initialize();
  virtual void finalize();
  virtual void exited(const UPID& pid);

  void fileAttached(const Future<Nothing>& result, const std::string& path);

  // Framework Operations
  std::vector<Framework*> getActiveFrameworks() const;
  Resources launchTask(const TaskInfo& task,
                       Framework* framework,
                       Slave* slave);
  void processTasks(Offer* offer,
                    Framework* framework,
                    Slave* slave,
                    const std::vector<TaskInfo>& tasks,
                    const Filters& filters);
  void removeTask(Task* task);


  void addFramework(Framework* framework);
  void removeFramework(Framework* framework);
  Framework* getFramework(const FrameworkID& frameworkId);
  FrameworkID newFrameworkId();

  // Slave Operations
  void addSlave(Slave* slave, bool reregister = false);
  void removeSlave(Slave* slave);
  Slave* getSlave(const SlaveID& slaveId);
  SlaveID newSlaveId();

  Offer* getOffer(const OfferID& offerId);
  void removeOffer(Offer* offer, bool rescind = false);
  OfferID newOfferId();

private:
  Master(const Master&);              // No copying.
  Master& operator = (const Master&); // No assigning.

  friend struct SlaveRegistrar;
  friend struct SlaveReregistrar;

  const flags::Flags<logging::Flags, master::Flags> flags;

  UPID leader; // Current leading master.

  bool elected;

  Allocator* allocator;
  SlavesManager* slavesManager;
  Files* files;

  MasterInfo info;

  multihashmap<std::string, uint16_t> slaveHostnamePorts;

  hashmap<FrameworkID, Framework*> frameworks;
  hashmap<SlaveID, Slave*> slaves;
  hashmap<OfferID, Offer*> offers;

  int64_t nextFrameworkId; // Used to give each framework a unique ID.
  int64_t nextOfferId;     // Used to give each slot offer a unique ID.
  int64_t nextSlaveId;     // Used to give each slave a unique ID.

  double startTime; // Start time used to calculate uptime.
};


// A connected slave.
struct Slave
{
  Slave(const SlaveInfo& _info,
        const SlaveID& _id,
        const UPID& _pid,
        double time)
    : id(_id),
      info(_info),
      pid(_pid),
      registeredTime(time),
      lastHeartbeat(time),
      observer(NULL) {}

  ~Slave() {}

  void addOffer(Offer* offer)
  {
    CHECK(!offers.contains(offer));
    offers.insert(offer);
    LOG(INFO) << "Adding offer with resources " << offer->resources()
              << " on slave " << id;
    resourcesOffered += offer->resources();
  }

  void removeOffer(Offer* offer)
  {
    CHECK(offers.contains(offer));
    offers.erase(offer);
    LOG(INFO) << "Removing offer with resources " << offer->resources()
              << " on slave " << id;
    resourcesOffered -= offer->resources();
  }


  Task* getTask(const FrameworkID& frameworkId, const TaskID& taskId)
  {
    foreachvalue (Task* task, tasks) {
      if (task->framework_id() == frameworkId &&
          task->task_id() == taskId) {
        return task;
      }
    }

    return NULL;
  }

  void addTask(Task* task)
  {
    std::pair<FrameworkID, TaskID> key =
      std::make_pair(task->framework_id(), task->task_id());
    CHECK(tasks.count(key) == 0);
    tasks[key] = task;
    LOG(INFO) << "Adding task with resources " << task->resources()
              << " on slave " << id;
    resourcesInUse += task->resources();
  }

  void removeTask(Task* task)
  {
    std::pair<FrameworkID, TaskID> key =
      std::make_pair(task->framework_id(), task->task_id());
    CHECK(tasks.count(key) > 0);
    tasks.erase(key);
    LOG(INFO) << "Removing task with resources " << task->resources()
              << " on slave " << id;
    resourcesInUse -= task->resources();
  }


  bool hasExecutor(const FrameworkID& frameworkId,
       const ExecutorID& executorId)
  {
    return executors.contains(frameworkId) &&
      executors[frameworkId].contains(executorId);
  }

  void addExecutor(const FrameworkID& frameworkId,
                   const ExecutorInfo& executorInfo)
  {
    CHECK(!hasExecutor(frameworkId, executorInfo.executor_id()));
    executors[frameworkId][executorInfo.executor_id()] = executorInfo;

    // Update the resources in use to reflect running this executor.
    resourcesInUse += executorInfo.resources();
  }

  void removeExecutor(const FrameworkID& frameworkId,
                      const ExecutorID& executorId)
  {
    if (hasExecutor(frameworkId, executorId)) {
      // Update the resources in use to reflect removing this executor.
      resourcesInUse -= executors[frameworkId][executorId].resources();

      executors[frameworkId].erase(executorId);
      if (executors[frameworkId].size() == 0) {
        executors.erase(frameworkId);
      }
    }
  }


  const SlaveID id;
  const SlaveInfo info;

  UPID pid;

  double registeredTime;
  double lastHeartbeat;

  Resources resourcesOffered; // Resources offered.
  Resources resourcesInUse;   // Resources used by tasks and executors.

  // Executors running on this slave.
  hashmap<FrameworkID, hashmap<ExecutorID, ExecutorInfo> > executors;
  hashmap<std::pair<FrameworkID, TaskID>, Task*> tasks;

  // Active offers on this slave.
  hashset<Offer*> offers;

  SlaveObserver* observer;

private:
  Slave(const Slave&);              // No copying.
  Slave& operator = (const Slave&); // No assigning.
};


// Information about a connected or completed framework.
struct Framework
{
  Framework(const FrameworkInfo& _info,
            const FrameworkID& _id,
            const UPID& _pid,
            double time)
    : id(_id),
      info(_info),
      pid(_pid),
      active(true),
      registeredTime(time),
      reregisteredTime(time),
      completedTasks(MAX_COMPLETED_TASKS_PER_FRAMEWORK) {}

  ~Framework() {}

  Task* getTask(const TaskID& taskId)
  {
    if (tasks.count(taskId) > 0) {
      return tasks[taskId];
    } else {
      return NULL;
    }
  }

  void addTask(Task* task)
  {
    CHECK(!tasks.contains(task->task_id()));
    tasks[task->task_id()] = task;
    resources += task->resources();
  }

  void removeTask(Task* task)
  {
    CHECK(tasks.contains(task->task_id()));

    completedTasks.push_back(*task);
    tasks.erase(task->task_id());
    resources -= task->resources();
  }

  void addOffer(Offer* offer)
  {
    CHECK(!offers.contains(offer));
    offers.insert(offer);
    resources += offer->resources();
  }

  void removeOffer(Offer* offer)
  {
    CHECK(offers.find(offer) != offers.end());
    offers.erase(offer);
    resources -= offer->resources();
  }

  bool hasExecutor(const SlaveID& slaveId,
                   const ExecutorID& executorId)
  {
    return executors.contains(slaveId) &&
      executors[slaveId].contains(executorId);
  }

  void addExecutor(const SlaveID& slaveId,
                   const ExecutorInfo& executorInfo)
  {
    CHECK(!hasExecutor(slaveId, executorInfo.executor_id()));
    executors[slaveId][executorInfo.executor_id()] = executorInfo;

    // Update our resources to reflect running this executor.
    resources += executorInfo.resources();
  }

  void removeExecutor(const SlaveID& slaveId,
                      const ExecutorID& executorId)
  {
    if (hasExecutor(slaveId, executorId)) {
      // Update our resources to reflect removing this executor.
      resources -= executors[slaveId][executorId].resources();

      executors[slaveId].erase(executorId);
      if (executors[slaveId].size() == 0) {
        executors.erase(slaveId);
      }
    }
  }


  const FrameworkID id; // TODO(benh): Store this in 'info.
  const FrameworkInfo info;

  UPID pid;

  bool active; // Turns false when framework is being removed.
  double registeredTime;
  double reregisteredTime;
  double unregisteredTime;

  hashmap<TaskID, Task*> tasks;

  boost::circular_buffer<Task> completedTasks;

  hashset<Offer*> offers; // Active offers for framework.

  Resources resources; // Total resources (tasks + offers + executors).

  hashmap<SlaveID, hashmap<ExecutorID, ExecutorInfo> > executors;

private:
  Framework(const Framework&);              // No copying.
  Framework& operator = (const Framework&); // No assigning.
};


} // namespace master {
} // namespace internal {
} // namespace avalon {

#endif // __MASTER_HPP__
