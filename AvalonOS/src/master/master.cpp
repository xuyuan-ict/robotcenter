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

#include <fstream>
#include <iomanip>
#include <list>
#include <sstream>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/id.hpp>
#include <process/run.hpp>

#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/utils.hpp>
#include <stout/uuid.hpp>

#include "common/build.hpp"
#include "common/date_utils.hpp"

#include "flags/flags.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "master/allocator.hpp"
#include "master/flags.hpp"
#include "master/master.hpp"
#include "master/slaves_manager.hpp"

namespace params = std::tr1::placeholders;

using std::list;
using std::string;
using std::vector;

using process::wait; // Necessary on some OS's to disambiguate.

using std::tr1::cref;
using std::tr1::bind;


namespace avalon {
namespace internal {
namespace master {


class SlaveObserver : public Process<SlaveObserver>
{
public:
  SlaveObserver(const UPID& _slave,
                const SlaveInfo& _slaveInfo,
                const SlaveID& _slaveId,
                const PID<Master>& _master)
    : ProcessBase(ID::generate("slave-observer")),
      slave(_slave),
      slaveInfo(_slaveInfo),
      slaveId(_slaveId),
      master(_master),
      timeouts(0),
      pinged(false)
  {
    install("PONG", &SlaveObserver::pong);
  }

protected:
  virtual void initialize()
  {
    send(slave, "PING");
    pinged = true;
    delay(SLAVE_PING_TIMEOUT, self(), &SlaveObserver::timeout);
  }

  void pong(const UPID& from, const string& body)
  {
    timeouts = 0;
    pinged = false;
  }

  void timeout()
  {
    if (pinged) { // So we haven't got back a pong yet ...
      if (++timeouts >= MAX_SLAVE_PING_TIMEOUTS) {
        deactivate();
        return;
      }
    }

    send(slave, "PING");
    pinged = true;
    delay(SLAVE_PING_TIMEOUT, self(), &SlaveObserver::timeout);
  }

  void deactivate()
  {
    dispatch(master, &Master::deactivatedSlaveHostnamePort,
             slaveInfo.hostname(), slave.port);
  }

private:
  const UPID slave;
  const SlaveInfo slaveInfo;
  const SlaveID slaveId;
  const PID<Master> master;
  uint32_t timeouts;
  bool pinged;
};


// Performs slave registration asynchronously. There are two means of
// doing this, one first tries to add this slave to the slaves
// manager, while the other one simply tells the master to add the
// slave.
struct SlaveRegistrar
{
  static bool run(Slave* slave, const PID<Master>& master)
  {
    // TODO(benh): Do a reverse lookup to ensure IP maps to
    // hostname, or check credentials of this slave.
    dispatch(master, &Master::addSlave, slave, false);
    return true;
  }

  // static bool run(Slave* slave,
  //                 const PID<Master>& master,
  //                 const PID<SlavesManager>& slavesManager)
  // {
  //   Future<bool> added = dispatch(slavesManager, &SlavesManager::add,
  //                                 slave->info.hostname(), slave->pid.port);
  //   added.await();
  //   if (!added.isReady() || !added.get()) {
  //     LOG(WARNING) << "Could not register slave on "<< slave->info.hostname()
  //                  << " because failed to add it to the slaves maanger";
  //     // TODO(benh): This could be because our acknowledgement to the
  //     // slave was dropped, so they retried, and now we should
  //     // probably send another acknowledgement.
  //     delete slave;
  //     return false;
  //   }

  //   return run(slave, master);
  // }
};


Master::Master(Allocator* _allocator, Files* _files)
  : ProcessBase("master"),
    flags(),
    allocator(_allocator),
    files(_files) {}


Master::Master(Allocator* _allocator,
               Files* _files,
               const flags::Flags<logging::Flags, master::Flags>& _flags)
  : ProcessBase("master"),
    flags(_flags),
    allocator(_allocator),
    files(_files) {}


Master::~Master()
{
  LOG(INFO) << "Shutting down master";

  foreachvalue (Slave* slave, utils::copy(slaves)) {
    removeSlave(slave);
  }

  // CHECK(offers.size() == 0);

  // terminate(slavesManager);
  // wait(slavesManager);

  // delete slavesManager;

}


void Master::initialize()
{
  LOG(INFO) << "Master started on " << string(self()).substr(7);

  // The master ID is currently comprised of the current date, the IP
  // address and port from self() and the OS PID.

  Try<string> id =
    strings::format("%s-%u-%u-%d", DateUtils::currentDate(),
                    self().ip, self().port, getpid());

  CHECK(!id.isError()) << id.error();

  info.set_id(id.get());
  info.set_ip(self().ip);
  info.set_port(self().port);

  LOG(INFO) << "Master ID: " << info.id();

  // Setup slave manager.
  // slavesManager = new SlavesManager(flags, self());
  // spawn(slavesManager);

  // Initialize the allocator.
  allocator->initialize(flags, self());

  nextSlaveId = 0;

  startTime = Clock::now();

  // Install handler functions for certain messages.
  install<NewMasterDetectedMessage>(
      &Master::newMasterDetected,
      &NewMasterDetectedMessage::pid);

  install<NoMasterDetectedMessage>(
      &Master::noMasterDetected);

  install<RegisterFrameworkMessage>(
      &Master::registerFramework,
      &RegisterFrameworkMessage::framework);

  install<UnregisterFrameworkMessage>(
      &Master::unregisterFramework,
      &UnregisterFrameworkMessage::framework_id);

  install<RegisterSlaveMessage>(
      &Master::registerSlave,
      &RegisterSlaveMessage::slave);

  install<UnregisterSlaveMessage>(
      &Master::unregisterSlave,
      &UnregisterSlaveMessage::slave_id);

  install<LaunchTasksMessage>(
      &Master::launchTasks,
      &LaunchTasksMessage::framework_id,
      &LaunchTasksMessage::offer_id,
      &LaunchTasksMessage::tasks,
      &LaunchTasksMessage::filters);

  install<StatusUpdateMessage>(
      &Master::statusUpdate,
      &StatusUpdateMessage::update,
      &StatusUpdateMessage::pid);

  if (flags.log_dir.isSome()) {
    Try<string> log = logging::getLogFile(google::INFO);
    if (log.isError()) {
      LOG(ERROR) << "Master log file cannot be found: " << log.error();
    } else {
      files->attach(log.get(), "/master/log")
        .onAny(defer(self(), &Self::fileAttached, params::_1, log.get()));
    }
  }
}


void Master::finalize()
{
  LOG(INFO) << "Master terminating";
  foreachvalue (Slave* slave, slaves) {
    send(slave->pid, ShutdownMessage());
  }
}


void Master::exited(const UPID& pid)
{
  foreachvalue (Slave* slave, slaves) {
    if (slave->pid == pid) {
      LOG(INFO) << "Slave " << slave->id << "(" << slave->info.hostname()
                << ") disconnected";
      removeSlave(slave);
      return;
    }
  }
}


void Master::fileAttached(const Future<Nothing>& result, const string& path)
{
  CHECK(!result.isDiscarded());
  if (result.isReady()) {
    LOG(INFO) << "Successfully attached file '" << path << "'";
  } else {
    LOG(ERROR) << "Failed to attach file '" << path << "': "
               << result.failure();
  }
}


void Master::newMasterDetected(const UPID& pid)
{
  // Check and see if we are 
  // (1) still waiting to be the elected master, 
  // (2) newly elected master, 
  // (3) no longer elected master, or
  // (4) still elected master.

  leader = pid;

  if (leader != self() && !elected) {
    LOG(INFO) << "Waiting to be master!";
  } else if (leader == self() && !elected) {
    LOG(INFO) << "Elected as master!";
    elected = true;
  } else if (leader != self() && elected) {
    LOG(FATAL) << "No longer elected master ... committing suicide!";
  } else if (leader == self() &&& elected) {
    LOG(INFO) << "Still acting as master!";
  }
}


void Master::noMasterDetected()
{
  if (elected) {
    LOG(FATAL) << "No longer elected master ... comitting suicide";
  } else {
    LOG(FATAL) << "No master detected (?) ... comitting suicide!";
  }
}

/**********************
 ***** Framework ******
 **********************/
void Master::registerFramework(const FrameworkInfo& frameworkInfo)
{
  if (!elected) {
    LOG(WARNING) << "Ignoring register framework message since not elected yet";
    return;
  }

  Framework* framework =
    new Framework(frameworkInfo, newFrameworkId(), from, Clock::now());

  LOG(INFO) << "Registering framework " << framework->id << " at " << from;

  bool rootSubmissions = flags.root_submissions;

  // if (framework->info.user() == "root" && rootSubmissions == false) {
  //   LOG(INFO) << framework << " registering as root, but "
  //             << "root submissions are disabled on this cluster";
  //   FrameworkErrorMessage message;
  //   message.set_message("User 'root' is not allowed to run frameworks");
  //   reply(message);
  //   delete framework;
  //   return;
  // }

  addFramework(framework);
}


void Master::unregisterFramework(const FrameworkID& frameworkId)
{
  LOG(INFO) << "Asked to unregister framework " << frameworkId;

  Framework* framework = getFramework(frameworkId);
  if (framework != NULL) {
    if (framework->pid == from) {
      removeFramework(framework);
    } else {
      LOG(WARNING) << from << " tried to unregister framework; "
                   << "expecting " << framework->pid;
    }
  }
}


void Master::addFramework(Framework* framework)
{
  CHECK(frameworks.count(framework->id) == 0);

  frameworks[framework->id] = framework;

  link(framework->pid);

  FrameworkRegisteredMessage message;
  message.mutable_framework_id()->MergeFrom(framework->id);
  message.mutable_master_info()->MergeFrom(info);
  send(framework->pid, message);

  allocator->frameworkAdded(framework->id,
                            framework->info,
                            framework->resources);
}


void Master::removeFramework(Framework* framework)
{
  // Tell slaves to shutdown the framework.
  foreachvalue (Slave* slave, slaves) {
    ShutdownFrameworkMessage message;
    message.mutable_framework_id()->MergeFrom(framework->id);
    send(slave->pid, message);
  }

  // Remove pointers to the framework's tasks in slaves.
  foreachvalue (Task* task, utils::copy(framework->tasks)) {
    Slave* slave = getSlave(task->slave_id());
    // Since we only find out about tasks when the slave reregisters,
    // it must be the case that the slave exists!
    CHECK(slave != NULL);
    removeTask(task);
  }

  // Remove the framework's offers (if they weren't removed before).
  foreach (Offer* offer, utils::copy(framework->offers)) {
    allocator->resourcesRecovered(offer->framework_id(),
                                  offer->slave_id(),
                                  Resources(offer->resources()));
    removeOffer(offer);
  }

  // Remove the framework's executors for correct resource accounting.
  foreachkey (const SlaveID& slaveId, framework->executors) {
    Slave* slave = getSlave(slaveId);
    if (slave != NULL) {
      foreachpair (const ExecutorID& executorId,
                   const ExecutorInfo& executorInfo,
                   framework->executors[slaveId]) {
        allocator->resourcesRecovered(framework->id,
                                      slave->id,
                                      executorInfo.resources());
        slave->removeExecutor(framework->id, executorId);
      }
    }
  }

  // TODO(benh): Similar code between removeFramework and
  // failoverFramework needs to be shared!

  // TODO(benh): unlink(framework->pid);

  framework->unregisteredTime = Clock::now();

  // The completedFramework buffer now owns the framework pointer.
  // completedFrameworks.push_back(std::tr1::shared_ptr<Framework>(framework));
  
  // Remove it.
  frameworks.erase(framework->id);
  allocator->frameworkRemoved(framework->id);
}


FrameworkID Master::newFrameworkId()
{
  std::ostringstream out;

  out << info.id() << "-" << std::setw(4)
      << std::setfill('0') << nextFrameworkId++;

  FrameworkID frameworkId;
  frameworkId.set_value(out.str());

  return frameworkId;
}


Framework* Master::getFramework(const FrameworkID& frameworkId)
{
  if (frameworks.count(frameworkId) > 0) {
    return frameworks[frameworkId];
  } else {
    return NULL;
  }
}


/**********************
 ******** Slave *******
 **********************/
void Master::registerSlave(const SlaveInfo& slaveInfo)
{
  if (!elected) {
    LOG(WARNING) << "Ignoring register slave message from "
                 << slaveInfo.hostname() << " since not elected yet";
    return;
  }

  // Check if this slave is already registered (because it retries).
  foreachvalue (Slave* slave, slaves) {
    if (slave->pid == from) {
      LOG(INFO) << "Slave " << slave->id << " (" << slave->info.hostname()
                << ") already registered, resending acknowledgement";
      SlaveRegisteredMessage message;
      message.mutable_slave_id()->MergeFrom(slave->id);
      reply(message);
      return;
    }
  }

  Slave* slave = new Slave(slaveInfo, newSlaveId(), from, Clock::now());

  LOG(INFO) << "Attempting to register slave on " << slave->info.hostname()
            << " at " << slave->pid;

  // TODO(benh): We assume all slaves can register for now.
  CHECK(flags.slaves == "*");
  activatedSlaveHostnamePort(slave->info.hostname(), slave->pid.port);
  addSlave(slave);

//   // Checks if this slave, or if all slaves, can be accepted.
//   if (slaveHostnamePorts.contains(slaveInfo.hostname(), from.port)) {
//     run(&SlaveRegistrar::run, slave, self());
//   } else if (flags.slaves == "*") {
//     run(&SlaveRegistrar::run, slave, self(), slavesManager->self());
//   } else {
//     LOG(WARNING) << "Cannot register slave at "
//                  << slaveInfo.hostname() << ":" << from.port
//                  << " because not in allocated set of slaves!";
//     reply(ShutdownMessage());
//   }
}


void Master::unregisterSlave(const SlaveID& slaveId)
{
  LOG(INFO) << "Asked to unregister slave " << slaveId;

  // TODO(benh): Check that only the slave is asking to unregister?

  Slave* slave = getSlave(slaveId);
  if (slave != NULL) {
    removeSlave(slave);
  }
}


void Master::statusUpdate(const StatusUpdate& update, const UPID& pid)
{
  const TaskStatus& status = update.status();

  LOG(INFO) << "Status update from " << from
            << ": task " << status.task_id()
            << " of framework " << update.framework_id()
            << " is now in state " << status.state();

  //------------------@Ailias Begin ---------------------//
 
  // if(status.has_robot_status()){
  //   RobotStatus robotStatus = status.robot_status();
  //   char robot_name[100], model_state_cmd[500];
  //   sprintf(robot_name, "%s", robotStatus.robot_name().c_str());
  //   getRobotModelStateCMD(robotStatus, model_state_cmd);
  //   if(setRobotModelState(robot_name, model_state_cmd))
  //     std::cout<<"\nSet robot["<<robot_name<<"] Model State success\n";
  //   else
  //     std::cout<<"\nSet robot["<<robot_name<<"] Model State failed\n";
  // }

  //------------------@Ailias End -----------------------//
 
  Slave* slave = getSlave(update.slave_id());
  if (slave != NULL) {
    Framework* framework = getFramework(update.framework_id());
    if (framework != NULL) {
      // Pass on the (transformed) status update to the framework.
      StatusUpdateMessage message;
      message.mutable_update()->MergeFrom(update);
      message.set_pid(pid);
      send(framework->pid, message);

      // Lookup the task and see if we need to update anything locally.
      Task* task = slave->getTask(update.framework_id(), status.task_id());
      if (task != NULL) {
        task->set_state(status.state());

        // Handle the task appropriately if it's terminated.
        if (status.state() == TASK_FINISHED ||
            status.state() == TASK_FAILED ||
            status.state() == TASK_KILLED ||
            status.state() == TASK_LOST) {
          removeTask(task);
        }

        // stats.tasks[status.state()]++;

        // stats.validStatusUpdates++;
      } else {
        LOG(WARNING) << "Status update from " << from << " ("
                     << slave->info.hostname() << "): error, couldn't lookup "
                     << "task " << status.task_id();
        // stats.invalidStatusUpdates++;
      }
    } else {
      LOG(WARNING) << "Status update from " << from << " ("
                   << slave->info.hostname() << "): error, couldn't lookup "
                   << "framework " << update.framework_id();
      // stats.invalidStatusUpdates++;
    }
  } else {
    LOG(WARNING) << "Status update from " << from
                 << ": error, couldn't lookup slave "
                 << update.slave_id();
    // stats.invalidStatusUpdates++;
  }
}


void Master::activatedSlaveHostnamePort(const string& hostname, uint16_t port)
{
  LOG(INFO) << "Master now considering a slave at "
            << hostname << ":" << port << " as active";
  slaveHostnamePorts.put(hostname, port);
}


void Master::deactivatedSlaveHostnamePort(const string& hostname,
                                          uint16_t port)
{
  if (slaveHostnamePorts.contains(hostname, port)) {
    // Look for a connected slave and remove it.
    foreachvalue (Slave* slave, slaves) {
      if (slave->info.hostname() == hostname && slave->pid.port == port) {
        LOG(WARNING) << "Removing slave " << slave->id << " at "
                     << hostname << ":" << port
                     << " because it has been deactivated";
        send(slave->pid, ShutdownMessage());
        removeSlave(slave);
        break;
      }
    }

    LOG(INFO) << "Master now considering a slave at "
	            << hostname << ":" << port << " as inactive";
    slaveHostnamePorts.remove(hostname, port);
  }
}


void Master::addSlave(Slave* slave, bool reregister)
{
  CHECK(slave != NULL);

  LOG(INFO) << "Adding slave " << slave->id
            << " at " << slave->info.hostname()
            << " with cRes: " << slave->info.cres() 
            << " and dRes: " << slave->info.dres() 
            << " and aRes: " << slave->info.ares();


  slaves[slave->id] = slave;

  link(slave->pid);

  if (!reregister) {
    SlaveRegisteredMessage message;
    message.mutable_slave_id()->MergeFrom(slave->id);
    send(slave->pid, message);
  } 

  // TODO(benh):
  //     // Ask the slaves manager to monitor this slave for us.
  //     dispatch(slavesManager->self(), &SlavesManager::monitor,
  //              slave->pid, slave->info, slave->id);

  // Set up an observer for the slave.
  slave->observer = new SlaveObserver(slave->pid, slave->info,
                                      slave->id, self());
  spawn(slave->observer);

  if (!reregister) {
    allocator->slaveAdded(slave->id,
                          slave->info);
  }

}


// Lose all of a slave's tasks and delete the slave object
void Master::removeSlave(Slave* slave)
{
  // Delete it.
  slaves.erase(slave->id);
  allocator->slaveRemoved(slave->id);
  delete slave;
}


Slave* Master::getSlave(const SlaveID& slaveId)
{
  if (slaves.count(slaveId) > 0) {
    return slaves[slaveId];
  } else {
    return NULL;
  }
}


SlaveID Master::newSlaveId()
{
  SlaveID slaveId;
  slaveId.set_value(info.id() + "-" + stringify(nextSlaveId++));
  return slaveId;
}


/**********************
 ******** Offer *******
 **********************/
bool compare(double a, double b) {return (a<b);}

SlaveID* Master::resortByPosition(
    const FrameworkID& frameworkId,
    const hashmap<SlaveID, Resources>& available,
    hashmap<FrameworkID, hashmap<SlaveID, double> > scores)
{
  std::vector<double> scoreList;
  hashmap<double, SlaveID> slaveList;
  SlaveID* newOder = new SlaveID[available.keys().size()];
  size_t i = 0;

  foreachkey(const SlaveID slaveId, available) {
    scoreList.push_back(scores[frameworkId][slaveId]);
    slaveList[scores[frameworkId][slaveId]] = slaveId;
  }

  sort(scoreList.begin(), scoreList.end(), compare);
  for (std::vector<double>::iterator it = scoreList.begin();
       it != scoreList.end(); it++, i++) {
    const SlaveID& slaveId = slaveList[*it];
    // const Resources& resources = available.get(slaveId).get();
    newOder[i] = slaveId;
    LOG(INFO) << "Resort Order: " << "Slave " << slaveId
              << " Score " << *it;
  }
  return newOder;
}

void Master::offer(const FrameworkID& frameworkId,
                   const hashmap<SlaveID, Resources>& resources,
                   const hashmap<FrameworkID, hashmap<SlaveID, double> > scores)
{
  SlaveID* newOder = 
    resortByPosition(frameworkId, resources, scores);

  if (!frameworks.contains(frameworkId) || !frameworks[frameworkId]->active) {
    LOG(WARNING) << "Master returning resources offered to framework "
                 << frameworkId << " because the framework"
                 << " has terminated or is inactive";

    foreachpair (const SlaveID& slaveId, const Resources& offered, resources) {
      allocator->resourcesRecovered(frameworkId, slaveId, offered);
    }
    return;
  }

  // Create an offer for each slave and add it to the message.
  ResourceOffersMessage message;

  Framework* framework = frameworks[frameworkId];
 
  for(size_t i=0; i<resources.keys().size(); ++i) {
    const SlaveID slaveId = newOder[i];
    const Resources& offered = resources.get(slaveId).get();
    if (!slaves.contains(slaveId)) {
      LOG(WARNING) << "Master returning resources offered to framework "
                   << frameworkId << " because slave " << slaveId
                   << " is not valid";

      allocator->resourcesRecovered(frameworkId, slaveId, offered);
      continue;
    }

    Slave* slave = slaves[slaveId];

    Offer* offer = new Offer();
    offer->mutable_id()->MergeFrom(newOfferId());
    offer->mutable_framework_id()->MergeFrom(framework->id);
    offer->mutable_slave_id()->MergeFrom(slave->id);
    offer->set_hostname(slave->info.hostname());
    offer->mutable_resources()->MergeFrom(offered);
    offer->mutable_attributes()->MergeFrom(slave->info.attributes());
    offer->set_type(slave->info.type());
    offer->mutable_pose()->MergeFrom(slave->info.pose());
    offer->set_robot_name(slave->info.robotname());

    // Add all framework's executors running on this slave.
    if (slave->executors.contains(framework->id)) {
      const hashmap<ExecutorID, ExecutorInfo>& executors =
        slave->executors[framework->id];
      foreachkey (const ExecutorID& executorId, executors) {
        offer->add_executor_ids()->MergeFrom(executorId);
      }
    }

    offers[offer->id()] = offer;

    framework->addOffer(offer);
    slave->addOffer(offer);

    // Add the offer *AND* the corresponding slave's PID.
    message.add_offers()->MergeFrom(*offer);
    message.add_pids(slave->pid);
  }

  if (message.offers().size() == 0) {
    return;
  }

  LOG(INFO) << "Sending " << message.offers().size()
            << " offers to framework " << framework->id;

  send(framework->pid, message);
}


Resources Master::launchTask(const TaskInfo& task,
                             Framework* framework,
                             Slave* slave)
{
  CHECK(framework != NULL);
  CHECK(slave != NULL);

  Resources resources; // Total resources used on slave by launching this task.

  // Determine if this task launches an executor, and if so make sure
  // the slave and framework state has been updated accordingly.
  Option<ExecutorID> executorId;

  if (task.has_executor()) {
    // TODO(benh): Refactor this code into Slave::addTask.
    if (!slave->hasExecutor(framework->id, task.executor().executor_id())) {
      CHECK(!framework->hasExecutor(slave->id, task.executor().executor_id()));
      slave->addExecutor(framework->id, task.executor());
      framework->addExecutor(slave->id, task.executor());
      resources += task.executor().resources();
    }

    executorId = Option<ExecutorID>::some(task.executor().executor_id());
  }

  // Add the task to the framework and slave.
  Task* t = new Task();
  t->mutable_framework_id()->MergeFrom(framework->id);
  t->set_state(TASK_STAGING);
  t->set_name(task.name());
  t->mutable_task_id()->MergeFrom(task.task_id());
  t->mutable_slave_id()->MergeFrom(task.slave_id());
  t->mutable_resources()->MergeFrom(task.resources());

  if (executorId.isSome()) {
    t->mutable_executor_id()->MergeFrom(executorId.get());
  }

  framework->addTask(t);

  slave->addTask(t);

  resources += task.resources();

  // Tell the slave to launch the task!
  LOG(INFO) << "Launching task " << task.task_id()
            << " of framework " << framework->id
            << " with resources " << task.resources() << " on slave "
            << slave->id << " (" << slave->info.hostname() << ")";

  RunTaskMessage message;
  message.mutable_framework()->MergeFrom(framework->info);
  message.mutable_framework_id()->MergeFrom(framework->id);
  message.set_pid(framework->pid);
  message.mutable_task()->MergeFrom(task);
  send(slave->pid, message);

  // stats.tasks[TASK_STAGING]++;

  return resources;
}


void Master::removeTask(Task* task)
{
  // Remove from framework.
  Framework* framework = getFramework(task->framework_id());
  if (framework != NULL) { // A framework might not be re-connected yet.
    framework->removeTask(task);
  }

  // Remove from slave.
  Slave* slave = getSlave(task->slave_id());
  CHECK(slave != NULL);
  slave->removeTask(task);

  // Tell the allocator about the recovered resources.
  allocator->resourcesRecovered(task->framework_id(),
                                task->slave_id(),
                                Resources(task->resources()));

  delete task;
}


void Master::removeOffer(Offer* offer, bool rescind)
{
  // Remove from framework.
  Framework* framework = getFramework(offer->framework_id());
  CHECK(framework != NULL);
  framework->removeOffer(offer);

  // Remove from slave.
  Slave* slave = getSlave(offer->slave_id());
  CHECK(slave != NULL);
  slave->removeOffer(offer);

  // if (rescind) {
  //   RescindResourceOfferMessage message;
  //   message.mutable_offer_id()->MergeFrom(offer->id());
  //   send(framework->pid, message);
  // }

  // Delete it.
  offers.erase(offer->id());
  delete offer;
}


// Process a resource offer reply (for a non-cancelled offer) by
// launching the desired tasks (if the offer contains a valid set of
// tasks) and reporting used resources to the allocator.
void Master::processTasks(Offer* offer,
                          Framework* framework,
                          Slave* slave,
                          const vector<TaskInfo>& tasks,
                          const Filters& filters)
{
  LOG(INFO) << "Processing reply for offer " << offer->id()
            << " on slave " << slave->id
            << " (" << slave->info.hostname() << ")"
            << " for framework " << framework->id;

  Resources usedResources; // Accumulated resources used from this offer.

  // Loop through each task and check it's validity.
  foreach (const TaskInfo& task, tasks) {
    usedResources += launchTask(task, framework, slave);
  } 

  // All used resources should be allocatable, enforced by our validators.
  CHECK(usedResources == usedResources.allocatable());

  LOG(INFO) << "Slave " << slave->id << " Resources Info";
  LOG(INFO) << "* TOTAL: " << offer->resources() << " *";
  LOG(INFO) << "* USED: " << usedResources << " *";

  usedResources = Resources::updateRes(offer->resources(), usedResources);
  Resources unusedResources = offer->resources() - usedResources;

  LOG(INFO) << "* UNUSED: " << unusedResources << " *";

  if (unusedResources.allocatable().size() > 0) {
    // Tell the allocator about the unused (e.g., refused) resources.
    allocator->resourcesUnused(offer->framework_id(),
                               offer->slave_id(),
                               unusedResources);
  }

  removeOffer(offer);
}


void Master::launchTasks(const FrameworkID& frameworkId,
                         const OfferID& offerId,
                         const vector<TaskInfo>& tasks,
                         const Filters& filters)
{
  Framework* framework = getFramework(frameworkId);
  if (framework != NULL) {
    // TODO(benh): Support offer "hoarding" and allow multiple offers
    // *from the same slave* to be used to launch tasks. This can be
    // accomplished rather easily by collecting and merging all offers
    // into a mega-offer and passing that offer to
    // Master::processTasks.
    Offer* offer = getOffer(offerId);
    if (offer != NULL) {
      CHECK(offer->framework_id() == frameworkId);
      Slave* slave = getSlave(offer->slave_id());
      CHECK(slave != NULL) << "An offer should not outlive a slave!";
      processTasks(offer, framework, slave, tasks, filters);
    } else {
      // The offer is gone (possibly rescinded, lost slave, re-reply
      // to same offer, etc). Report all tasks in it as failed.
      // TODO: Consider adding a new task state TASK_INVALID for
      // situations like these.
      LOG(WARNING) << "Offer " << offerId << " is no longer valid";
      // foreach (const TaskInfo& task, tasks) {
      //   StatusUpdateMessage message;
      //   StatusUpdate* update = message.mutable_update();
      //   update->mutable_framework_id()->MergeFrom(frameworkId);
      //   TaskStatus* status = update->mutable_status();
      //   status->mutable_task_id()->MergeFrom(task.task_id());
      //   status->set_state(TASK_LOST);
      //   status->set_message("Task launched with invalid offer");
      //   update->set_timestamp(Clock::now());
      //   update->set_uuid(UUID::random().toBytes());
      //   send(framework->pid, message);
      // }
    }
  }
}


Offer* Master::getOffer(const OfferID& offerId)
{
  if (offers.count(offerId) > 0) {
    return offers[offerId];
  } else {
    return NULL;
  }
}


OfferID Master::newOfferId()
{
  OfferID offerId;
  offerId.set_value(info.id() + "-" + stringify(nextOfferId++));
  return offerId;
}


} // namespace master {
} // namespace internal {
} // namespace avalon {
