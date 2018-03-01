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

#include <errno.h>
#include <signal.h>

#include <algorithm>
#include <iomanip>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>

#include <stout/duration.hpp>
#include <stout/fs.hpp>
#include <stout/lambda.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/numify.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/utils.hpp>

#include "common/build.hpp"
#include "common/protobuf_utils.hpp"
#include "common/type_utils.hpp"
#include "common/robot.hpp"

#include "logging/logging.hpp"

#include "slave/flags.hpp"
#include "slave/paths.hpp"
#include "slave/slave.hpp"

namespace params = std::tr1::placeholders;

using std::string;

using process::wait; // Necessary on some OS's to disambiguate.

using std::tr1::cref;
using std::tr1::bind;


namespace avalon {
namespace internal {
namespace slave {

using namespace avalon::internal::robot;

Slave::Slave(const Resources& _cRes,
             bool _local,
             IsolationModule* _isolationModule,
             Files* _files)
  : ProcessBase(ID::generate("slave")),
    flags(),
    local(_local),
    cRes(_cRes),
    completedFrameworks(MAX_COMPLETED_FRAMEWORKS),
    isolationModule(_isolationModule),
    files(_files)
{
  // Ensure slave work directory exists.
  CHECK_SOME(os::mkdir(flags.work_dir))
    << "Failed to create slave work directory '" << flags.work_dir << "'";
}


Slave::Slave(const flags::Flags<logging::Flags, slave::Flags>& _flags,
             bool _local,
             IsolationModule* _isolationModule,
             Files* _files)
  : ProcessBase(ID::generate("slave")),
    flags(_flags),
    local(_local),
    completedFrameworks(MAX_COMPLETED_FRAMEWORKS),
    isolationModule(_isolationModule),
    files(_files)
{
  // Ensure slave work directory exists.
  CHECK_SOME(os::mkdir(flags.work_dir))
    << "Failed to create slave work directory '" << flags.work_dir << "'";

  if (flags.cRes.isNone()) {
    // TODO(benh): Move this computation into Flags as the "default".
    Try<long> cpus = os::cpus();
    Try<uint64_t> mem = os::memory();

    // NOTE: We calculate disk availability of the file system on
    // which the slave work directory is mounted.
    Try<uint64_t> disk = fs::available(flags.work_dir);

    if (!cpus.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the number of cpus to use,"
                   << " defaulting to 1";
      cpus = Try<long>::some(1);
    }

    if (!mem.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the size of main memory,"
                   << " defaulting to 1024 MB";
      mem = Try<uint64_t>::some(1024);
    } else {
      // Convert to MB.
      mem = mem.get() / 1048576;

      // Leave 1 GB free if we have more than 1 GB, otherwise, use all!
      // TODO(benh): Have better default scheme (e.g., % of mem not
      // greater than 1 GB?)
      if (mem.get() > 1024) {
        mem = Try<uint64_t>::some(mem.get() - 1024);
      }
    }

    if (!disk.isSome()) {
      LOG(WARNING) << "Failed to auto-detect the free disk space,"
                   << " defaulting to 10 GB";
      disk = Try<uint64_t>::some(1024 * 10);
    } else {
      // Convert to MB.
      disk = disk.get() / 1048576;

      // Leave 5 GB free if we have more than 10 GB, otherwise, use all!
      // TODO(benh): Have better default scheme (e.g., % of disk not
      // greater than 10 GB?)
      if (disk.get() > 1024 * 10) {
        disk = Try<uint64_t>::some(disk.get() - (1024 * 5));
      }
    }

    Try<string> defaults = strings::format(
        "cpus:%d;mem:%d;ports:[31000-32000];disk:%d",
        cpus.get(),
        mem.get(),
        disk.get());

    CHECK_SOME(defaults);

    cout << "cRes: " << (defaults.get()).c_str() << endl;


    cRes = Resources::parse(defaults.get(), 0);
  } else {
    cRes = Resources::parse(flags.cRes.get(), 0);
  }

  if(flags.dRes.isSome()) {
    cout << "dRes: " << (flags.dRes.get()).c_str() << endl;
    dRes = Resources::parse(flags.dRes.get(), 1);
  }

  if(flags.aRes.isSome()) {
    cout << "aRes: " << (flags.aRes.get()).c_str() << endl;
    aRes = Resources::parse(flags.aRes.get(), 2);
  }

  if(flags.pose.isNone()) {
    char** location = (char**)malloc(sizeof(char*)*13);
    for(int i=0; i<13; i++)
        location[i] = (char*)malloc(sizeof(char)*32);

    getLocation(location, getenv("AVALON_ROBOT_NAME"), flags.odom.get());
    pose = getRobotPoseFromLocation(location);

    for(int i=0; i<13; i++)
      free(location[i]);
    free(location);
  } else {
    pose = robotParse(flags.pose.get());
  }

  if(flags.transform.isSome()) {
    Position transform = robotTfParse(flags.transform.get());
    tfRobotPose(pose, transform);
  }

  if (flags.attributes.isSome()) {
    attributes = Attributes::parse(flags.attributes.get());
  }
}


Slave::~Slave()
{
  // TODO(benh): Shut down frameworks?

  // TODO(benh): Shut down executors? The executor should get an "exited"
  // event and initiate a shut down itself.
}


void Slave::initialize()
{
  LOG(INFO) << "Slave started on " << string(self()).substr(6);
  LOG(INFO) << "Slave computation resources: " << cRes;
  LOG(INFO) << "Slave data-driven resources: " << dRes;
  LOG(INFO) << "Slave action resources: " << aRes;
  printRobotPose(pose);

  // Determine our hostname.
  Try<string> result = os::hostname();

  if (result.isError()) {
    LOG(FATAL) << "Failed to get hostname: " << result.error();
  }

  string hostname = result.get();
  string robotname = getenv("AVALON_ROBOT_NAME");
  string type = getenv("AVALON_ROBOT_TYPE");

  // Initialize slave info.
  info.set_hostname(hostname);
  info.set_robotname(robotname); // Deprecated!
  info.mutable_cres()->MergeFrom(cRes);
  info.mutable_dres()->MergeFrom(dRes);
  info.mutable_ares()->MergeFrom(aRes);
  info.mutable_attributes()->MergeFrom(attributes);
  info.set_type(type);
  info.mutable_pose()->MergeFrom(pose);

  // Spawn and initialize the isolation module.
  // TODO(benh): Seems like the isolation module should really be
  // spawned before being passed to the slave.
  spawn(isolationModule);
  dispatch(isolationModule,
           &IsolationModule::initialize,
           flags,
           cRes,
           local,
           self());

  // Start disk monitoring.
  // NOTE: We send a delayed message here instead of directly calling
  // checkDiskUsage, to make disabling this feature easy (e.g by specifying
  // a very large disk_watch_interval).
  delay(flags.disk_watch_interval, self(), &Slave::checkDiskUsage);

  // Start all the statistics at 0.
  // stats.tasks[TASK_STAGING] = 0;
  // stats.tasks[TASK_STARTING] = 0;
  // stats.tasks[TASK_RUNNING] = 0;
  // stats.tasks[TASK_FINISHED] = 0;
  // stats.tasks[TASK_FAILED] = 0;
  // stats.tasks[TASK_KILLED] = 0;
  // stats.tasks[TASK_LOST] = 0;
  // stats.validStatusUpdates = 0;
  // stats.invalidStatusUpdates = 0;
  // stats.validFrameworkMessages = 0;
  // stats.invalidFrameworkMessages = 0;

  startTime = Clock::now();

  connected = false;

  // Install protobuf handlers.
  install<NewMasterDetectedMessage>(
      &Slave::newMasterDetected,
      &NewMasterDetectedMessage::pid);

  install<NoMasterDetectedMessage>(
      &Slave::noMasterDetected);

  install<SlaveRegisteredMessage>(
      &Slave::registered,
      &SlaveRegisteredMessage::slave_id);

  // install<SlaveReregisteredMessage>(
  //     &Slave::reregistered,
  //     &SlaveReregisteredMessage::slave_id);

  install<RunTaskMessage>(
      &Slave::runTask,
      &RunTaskMessage::framework,
      &RunTaskMessage::framework_id,
      &RunTaskMessage::pid,
      &RunTaskMessage::task);

  // install<KillTaskMessage>(
  //     &Slave::killTask,
  //     &KillTaskMessage::framework_id,
  //     &KillTaskMessage::task_id);

  install<ShutdownFrameworkMessage>(
      &Slave::shutdownFramework,
      &ShutdownFrameworkMessage::framework_id);

  // install<FrameworkToExecutorMessage>(
  //     &Slave::schedulerMessage,
  //     &FrameworkToExecutorMessage::slave_id,
  //     &FrameworkToExecutorMessage::framework_id,
  //     &FrameworkToExecutorMessage::executor_id,
  //     &FrameworkToExecutorMessage::data);

  // install<UpdateFrameworkMessage>(
  //     &Slave::updateFramework,
  //     &UpdateFrameworkMessage::framework_id,
  //     &UpdateFrameworkMessage::pid);

  // install<StatusUpdateAcknowledgementMessage>(
  //     &Slave::statusUpdateAcknowledgement,
  //     &StatusUpdateAcknowledgementMessage::slave_id,
  //     &StatusUpdateAcknowledgementMessage::framework_id,
  //     &StatusUpdateAcknowledgementMessage::task_id,
  //     &StatusUpdateAcknowledgementMessage::uuid);

  install<RegisterExecutorMessage>(
      &Slave::registerExecutor,
      &RegisterExecutorMessage::framework_id,
      &RegisterExecutorMessage::executor_id);

  install<StatusUpdateMessage>(
      &Slave::statusUpdate,
      &StatusUpdateMessage::update);

  // install<ExecutorToFrameworkMessage>(
  //     &Slave::executorMessage,
  //     &ExecutorToFrameworkMessage::slave_id,
  //     &ExecutorToFrameworkMessage::framework_id,
  //     &ExecutorToFrameworkMessage::executor_id,
  //     &ExecutorToFrameworkMessage::data);

  install<ShutdownMessage>(
      &Slave::shutdown);

  // Install the ping message handler.
  install("PING", &Slave::ping);

  // Setup some HTTP routes.
  // route("/vars", bind(&http::vars, cref(*this), params::_1));
  // route("/stats.json", bind(&http::json::stats, cref(*this), params::_1));
  // route("/state.json", bind(&http::json::state, cref(*this), params::_1));

  if (flags.log_dir.isSome()) {
    Try<string> log = logging::getLogFile(google::INFO);
    if (log.isError()) {
      LOG(ERROR) << "Slave log file cannot be found: " << log.error();
    } else {
      files->attach(log.get(), "/slave/log")
        .onAny(defer(self(), &Self::fileAttached, params::_1, log.get()));
    }
  }
}


void Slave::finalize()
{
  LOG(INFO) << "Slave terminating";

  // foreachkey (const FrameworkID& frameworkId, frameworks) {
  //   // TODO(benh): Because a shut down isn't instantaneous (but has
  //   // a shut down/kill phases) we might not actually propogate all
  //   // the status updates appropriately here. Consider providing
  //   // an alternative function which skips the shut down phase and
  //   // simply does a kill (sending all status updates
  //   // immediately). Of course, this still isn't sufficient
  //   // because those status updates might get lost and we won't
  //   // resend them unless we build that into the system.
  //   shutdownFramework(frameworkId);
  // }

  // Stop the isolation module.
  terminate(isolationModule);
  wait(isolationModule);
}


void Slave::shutdown()
{
  if (from != master) {
    LOG(WARNING) << "Ignoring shutdown message from " << from
                 << " because it is not from the registered master ("
                 << master << ")";
    return;
  }

  LOG(INFO) << "Slave asked to shut down by " << from;

  terminate(self());
}


void Slave::fileAttached(const Future<Nothing>& result, const string& path)
{
  CHECK(!result.isDiscarded());
  if (result.isReady()) {
    LOG(INFO) << "Successfully attached file '" << path << "'";
  } else {
    LOG(ERROR) << "Failed to attach file '" << path << "': "
               << result.failure();
  }
}


void Slave::detachFile(const Future<Nothing>& result, const std::string& path)
{
  CHECK(!result.isDiscarded());
  files->detach(path);
}


void Slave::newMasterDetected(const UPID& pid)
{
  LOG(INFO) << "New master detected at " << pid;

  master = pid;
  link(master);

  connected = false;
  doReliableRegistration();
}


void Slave::noMasterDetected()
{
  LOG(INFO) << "Lost master(s) ... waiting";
  connected = false;
  master = UPID();
}


void Slave::registered(const SlaveID& slaveId)
{
  LOG(INFO) << "Registered with master; given slave ID " << slaveId;
  id = slaveId;

  connected = true;

  // Schedule all old slave directories to get garbage
  // collected. TODO(benh): It's unclear if we really need/want to
  // wait until the slave is registered to do this.
  const string& directory = path::join(flags.work_dir, "slaves");

  foreach (const string& file, os::ls(directory)) {
    const string& path = path::join(directory, file);

    // Check that this path is a directory but not our directory!
    if (os::isdir(path) && file != id.value()) {
      gc.schedule(flags.gc_delay, path);
    }
  }
}


// void Slave::reregistered(const SlaveID& slaveId)
// {
//   LOG(INFO) << "Re-registered with master";

//   if (!(id == slaveId)) {
//     LOG(FATAL) << "Slave re-registered but got wrong ID";
//   }
//   connected = true;
// }


void Slave::doReliableRegistration()
{
  if (connected || !master) {
    return;
  }

  if (id == "") {
    // Slave started before master.
    // (Vinod): Is the above comment true?
    RegisterSlaveMessage message;
    message.mutable_slave()->MergeFrom(info);
    send(master, message);
  } else {
    // Re-registering, so send tasks running.
    // ReregisterSlaveMessage message;
    // message.mutable_slave_id()->MergeFrom(id);
    // message.mutable_slave()->MergeFrom(info);

    // foreachvalue (Framework* framework, frameworks) {
    //   foreachvalue (Executor* executor, framework->executors) {
    //     // TODO(benh): Kill this once framework_id is required on ExecutorInfo.
    //     ExecutorInfo* executorInfo = message.add_executor_infos();
    //     executorInfo->MergeFrom(executor->info);
    //     executorInfo->mutable_framework_id()->MergeFrom(framework->id);
    //     foreachvalue (Task* task, executor->launchedTasks) {
    //       // TODO(benh): Also need to send queued tasks here ...
    //       message.add_tasks()->MergeFrom(*task);
    //     }
    //   }
    // }

    // send(master, message);
  }

  // Re-try registration if necessary.
  delay(Seconds(1.0), self(), &Slave::doReliableRegistration);
}


void Slave::statusUpdateTimeout(
    const FrameworkID& frameworkId,
    const UUID& uuid)
{
  // Check and see if we still need to send this update.
  Framework* framework = getFramework(frameworkId);
  if (framework != NULL) {
    if (framework->updates.contains(uuid)) {
      const StatusUpdate& update = framework->updates[uuid];

      LOG(INFO) << "Resending status update"
                << " for task " << update.status().task_id()
                << " of framework " << update.framework_id();

      StatusUpdateMessage message;
      message.mutable_update()->MergeFrom(update);
      message.set_pid(self());
      send(master, message);

      // Send us a message to try and resend after some delay.
      delay(STATUS_UPDATE_RETRY_INTERVAL,
            self(), &Slave::statusUpdateTimeout,
            framework->id, uuid);
    }
  }
}


void Slave::statusUpdate(const StatusUpdate& update)
{
  const TaskStatus& status = update.status();
  LOG(INFO) << "Status update: task " << status.task_id()
            << " of framework " << update.framework_id()
            << " is now in state " << status.state();

  Framework* framework = getFramework(update.framework_id());
  if (framework != NULL) {
    // Send message and record the status for possible resending.
    // TODO(vinod): Revisit the strategy of always sending a status update
    // upstream, when we have persistent state at the master and slave.
    StatusUpdateMessage message;
    message.mutable_update()->MergeFrom(update);
    message.set_pid(self());
    send(master, message);

    UUID uuid = UUID::fromBytes(update.uuid());

    // Send us a message to try and resend after some delay.
    delay(STATUS_UPDATE_RETRY_INTERVAL,
          self(),
          &Slave::statusUpdateTimeout,
          framework->id,
          uuid);

    framework->updates[uuid] = update;

    // stats.tasks[status.state()]++;

    // stats.validStatusUpdates++;

    Executor* executor = framework->getExecutor(status.task_id());
    if (executor != NULL) {
      executor->updateTaskState(status.task_id(), status.state());

      // Handle the task appropriately if it's terminated.
      if (protobuf::isTerminalState(status.state())) {
        executor->removeTask(status.task_id());

        dispatch(isolationModule,
                 &IsolationModule::resourcesChanged,
                 framework->id,
                 executor->id,
                 executor->resources);
      }
    } else {
      LOG(WARNING) << "Status update error: couldn't lookup "
                   << "executor for framework " << update.framework_id();
      // stats.invalidStatusUpdates++;
    }
  } else {
    LOG(WARNING) << "Status update error: couldn't lookup "
                 << "framework " << update.framework_id();
    // stats.invalidStatusUpdates++;
  }
}


void Slave::runTask(
    const FrameworkInfo& frameworkInfo,
    const FrameworkID& frameworkId,
    const string& pid,
    const TaskInfo& task)
{
  LOG(INFO) << "Got assigned task " << task.task_id()
            << " for framework " << frameworkId;

  Framework* framework = getFramework(frameworkId);
  if (framework == NULL) {
    framework = new Framework(frameworkId, frameworkInfo, pid, flags);
    frameworks[frameworkId] = framework;
  }

  const ExecutorInfo& executorInfo = framework->getExecutorInfo(task);

  const ExecutorID& executorId = executorInfo.executor_id();

  // Either send the task to an executor or start a new executor
  // and queue the task until the executor has started.
  Executor* executor = framework->getExecutor(executorId);

  if (executor != NULL) {
    if (executor->shutdown) {
      LOG(WARNING) << "WARNING! Asked to run task '" << task.task_id()
                   << "' for framework " << frameworkId
                   << " with executor '" << executorId
                   << "' which is being shut down";

      // StatusUpdateMessage message;
      // StatusUpdate* update = message.mutable_update();
      // update->mutable_framework_id()->MergeFrom(frameworkId);
      // update->mutable_slave_id()->MergeFrom(id);
      // TaskStatus* status = update->mutable_status();
      // status->mutable_task_id()->MergeFrom(task.task_id());
      // status->set_state(TASK_LOST);
      // update->set_timestamp(Clock::now());
      // update->set_uuid(UUID::random().toBytes());
      // send(master, message);
    } else if (!executor->pid) {
      // Queue task until the executor starts up.
      LOG(INFO) << "Queuing task '" << task.task_id()
                << "' for executor " << executorId
                << " of framework '" << frameworkId;
      // executor->queuedTasks[task.task_id()] = task;
    } else {
      // Add the task and send it to the executor.
      executor->addTask(task);

      // stats.tasks[TASK_STAGING]++;

      // Update the resources.
      // TODO(Charles Reiss): The isolation module is not guaranteed to update
      // the resources before the executor acts on its RunTaskMessage.
      // dispatch(isolationModule,
      //          &IsolationModule::resourcesChanged,
      //          framework->id, executor->id, executor->resources);

      LOG(INFO) << "Sending task '" << task.task_id()
                << "' to executor '" << executorId
                << "' of framework " << framework->id;

      // RunTaskMessage message;
      // message.mutable_framework()->MergeFrom(framework->info);
      // message.mutable_framework_id()->MergeFrom(framework->id);
      // message.set_pid(framework->pid);
      // message.mutable_task()->MergeFrom(task);
      // send(executor->pid, message);
    }
  } else {
    // Launch an executor for this task.
    executor = framework->createExecutor(id, executorInfo);

    files->attach(executor->directory, executor->directory)
      .onAny(defer(self(),
                   &Self::fileAttached,
                   params::_1,
                   executor->directory));

    // Queue task until the executor starts up.
    executor->queuedTasks[task.task_id()] = task;

    // Tell the isolation module to launch the executor. (TODO(benh):
    // Make the isolation module a process so that it can block while
    // trying to launch the executor.)
    dispatch(isolationModule,
             &IsolationModule::launchExecutor,
             framework->id, framework->info, executor->info,
             executor->directory, executor->resources);
  }
}


void Slave::registerExecutor(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  LOG(INFO) << "Got registration for executor '" << executorId
            << "' of framework " << frameworkId;

  Framework* framework = getFramework(frameworkId);
  if (framework == NULL) {
    // Framework is gone; tell the executor to exit.
    LOG(WARNING) << "Framework " << frameworkId
                 << " does not exist (it may have been killed),"
                 << " telling executor to exit";
    reply(ShutdownExecutorMessage());
    return;
  }

  Executor* executor = framework->getExecutor(executorId);

  // Check the status of the executor.
  if (executor == NULL) {
    LOG(WARNING) << "WARNING! Unexpected executor '" << executorId
                 << "' registering for framework " << frameworkId;
    reply(ShutdownExecutorMessage());
  } else if (executor->pid) {
    LOG(WARNING) << "WARNING! executor '" << executorId
                 << "' of framework " << frameworkId
                 << " is already running";
    reply(ShutdownExecutorMessage());
  } else if (executor->shutdown) {
    LOG(WARNING) << "WARNING! executor '" << executorId
                 << "' of framework " << frameworkId
                 << " should be shutting down";
    reply(ShutdownExecutorMessage());
  } else {
    // Save the pid for the executor.
    if(os::getenv("ISOLATION_TYPE") == "process") {
      executor->pid = from;
    } else {
      const vector<string> id = strings::tokenize(from, "@");
      const vector<string> port = strings::tokenize(from, ":");
      const string container_name = "avalon_executor_" + executorId.value() + 
                                    "_framework_" + frameworkId.value();
      const string ip = lxcGetContainerIP(container_name);

      string contain_id = id[0] + "@" + ip + ":" + port[1];
      executor->pid = contain_id;
    }

    // First account for the tasks we're about to start.
    foreachvalue (const TaskInfo& task, executor->queuedTasks) {
      // Add the task to the executor.
      executor->addTask(task);
    }

    // Now that the executor is up, set its resource limits including the
    // currently queued tasks.
    // TODO(Charles Reiss): We don't actually have a guarantee that this will
    // be delivered or (where necessary) acted on before the executor gets its
    // RunTaskMessages.
    dispatch(isolationModule,
             &IsolationModule::resourcesChanged,
             framework->id, executor->id, executor->resources);

    // Tell executor it's registered and give it any queued tasks.
    ExecutorRegisteredMessage message;
    message.mutable_executor_info()->MergeFrom(executor->info);
    message.mutable_framework_id()->MergeFrom(framework->id);
    message.mutable_framework_info()->MergeFrom(framework->info);
    message.mutable_slave_id()->MergeFrom(id);
    message.mutable_slave_info()->MergeFrom(info);
    send(executor->pid, message);

    LOG(INFO) << "Flushing queued tasks for framework " << framework->id;

    foreachvalue (const TaskInfo& task, executor->queuedTasks) {
      // stats.tasks[TASK_STAGING]++;

      RunTaskMessage message;
      message.mutable_framework_id()->MergeFrom(framework->id);
      message.mutable_framework()->MergeFrom(framework->info);
      message.set_pid(framework->pid);
      message.mutable_task()->MergeFrom(task);
      send(executor->pid, message);
    }

    executor->queuedTasks.clear();
  }
}


void _watch(
    const Future<Nothing>& watch,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CHECK(!watch.isDiscarded());

  if (!watch.isReady()) {
    LOG(ERROR) << "Failed to watch executor " << executorId
               << " of framework " << frameworkId
               << ": " << watch.failure();
  }
}


void _unwatch(
    const Future<Nothing>& unwatch,
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CHECK(!unwatch.isDiscarded());

  if (!unwatch.isReady()) {
    LOG(ERROR) << "Failed to unwatch executor " << executorId
               << " of framework " << frameworkId
               << ": " << unwatch.failure();
  }
}


// N.B. When the slave is running in "local" mode then the pid is
// uninteresting (and possibly could cause bugs).
void Slave::executorStarted(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    pid_t pid)
{
  Framework* framework = getFramework(frameworkId);
  if (framework == NULL) {
    LOG(WARNING) << "Framework " << frameworkId
                 << " for executor '" << executorId
                 << "' is no longer valid";
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  if (executor == NULL) {
    LOG(WARNING) << "Invalid executor '" << executorId
                 << "' of framework " << frameworkId
                 << " has started";
    return;
  }

  // monitor.watch(
  //     frameworkId,
  //     executorId,
  //     executor->info,
  //     flags.resource_monitoring_interval)
  //   .onAny(lambda::bind(_watch, lambda::_1, frameworkId, executorId));
}


// Called by the isolation module when an executor process terminates.
void Slave::executorTerminated(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    int status,
    bool destroyed,
    const string& message)
{
  LOG(INFO) << "Executor '" << executorId
            << "' of framework " << frameworkId
            << (WIFEXITED(status)
                ? " has exited with status "
                : " has terminated with signal ")
            << (WIFEXITED(status)
                ? stringify(WEXITSTATUS(status))
                : strsignal(WTERMSIG(status)));

  // Stop monitoring this executor.
  // monitor.unwatch(frameworkId, executorId)
  //     .onAny(lambda::bind(_unwatch, lambda::_1, frameworkId, executorId));

  Framework* framework = getFramework(frameworkId);
  if (framework == NULL) {
    LOG(WARNING) << "Framework " << frameworkId
                 << " for executor '" << executorId
                 << "' is no longer valid";
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  if (executor == NULL) {
    LOG(WARNING) << "Invalid executor '" << executorId
                 << "' of framework " << frameworkId
                 << " has exited/terminated";
    return;
  }

  bool isCommandExecutor = false;

  // Transition all live tasks to TASK_LOST/TASK_FAILED.
  // If the isolation module destroyed the executor (e.g., due to OOM event)
  // or if this is a command executor, we send TASK_FAILED status updates
  // instead of TASK_LOST.

  // Transition all live launched tasks.
  // foreachvalue (Task* task, utils::copy(executor->launchedTasks)) {
  //   if (!protobuf::isTerminalState(task->state())) {
  //     isCommandExecutor = !task->has_executor_id();

  //     if (destroyed || isCommandExecutor) {
  //       sendStatusUpdate(
  //           frameworkId, executorId, task->task_id(), TASK_FAILED, message);
  //     } else {
  //       sendStatusUpdate(
  //           frameworkId, executorId, task->task_id(), TASK_LOST, message);
  //     }
  //   }
  // }

  // // Transition all queued tasks.
  // foreachvalue (const TaskInfo& task, utils::copy(executor->queuedTasks)) {
  //   isCommandExecutor = task.has_command();

  //   if (destroyed || isCommandExecutor) {
  //     sendStatusUpdate(
  //         frameworkId, executorId, task.task_id(), TASK_FAILED, message);
  //   } else {
  //     sendStatusUpdate(
  //         frameworkId, executorId, task.task_id(), TASK_LOST, message);
  //   }
  // }

  // if (!isCommandExecutor) {
  //   ExitedExecutorMessage message;
  //   message.mutable_slave_id()->MergeFrom(id);
  //   message.mutable_framework_id()->MergeFrom(frameworkId);
  //   message.mutable_executor_id()->MergeFrom(executorId);
  //   message.set_status(status);

  //   send(master, message);
  // }

  // Schedule the executor directory to get garbage collected.
  gc.schedule(flags.gc_delay, executor->directory)
    .onAny(defer(self(), &Self::detachFile, params::_1, executor->directory));

  framework->destroyExecutor(executor->id);
}


void Slave::shutdownFramework(const FrameworkID& frameworkId)
{
  if (from != master) {
    LOG(WARNING) << "Ignoring shutdown framework message from " << from
                 << "because it is not from the registered master ("
                 << master << ")";
    return;
  }

  LOG(INFO) << "Asked to shut down framework " << frameworkId;

  Framework* framework = getFramework(frameworkId);
  if (framework != NULL) {
    LOG(INFO) << "Shutting down framework " << framework->id;

    // Shut down all executors of this framework.
    foreachvalue (Executor* executor, framework->executors) {
      shutdownExecutor(framework, executor);
    }
  }
}


void Slave::shutdownExecutor(Framework* framework, Executor* executor)
{
  LOG(INFO) << "Shutting down executor '" << executor->id
            << "' of framework " << framework->id;

  // If the executor hasn't yet registered, this message
  // will be dropped to the floor!
  send(executor->pid, ShutdownExecutorMessage());

  executor->shutdown = true;

  // Prepare for sending a kill if the executor doesn't comply.
  delay(flags.executor_shutdown_grace_period,
        self(),
        &Slave::shutdownExecutorTimeout,
        framework->id, executor->id, executor->uuid);
}


void Slave::shutdownExecutorTimeout(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const UUID& uuid)
{
  Framework* framework = getFramework(frameworkId);
  if (framework == NULL) {
    return;
  }

  Executor* executor = framework->getExecutor(executorId);
  // Make sure this timeout is valid.
  if (executor != NULL && executor->uuid == uuid) {
    LOG(INFO) << "Killing executor '" << executor->id
              << "' of framework " << framework->id;

    dispatch(isolationModule,
             &IsolationModule::killExecutor,
             framework->id,
             executor->id);

    // Schedule the executor directory to get garbage collected.
    gc.schedule(flags.gc_delay, executor->directory)
      .onAny(defer(self(), &Self::detachFile, params::_1, executor->directory));;

    framework->destroyExecutor(executor->id);
  }

  // Cleanup if this framework has no executors running.
  if (framework->executors.size() == 0) {
    frameworks.erase(framework->id);

    // Pass ownership of the framework pointer.
    completedFrameworks.push_back(std::tr1::shared_ptr<Framework>(framework));
  }
}


void Slave::ping(const UPID& from, const string& body)
{
  send(from, "PONG");
}



void Slave::exited(const UPID& pid)
{
  LOG(INFO) << "Process exited: " << from;

  if (master == pid) {
    LOG(WARNING) << "WARNING! Master disconnected!"
                 << " Waiting for a new master to be elected.";
    // TODO(benh): After so long waiting for a master, commit suicide.
  }
}



// TODO(vinod): Figure out a way to express this function via cmd line.
Duration Slave::age(double usage)
{
 return Weeks(flags.gc_delay.weeks() * (1.0 - usage));
}


void Slave::checkDiskUsage()
{
  // TODO(vinod): We are making usage a Future, so that we can plug in
  // os::usage() into async.
  // NOTE: We calculate disk availability of the file system on
  // which the slave work directory is mounted.
  Future<Try<double> >(os::usage(flags.work_dir))
    .onAny(defer(self(), &Slave::_checkDiskUsage, params::_1));
}


void Slave::_checkDiskUsage(const Future<Try<double> >& usage)
{
  if (!usage.isReady()) {
    LOG(WARNING) << "Error getting disk usage";
  } else {
    Try<double> result = usage.get();

    if (result.isSome()) {
      double use = result.get();

      LOG(INFO) << "Current disk usage " << std::setiosflags(std::ios::fixed)
                << std::setprecision(2) << 100 * use << "%."
                << " Max allowed age: " << age(use);

      // We prune all directories whose deletion time is within
      // the next 'gc_delay - age'. Since a directory is always
      // scheduled for deletion 'gc_delay' into the future, only directories
      // that are at least 'age' old are deleted.
      gc.prune(Weeks(flags.gc_delay.weeks() - age(use).weeks()));
    } else {
      LOG(WARNING) << "Unable to get disk usage: " << result.error();
    }
  }
  delay(flags.disk_watch_interval, self(), &Slave::checkDiskUsage);
}


Framework* Slave::getFramework(const FrameworkID& frameworkId)
{
  if (frameworks.count(frameworkId) > 0) {
    return frameworks[frameworkId];
  }

  return NULL;
}


} // namespace slave {
} // namespace internal {
} // namespace avalon {
