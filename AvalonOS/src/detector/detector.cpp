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

#include <glog/logging.h>

#include <fstream>
#include <ios>
#include <vector>

#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>
#include <process/timer.hpp>

#include <stout/error.hpp>
#include <stout/foreach.hpp>
#include <stout/none.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

#include "detector/detector.hpp"

#include "logging/logging.hpp"

#include "messages/messages.hpp"

using process::Future;
using process::Process;
using process::Timer;
using process::UPID;
using process::wait; // Necessary on some OS's to disambiguate.

using std::pair;
using std::string;
using std::vector;

namespace avalon {
namespace internal {


MasterDetector::~MasterDetector() {}


Try<MasterDetector*> MasterDetector::create(const string& master,
                                            const UPID& pid,
                                            bool contend,
                                            bool quiet)
{
  if (master == "") {
    if (contend) {
      return new BasicMasterDetector(pid);
    } else {
      return Error("Cannot detect master");
    }
  } else if (master.find("file://") == 0) {
    const std::string& path = master.substr(7);
    std::ifstream file(path.c_str());
    if (!file.is_open()) {
      return Error("Failed to open file at '" + path + "'");
    }

    std::string line;
    getline(file, line);

    if (!file) {
      file.close();
      return Error("Failed to read from file at '" + path + "'");
    }

    file.close();

    return create(line, pid, contend, quiet);
  }

  // Okay, try and parse what we got as a PID.
  process::UPID masterPid = master.find("master@") == 0
    ? process::UPID(master)
    : process::UPID("master@" + master);

  if (!masterPid) {
    return Error("Cannot parse '" + std::string(masterPid) + "'");
  }

  return new BasicMasterDetector(masterPid, pid);
}


void MasterDetector::destroy(MasterDetector *detector)
{
  if (detector != NULL)
    delete detector;
}


BasicMasterDetector::BasicMasterDetector(const UPID& _master)
  : master(_master)
{
  // Elect the master.
  NewMasterDetectedMessage message;
  message.set_pid(master);
  process::post(master, message);
}


BasicMasterDetector::BasicMasterDetector(const UPID& _master,
					 const UPID& pid,
					 bool elect)
  : master(_master)
{
  if (elect) {
    // Elect the master.
    NewMasterDetectedMessage message;
    message.set_pid(master);
    process::post(master, message);
  }

  // Tell the pid about the master.
  NewMasterDetectedMessage message;
  message.set_pid(master);
  process::post(pid, message);
}


BasicMasterDetector::BasicMasterDetector(const UPID& _master,
					 const vector<UPID>& pids,
					 bool elect)
  : master(_master)
{
  if (elect) {
    // Elect the master.
    NewMasterDetectedMessage message;
    message.set_pid(master);
    process::post(master, message);
  }

  // Tell each pid about the master.
  foreach (const UPID& pid, pids) {
    NewMasterDetectedMessage message;
    message.set_pid(master);
    process::post(pid, message);
  }
}


BasicMasterDetector::~BasicMasterDetector() {}


} // namespace internal {
} // namespace avalon {
