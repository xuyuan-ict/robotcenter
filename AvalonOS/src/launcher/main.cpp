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

#include <avalon/avalon.hpp>

#include <stout/strings.hpp>
#include <stout/os.hpp>

#include "launcher/launcher.hpp"

using namespace avalon;
using namespace avalon::internal; // For 'utils'.


int main(int argc, char** argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  std::string env;
  std::ifstream fin("slaveConf", std::ios::in);
  getline(fin, env);
  os::setenv("AVALON_FRAMEWORK_ID", env);
  getline(fin, env);
  os::setenv("AVALON_EXECUTOR_ID", env);
  getline(fin, env);
  os::setenv("AVALON_COMMAND", env);
  getline(fin, env);
  os::setenv("AVALON_EXECUTOR_URIS", env);
  getline(fin, env);
  os::setenv("AVALON_USER", env);
  getline(fin, env);
  os::setenv("AVALON_WORK_DIRECTORY", env);
  getline(fin, env);
  os::setenv("AVALON_SLAVE_PID", env);
  getline(fin, env);
  os::setenv("AVALON_HADOOP_HOME", env);
  getline(fin, env);
  os::setenv("AVALON_REDIRECT_IO", env);
  getline(fin, env);
  os::setenv("AVALON_SWITCH_USER", env);
  getline(fin, env);
  os::setenv("AVALON_CONTAINER", env);
  fin.close();
  os::rm("slaveConf");

  FrameworkID frameworkId;
  frameworkId.set_value(os::getenv("AVALON_FRAMEWORK_ID"));

  ExecutorID executorId;
  executorId.set_value(os::getenv("AVALON_EXECUTOR_ID"));

  CommandInfo commandInfo;
  commandInfo.set_value(os::getenv("AVALON_COMMAND"));

  // Construct URIs from the encoded environment string.
  const std::string& uris = os::getenv("AVALON_EXECUTOR_URIS");
  foreach (const std::string& token, strings::tokenize(uris, " ")) {
    size_t pos = token.rfind("+"); // Delim between uri and exec permission.
    CHECK(pos != std::string::npos) << "Invalid executor uri token in env "
                                    << token;

    CommandInfo::URI uri;
    uri.set_value(token.substr(0, pos));
    uri.set_executable(token.substr(pos + 1) == "1");

    commandInfo.add_uris()->MergeFrom(uri);
  }

  return avalon::internal::launcher::ExecutorLauncher(
      frameworkId,
      executorId,
      commandInfo,
      os::getenv("AVALON_USER"),
      os::getenv("AVALON_WORK_DIRECTORY"),
      os::getenv("AVALON_SLAVE_PID"),
      os::getenv("AVALON_FRAMEWORKS_HOME", false),
      os::getenv("AVALON_HADOOP_HOME"),
      os::getenv("AVALON_REDIRECT_IO") == "1",
      os::getenv("AVALON_SWITCH_USER") == "1",
      os::getenv("AVALON_CONTAINER", false))
    .run();
}
