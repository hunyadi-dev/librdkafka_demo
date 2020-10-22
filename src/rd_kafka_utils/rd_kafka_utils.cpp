/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "rd_kafka_utils/rd_kafka_utils.h"

#include <array>
#include <exception>

namespace rd_kafka_utils {
void setKafkaConfigurationField(rd_kafka_conf_t* configuration, const std::string& field_name, const std::string& value) {
  static std::array<char, 512U> errstr{};
  rd_kafka_conf_res_t result;
  result = rd_kafka_conf_set(configuration, field_name.c_str(), value.c_str(), errstr.data(), errstr.size());
  // logger_->log_debug("Setting kafka configuration field bootstrap.servers:= %s", value);
  if (result != RD_KAFKA_CONF_OK) {
    const std::string error_msg { errstr.begin(), errstr.end() };
    throw std::runtime_error("rd_kafka configuration error" + error_msg);
  }
}
}  // namespace rd_kafka_utils
