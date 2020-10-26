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

void print_topics_list(rd_kafka_topic_partition_list_t* kf_topic_partition_list) {
  for (std::size_t i = 0; i < kf_topic_partition_list->cnt; ++i) {
    std::cerr << "kf_topic_partition_list: \u001b[33m[topic: " << kf_topic_partition_list->elems[i].topic <<
        ", partition: " << kf_topic_partition_list->elems[i].partition <<
        ", offset: " << kf_topic_partition_list->elems[i].offset << "]\u001b[0m" << std::endl;
  }
}

void print_kafka_message(const rd_kafka_message_t* rkmessage) {
  if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error("Received error message from broker.");
  }
  std::string topicName = rd_kafka_topic_name(rkmessage->rkt);
  std::string message(reinterpret_cast<char*>(rkmessage->payload), rkmessage->len);
  char* key = reinterpret_cast<char*>(rkmessage->key);
  rd_kafka_timestamp_type_t tstype;
  int64_t timestamp;
  rd_kafka_headers_t *hdrs;
  timestamp = rd_kafka_message_timestamp(rkmessage, &tstype);
  const char *tsname = "?";
  if (tstype != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
    if (tstype == RD_KAFKA_TIMESTAMP_CREATE_TIME) {
      tsname = "create time";
    } else if (tstype == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME) {
      tsname = "log append time";
    }
  }

  std::cerr << "Message: \u001b[33m" << message.c_str() << "\u001b[0m" << std::endl;
  std::cerr << "Topic: " << topicName.c_str() <<
      ", \u001b[32mOffset: " << rkmessage->offset <<
      ", (" << rkmessage->len << " bytes)\n" <<
      "Message timestamp: " << timestamp <<
      " \u001b[33m(" << (!timestamp ? 0 : static_cast<int>(time(NULL)) - static_cast<int>((timestamp / 1000))) << "s ago)\u001b[0m" << std::endl;
}
}  // namespace rd_kafka_utils
