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

#pragma once

#include <array>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rd_kafka_utils/rd_kafka_utils.h"

class Simple_kafka_consumer {
 public:
  explicit Simple_kafka_consumer(
      const std::string& kafka_brokers, const std::vector<std::string>& topic_list, const std::string& topic_name_format, const std::string& group_id, const bool transactional);

  void createTopicPartitionList(const std::vector<std::string>& topic_list, const std::string& topic_name_format);
  std::vector<std::string> poll_messages(const std::size_t max_poll_records);

 private:
  static const int COMMUNICATIONS_TIMEOUT_MS;

  std::unique_ptr<rd_kafka_t, rd_kafka_utils::rd_kafka_consumer_deleter> consumer_;
  std::unique_ptr<rd_kafka_conf_t, rd_kafka_utils::rd_kafka_conf_deleter> conf_;
  std::unique_ptr<rd_kafka_topic_partition_list_t, rd_kafka_utils::rd_kafka_topic_partition_list_deleter> kf_topic_partition_list_;
};
