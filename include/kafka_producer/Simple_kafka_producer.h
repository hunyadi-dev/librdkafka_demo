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

#include <chrono>
#include <iostream>
#include <memory>
#include <deque>
#include <string>
#include <vector>

#include "rd_kafka_utils/rd_kafka_utils.h"

class Simple_kafka_producer {
 public:
  static const std::string CANCEL_MESSAGE;

  enum class PublishEvent {
    PUBLISH,
    TRANSACTION_START,
    TRANSACTION_COMMIT,
    CANCEL
  };

  explicit Simple_kafka_producer(const std::string& kafka_brokers, const std::string& topic, const bool transactional);

  void publish_messages_to_topic(const std::vector<std::string>& messages_container, const std::vector<PublishEvent>& events);

 private:
  static const std::chrono::milliseconds TRANSACTIONS_TIMEOUT_MS;

  void publish_message(const std::string& message);

  std::unique_ptr<rd_kafka_t, rd_kafka_utils::rd_kafka_producer_deleter> producer_;
  std::unique_ptr<rd_kafka_topic_t, rd_kafka_utils::rd_kafka_topic_deleter> topic_;
};
