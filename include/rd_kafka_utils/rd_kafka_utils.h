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

#include <string>
#include <iostream>
#include <chrono>

#include "librdkafka/rdkafka.h"

namespace rd_kafka_utils {
struct rd_kafka_conf_deleter {
  void operator()(rd_kafka_conf_t* ptr) const noexcept { rd_kafka_conf_destroy(ptr); }
};

struct rd_kafka_producer_deleter {
  void operator()(rd_kafka_t* ptr) const noexcept {
    std::cerr << "\u001b[37;1mInvoked producer deleter\u001b[0m" << std::endl;
    rd_kafka_resp_err_t flush_ret = rd_kafka_flush(ptr, 10000 /* ms */);
    if (RD_KAFKA_RESP_ERR__TIMED_OUT == flush_ret) {
      std::cerr << "Deleting producer failed: time-out while trying to flush" << std::endl;
    }
    rd_kafka_destroy(ptr);
    std::cerr << "\u001b[37;1mProducer_deleter done\u001b[0m" << std::endl;
  }
};

struct rd_kafka_consumer_deleter {
  void operator()(rd_kafka_t* ptr) const noexcept {
    std::cerr << "\u001b[37;1mInvoked consumer deleter.\u001b[0m" << std::endl;
    // rd_kafka_unsubscribe(ptr);
    std::cerr << "\u001b[37;1mClosing consumer connections...\u001b[0m" << std::endl;
    rd_kafka_consumer_close(ptr);
    std::cerr << "\u001b[37;1mAttempting to destroy consumer...\u001b[0m" << std::endl;
    rd_kafka_destroy(ptr);
    std::cerr << "\u001b[37;1mConsumer deleter done,\u001b[0m" << std::endl;
  }
};

struct rd_kafka_topic_partition_list_deleter {
  void operator()(rd_kafka_topic_partition_list_t* ptr) const noexcept {
    std::cerr << "\u001b[37;1mInvoked topic partition list deleter.\u001b[0m" << std::endl;
    rd_kafka_topic_partition_list_destroy(ptr);
  }
};

struct rd_kafka_topic_conf_deleter {
  void operator()(rd_kafka_topic_conf_t* ptr) const noexcept { rd_kafka_topic_conf_destroy(ptr); }
};

struct rd_kafka_topic_deleter {
  void operator()(rd_kafka_topic_t* ptr) const noexcept {
    std::cerr << "\u001b[37;1mInvoked topic deleter.\u001b[0m" << std::endl;
    rd_kafka_topic_destroy(ptr);
  }
};

struct rd_kafka_headers_deleter {
  void operator()(rd_kafka_headers_t* ptr) const noexcept { rd_kafka_headers_destroy(ptr); }
};

struct rd_kafka_message_deleter {
  void operator()(rd_kafka_message_t* ptr) const noexcept { rd_kafka_message_destroy(ptr); }
};

void setKafkaConfigurationField(rd_kafka_conf_t* configuration, const std::string& field_name, const std::string& value);
void print_kafka_message(const rd_kafka_message_t* rkmessage);
void print_topics_list(rd_kafka_topic_partition_list_t* kf_topic_partition_list);

}  // namespace rd_kafka_utils
