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

void print_topics_list(rd_kafka_topic_partition_list_t* kf_topic_partition_list) {
  for (std::size_t i = 0; i < kf_topic_partition_list->cnt; ++i) {
    std::cerr << "kf_topic_partition_list: \u001b[33m[topic: " << kf_topic_partition_list->elems[i].topic <<
        ", partition: " << kf_topic_partition_list->elems[i].partition <<
        ", offset: " << kf_topic_partition_list->elems[i].offset << "]\u001b[0m" << std::endl;
  }
}

void rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* partitions, void* /*opaque*/) {
  std::cerr << "\u001b[37;1mRebalance triggered.\u001b[0m" << std::endl;
  switch (err) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
      std::cerr << "assigned" << std::endl;
      // FIXME(hunyadi): this should only happen when running the tests -> move this implementation there
      // for (std::size_t i = 0; i < partitions->cnt; ++i) {
      //   partitions->elems[i].offset = RD_KAFKA_OFFSET_END;
      // }
      print_topics_list(partitions);
      rd_kafka_assign(rk, partitions);
      break;

    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
      std::cerr << "revoked:" << std::endl;
      print_topics_list(partitions);
      rd_kafka_assign(rk, NULL);
      break;

    default:
      std::cerr << "failed: " << rd_kafka_err2str(err) << std::endl;
      rd_kafka_assign(rk, NULL);
      break;
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

class Simple_kafka_consumer {
 public:
  explicit Simple_kafka_consumer(
      const std::string& kafka_brokers, const std::vector<std::string>& topic_list, const std::string& topic_name_format, const std::string& group_id, const bool transactional) {
    using rd_kafka_utils::setKafkaConfigurationField;

    conf_ = { rd_kafka_conf_new(), rd_kafka_utils::rd_kafka_conf_deleter() };
    if (conf_ == nullptr) {
      throw std::runtime_error("Failed to create rd_kafka_conf_t object");
    }

    // Set rebalance callback for use with coordinated consumer group balancing
    // Rebalance handlers are needed for the initial configuration of the consumer
    // If they are not set, offset reset is ignored and polling produces messages
    // Registering a rebalance_cb turns off librdkafka's automatic partition
    // assignment/revocation and instead delegates that responsibility to the
    // application's rebalance_cb.
    rd_kafka_conf_set_rebalance_cb(conf_.get(), rebalance_cb);

    // setKafkaConfigurationField(conf_.get(), "debug", "all");
    setKafkaConfigurationField(conf_.get(), "bootstrap.servers", kafka_brokers);
    setKafkaConfigurationField(conf_.get(), "auto.offset.reset", "latest");
    setKafkaConfigurationField(conf_.get(), "enable.auto.commit", "false");
    setKafkaConfigurationField(conf_.get(), "enable.auto.offset.store", "false");
    setKafkaConfigurationField(conf_.get(), "isolation.level", transactional ? "read_committed" : "read_uncommitted");
    setKafkaConfigurationField(conf_.get(), "group.id", group_id);
    setKafkaConfigurationField(conf_.get(), "batch.num.messages", std::to_string(BATCH_NUM_MESSAGES));

    std::array<char, 512U> errstr{};
    consumer_ = { rd_kafka_new(RD_KAFKA_CONSUMER, conf_.release(), errstr.data(), errstr.size()), rd_kafka_utils::rd_kafka_consumer_deleter() };
    if (consumer_ == nullptr) {
      const std::string error_msg { errstr.begin(), errstr.end() };
      throw std::runtime_error("Failed to create Kafka consumer: " + error_msg);
    }

    createTopicPartitionList(topic_list, topic_name_format);

    rd_kafka_resp_err_t poll_set_consumer_response = rd_kafka_poll_set_consumer(consumer_.get());
    if (poll_set_consumer_response != RD_KAFKA_RESP_ERR_NO_ERROR) {
      std::cerr << "\u001b[31mrd_kafka_poll_set_consumer error " << poll_set_consumer_response << ": " << rd_kafka_err2str(poll_set_consumer_response) << "\u001b[0m" << std::endl;
    }

    std::cerr << "Resetting offset manually." << std::endl;
    while (true) {
      rd_kafka_message_t* message_wrapper = rd_kafka_consumer_poll(consumer_.get(), COMMUNICATIONS_TIMEOUT_MS);
      if (!message_wrapper) {
        break;
      }
      print_kafka_message(message_wrapper);
      // Commit offsets on broker for the provided list of partitions
      std::cerr << "\u001b[33mCommitting offset: " << message_wrapper->offset << ".\u001b[0m" << std::endl;
      rd_kafka_commit_message(consumer_.get(), message_wrapper, /* async = */ false);
    }
    std::cerr << "Done resetting offset manually." << std::endl;
  }

  void createTopicPartitionList(const std::vector<std::string>& topic_list, const std::string& topic_name_format) {
    kf_topic_partition_list_ = { rd_kafka_topic_partition_list_new(topic_list.size()), rd_kafka_utils::rd_kafka_topic_partition_list_deleter() };

    // On subscriptions any topics prefixed with ^ will be regex matched
    if (topic_name_format == "pattern") {
      for (const std::string& topic : topic_list) {
        const std::string regex_format = "^" + topic;
        rd_kafka_topic_partition_list_add(kf_topic_partition_list_.get(), regex_format.c_str(), RD_KAFKA_PARTITION_UA);  // TODO(hunyadi): check RD_KAFKA_PARTITION_UA
      }
    } else {
      for (const std::string& topic : topic_list) {
        rd_kafka_topic_partition_list_add(kf_topic_partition_list_.get(), topic.c_str(), RD_KAFKA_PARTITION_UA);  // TODO(hunyadi): check RD_KAFKA_PARTITION_UA
      }
    }

    // Subscribe to topic set using balanced consumer groups
    // Subscribing from the same process without an inbetween unsubscribe call
    // Does not seem to be triggering a rebalance (maybe librdkafka bug?)
    // This might happen until the cross-overship between processors and connections are settled
    rd_kafka_resp_err_t subscribe_response = rd_kafka_subscribe(consumer_.get(), kf_topic_partition_list_.get());
    if (subscribe_response != RD_KAFKA_RESP_ERR_NO_ERROR) {
      std::cerr << "\u001b[31mrd_kafka_subscribe error " << subscribe_response << ": " << rd_kafka_err2str(subscribe_response) << "\u001b[0m" << std::endl;
    }
  }

 private:
  static const std::size_t BATCH_NUM_MESSAGES;
  static const int COMMUNICATIONS_TIMEOUT_MS;

  std::unique_ptr<rd_kafka_t, rd_kafka_utils::rd_kafka_consumer_deleter> consumer_;
  std::unique_ptr<rd_kafka_conf_t, rd_kafka_utils::rd_kafka_conf_deleter> conf_;
  std::unique_ptr<rd_kafka_topic_partition_list_t, rd_kafka_utils::rd_kafka_topic_partition_list_deleter> kf_topic_partition_list_;
};
