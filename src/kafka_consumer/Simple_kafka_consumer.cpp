/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "kafka_consumer/Simple_kafka_consumer.h"

const int Simple_kafka_consumer::COMMUNICATIONS_TIMEOUT_MS{ 2000 };

void rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* partitions, void* /*opaque*/) {
  std::cerr << "\u001b[37;1mRebalance triggered.\u001b[0m" << std::endl;
  switch (err) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
      std::cerr << "assigned" << std::endl;
      rd_kafka_utils::print_topics_list(partitions);
      rd_kafka_assign(rk, partitions);
      break;

    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
      std::cerr << "revoked:" << std::endl;
      rd_kafka_utils::print_topics_list(partitions);
      rd_kafka_assign(rk, NULL);
      break;

    default:
      std::cerr << "failed: " << rd_kafka_err2str(err) << std::endl;
      rd_kafka_assign(rk, NULL);
      break;
  }
}

Simple_kafka_consumer::Simple_kafka_consumer(
    const std::string& kafka_brokers, const std::vector<std::string>& topic_list, const std::string& topic_name_format, const std::string& group_id, const bool transactional) {
  using rd_kafka_utils::setKafkaConfigurationField;

  conf_ = { rd_kafka_conf_new(), rd_kafka_utils::rd_kafka_conf_deleter() };
  if (conf_ == nullptr) {
    throw std::runtime_error("Failed to create rd_kafka_conf_t object");
  }

  // Set rebalance callback for use with coordinated consumer group balancing
  rd_kafka_conf_set_rebalance_cb(conf_.get(), rebalance_cb);

  // setKafkaConfigurationField(conf_.get(), "debug", "all");
  setKafkaConfigurationField(conf_.get(), "bootstrap.servers", kafka_brokers);
  setKafkaConfigurationField(conf_.get(), "auto.offset.reset", "latest");
  setKafkaConfigurationField(conf_.get(), "enable.auto.commit", "false");
  setKafkaConfigurationField(conf_.get(), "enable.auto.offset.store", "false");
  setKafkaConfigurationField(conf_.get(), "isolation.level", transactional ? "read_committed" : "read_uncommitted");
  setKafkaConfigurationField(conf_.get(), "group.id", group_id);

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
    rd_kafka_utils::print_kafka_message(message_wrapper);
    // Commit offsets on broker for the provided list of partitions
    std::cerr << "\u001b[33mCommitting offset: " << message_wrapper->offset << ".\u001b[0m" << std::endl;
    rd_kafka_commit_message(consumer_.get(), message_wrapper, /* async = */ false);
  }
  std::cerr << "Done resetting offset manually." << std::endl;
}

void Simple_kafka_consumer::createTopicPartitionList(const std::vector<std::string>& topic_list, const std::string& topic_name_format) {
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

  rd_kafka_resp_err_t subscribe_response = rd_kafka_subscribe(consumer_.get(), kf_topic_partition_list_.get());
  if (subscribe_response != RD_KAFKA_RESP_ERR_NO_ERROR) {
    std::cerr << "\u001b[31mrd_kafka_subscribe error " << subscribe_response << ": " << rd_kafka_err2str(subscribe_response) << "\u001b[0m" << std::endl;
  }
}

std::vector<std::string> Simple_kafka_consumer::poll_messages(const std::size_t max_poll_records) {
  std::cerr << "Polling for " << max_poll_records << " messages." << std::endl;
  std::vector<std::unique_ptr<rd_kafka_message_t, rd_kafka_utils::rd_kafka_message_deleter>> messages;
  messages.reserve(max_poll_records);
  while (messages.size() < max_poll_records) {
    std::cerr << "Polling for new message..." << std::endl;
    rd_kafka_message_t* message = rd_kafka_consumer_poll(consumer_.get(), COMMUNICATIONS_TIMEOUT_MS);
    if (!message || RD_KAFKA_RESP_ERR_NO_ERROR != message->err) {
      break;
    }
    rd_kafka_utils::print_kafka_message(message);
    messages.emplace_back(std::move(message), rd_kafka_utils::rd_kafka_message_deleter());
  }

  // if (messages.empty()) {
  //   return {};
  // }
  for (const auto& message : messages) {
    // Commit offsets on broker for the provided list of partitions
    const int async = 0;
    // TODO(hunyadi): check commit queue requirements
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_commit_message(consumer_.get(), message.get(), async)) {
      std::cerr << "\u001b[31mCommitting offsets failed.\u001b[0m" << std::endl;
    }
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_position(consumer_.get(), kf_topic_partition_list_.get())) {
      std::cerr << "\u001b[31mRetrieving current offsets for topics+partitions failed.\u001b[0m" << std::endl;
    }
    if (RD_KAFKA_RESP_ERR_NO_ERROR != rd_kafka_committed(consumer_.get(), kf_topic_partition_list_.get(), COMMUNICATIONS_TIMEOUT_MS)) {
      std::cerr << "\u001b[31mRetrieving committed offsets for topics+partitions failed.\u001b[0m" << std::endl;
    }
  }
  std::vector<std::string> message_contents;
  message_contents.reserve(messages.size());
  std::transform(messages.begin(), messages.end(), std::back_inserter(message_contents), [&]
    (const auto& message) {
      std::string message_content{ reinterpret_cast<char*>(message->payload), message->len };
      if (message_content.empty()) {
        std::cerr << "\u001b[31mError: message received contains no data.\u001b[0m" << std::endl;
        std::runtime_error("Unexpected empty messsage during polling.");
      }
      return message_content;
    });
  if (max_poll_records != message_contents.size()) {
    std::cerr << "\u001b[31mError: failed to fetch " << max_poll_records << " message(s).\u001b[0m" << std::endl;
  }
  return message_contents;
}
