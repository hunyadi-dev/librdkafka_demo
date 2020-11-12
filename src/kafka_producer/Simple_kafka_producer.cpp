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

#include "kafka_producer/Simple_kafka_producer.h"

#include <array>

#include <thread>
#include <chrono>

const std::chrono::milliseconds Simple_kafka_producer::TRANSACTIONS_TIMEOUT_MS{ 2000 };

Simple_kafka_producer::Simple_kafka_producer(const std::string& kafka_brokers, const std::string& topic, const bool transactional) {
  using rd_kafka_utils::setKafkaConfigurationField;
  std::unique_ptr<rd_kafka_conf_t, rd_kafka_utils::rd_kafka_conf_deleter> conf = { rd_kafka_conf_new(), rd_kafka_utils::rd_kafka_conf_deleter() };

  setKafkaConfigurationField(conf.get(), "bootstrap.servers", kafka_brokers);
  setKafkaConfigurationField(conf.get(), "compression.codec", "snappy");
  setKafkaConfigurationField(conf.get(), "batch.num.messages", "1");

  if (transactional) {
    setKafkaConfigurationField(conf.get(), "transactional.id", "ConsumeKafkaTest_transaction_id");
    setKafkaConfigurationField(conf.get(), "transaction.timeout.ms", std::to_string(TRANSACTIONS_TIMEOUT_MS.count()));
  }

  static std::array<char, 512U> errstr{};
  producer_ = { rd_kafka_new(RD_KAFKA_PRODUCER, conf.release(), errstr.data(), errstr.size()), rd_kafka_utils::rd_kafka_producer_deleter() };
  if (producer_ == nullptr) {
    const std::string error_msg { errstr.begin(), errstr.end() };
    throw std::runtime_error("Failed to create Kafka producer: " + error_msg);
  }

  topic_ = { rd_kafka_topic_new(producer_.get(), topic.c_str(), nullptr), rd_kafka_utils::rd_kafka_topic_deleter() };

  if (transactional) {
    rd_kafka_init_transactions(producer_.get(), TRANSACTIONS_TIMEOUT_MS.count());
  }
}

void Simple_kafka_producer::publish_messages_to_topic(const std::vector<std::string>& messages_container, const std::vector<PublishEvent>& events) {
  auto next_message = messages_container.cbegin();
  for (const PublishEvent event : events) {
    switch (event) {
      case PublishEvent::PUBLISH:
        assert(messages_container.cend() != next_message);
        publish_message(*next_message);
        std::advance(next_message, 1);
        break;
      case PublishEvent::TRANSACTION_START:
        std::cerr << "Starting new transaction..." << std::endl;
        rd_kafka_begin_transaction(producer_.get());
        break;
      case PublishEvent::TRANSACTION_COMMIT:
        std::cerr << "Committing transaction..." << std::endl;
        rd_kafka_commit_transaction(producer_.get(), TRANSACTIONS_TIMEOUT_MS.count());
        break;
      case PublishEvent::CANCEL:
        std::cerr << "Cancelling transaction..." << std::endl;
        rd_kafka_abort_transaction(producer_.get(), TRANSACTIONS_TIMEOUT_MS.count());
    }
  }
}
void Simple_kafka_producer::publish_message(const std::string& message) {
  std::cerr << "Producing: " << message.c_str() << std::endl;

  std::unique_ptr<rd_kafka_headers_t, rd_kafka_utils::rd_kafka_headers_deleter> headers(rd_kafka_headers_new(8), rd_kafka_utils::rd_kafka_headers_deleter());
  if (!headers) {
    throw std::runtime_error("Generating message headers failed.");
  }
  const auto err = rd_kafka_producev(
      producer_.get(),
      RD_KAFKA_V_RKT(topic_.get()),
      RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
      RD_KAFKA_V_VALUE(const_cast<char*>(&message[0]), message.size()),
      RD_KAFKA_V_HEADERS(headers.release()),
      RD_KAFKA_V_END);

  if (err) {
    std::cerr << "Producer failure: " << rd_kafka_last_error() << ": " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
  }

  // if (rd_kafka_produce(topic_.get(), RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, strdup(message.c_str()), message.size(), nullptr, 0, nullptr)) {
  //   std::cerr << "Producer failure: " << rd_kafka_last_error() << ": " << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
  // }
}
