#include "kafka_producer/Simple_kafka_producer.h"
#include "kafka_consumer/Simple_kafka_consumer.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

class Lib_rdkafka_test {
 public:
  void singleConsumerWithPlainTextTest(const std::size_t num_expected_messages,
      const std::vector<std::string>& messages,
      std::vector<Simple_kafka_producer::PublishEvent> event_list,
      const std::size_t max_poll_records) {
    Simple_kafka_consumer consumer("localhost:9092", { "ConsumeKafkaTest" }, "pattern", "librdkafka_issue_demo_group", true);
    Simple_kafka_producer producer("localhost:9092", "ConsumeKafkaTest", true);
    producer.publish_messages_to_topic(messages, event_list);
    std::vector<std::string> received_messages = consumer.poll_messages(max_poll_records);
    for (const std::string& message : received_messages) {
      std::cout << "Message: \u001b[33m" << message << "\u001b[0m" << std::endl;
    }
    REQUIRE(num_expected_messages == received_messages.size());
  }
};

TEST_CASE_METHOD(Lib_rdkafka_test, "Publish and receive flow-files from Kafka.") {
  const auto PUBLISH            = Simple_kafka_producer::PublishEvent::PUBLISH;
  const auto TRANSACTION_START  = Simple_kafka_producer::PublishEvent::TRANSACTION_START;
  const auto TRANSACTION_COMMIT = Simple_kafka_producer::PublishEvent::TRANSACTION_COMMIT;
  const auto CANCEL             = Simple_kafka_producer::PublishEvent::CANCEL;

  singleConsumerWithPlainTextTest(2, { "Mistborn", "Brandon Sanderson" }, { TRANSACTION_START, PUBLISH, PUBLISH, TRANSACTION_COMMIT }, 2);

  // Consume only one of the two published messages here
  singleConsumerWithPlainTextTest(1, {"The Lord of the Rings",  "J. R. R. Tolkien"}, { TRANSACTION_START, PUBLISH, PUBLISH, TRANSACTION_COMMIT }, 1);

  // Non-committed transaction
  singleConsumerWithPlainTextTest(0, { "Magician", "Raymond E. Feist" }, { TRANSACTION_START, PUBLISH, PUBLISH }, 2);

  // Commit and cancel
  singleConsumerWithPlainTextTest(0, { "Operation Dark Heart" }, { TRANSACTION_START, PUBLISH, CANCEL }, 1);

  REQUIRE(true);
}
