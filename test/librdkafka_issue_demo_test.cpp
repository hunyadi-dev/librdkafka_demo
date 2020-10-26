#include "kafka_producer/Simple_kafka_producer.h"
#include "kafka_consumer/Simple_kafka_consumer.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

class Lib_rdkafka_test {
 public:
  void singleConsumerWithPlainTextTest(const std::vector<std::string>& messages, std::vector<Simple_kafka_producer::PublishEvent> event_list) {
    Simple_kafka_consumer consumer("localhost:9092", { "ConsumeKafkaTest" }, "pattern", "librdkafka_issue_demo_group", true);
    Simple_kafka_producer producer("localhost:9092", "ConsumeKafkaTest", true);
    producer.publish_messages_to_topic(messages, event_list);
  }
 private:
};

TEST_CASE_METHOD(Lib_rdkafka_test, "Publish and receive flow-files from Kafka.") {
  const auto PUBLISH            = Simple_kafka_producer::PublishEvent::PUBLISH;
  const auto TRANSACTION_START  = Simple_kafka_producer::PublishEvent::TRANSACTION_START;
  const auto TRANSACTION_COMMIT = Simple_kafka_producer::PublishEvent::TRANSACTION_COMMIT;
  const auto CANCEL             = Simple_kafka_producer::PublishEvent::CANCEL;
  // singleConsumerWithPlainTextTest(true, "both", false, {}, {      "The Black Sheep",              "Honore De Balzac"},    NON_COMMITTED_TRANSACTION, "localhost:9092", "PLAINTEXT",         "ConsumeKafkaTest",        {},    {}, {"test_group_id"}, {},        {}, {}, {}, {}, {}, "1 sec", "2 sec" ); // NOLINT
  // singleConsumerWithPlainTextTest(true, "both", false, {}, {               "Brexit",                  CANCEL_MESSAGE},            COMMIT_AND_CANCEL, "localhost:9092", "PLAINTEXT",         "ConsumeKafkaTest",        {}, false, {"test_group_id"}, {},        {}, {}, {}, {}, {}, "1 sec", "2 sec" ); // NOLINT
  singleConsumerWithPlainTextTest({ "Magician", "Raymond E. Feist" }, { TRANSACTION_START, PUBLISH, PUBLISH, TRANSACTION_COMMIT });
  singleConsumerWithPlainTextTest({ "Operation Dark Heart" }, { TRANSACTION_START, PUBLISH, CANCEL }); // NOLINT

  REQUIRE(true);
}
