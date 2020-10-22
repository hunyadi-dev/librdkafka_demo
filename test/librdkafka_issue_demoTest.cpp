#include "kafka_producer/Simple_kafka_producer.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

class Lib_rdkafka_test {
 public:
 private:
};

TEST_CASE_METHOD(Lib_rdkafka_test, "Publish and receive flow-files from Kafka.") {
  REQUIRE(true);
}
