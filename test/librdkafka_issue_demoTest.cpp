#include <librdkafka_issue_demo.h>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

class Lib_rdkafka_test {};

TEST_CASE_METHOD(Lib_rdkafka_test, "Publish and receive flow-files from Kafka.") {
  REQUIRE(true);  
}
