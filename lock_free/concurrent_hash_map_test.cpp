#include "SimpleConcurrentHashMap.hpp"
#include <folly/concurrency/ConcurrentHashMap.h>
#include <iostream>
#include <unordered_map>
#include <string>
#include <gtest/gtest.h>

TEST(FollyConcurrentHashMap, insert) {
  folly::ConcurrentHashMap<int,int> schm;
  auto res = schm.emplace(1, 2);
  auto it = schm.find(1);
  ASSERT_TRUE(it != schm.end());
  ASSERT_TRUE(res.second);
}

TEST(SimpleConcurrentHashMap, emplace) {
  util::Concurrent::SimpleConcurrentHashMap<std::string, int> chm;
  auto res1 = chm.emplace("aaaa", 2);
  auto it1 = chm.find("aaaa");
  ASSERT_NE(it1, chm.end());
  ASSERT_TRUE(res1.second);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}


