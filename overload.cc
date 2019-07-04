#include <glog/logging.h>
#include <string_view>

template<typename F1, typename F2>
struct overloaded : F1, F2 {
  overloaded(F1 f1, F2 f2) : F1(f1), F2(f2) {}
  using F1::operator();
  using F2::operator();
};

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  auto f1 = 
      [](int a, int b) {LOG(INFO) << a + b;};
  auto f2 = 
      [](std::string_view s, std::string_view s2) {
        LOG(INFO) << s << "\t" << s2;};
  auto ol = overloaded<decltype(f1), decltype(f2)>(
      f1,
      f2
  );
  ol(1,2);
  ol("abc", "efg");
}
