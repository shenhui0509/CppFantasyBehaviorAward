#include <glog/logging.h>

#include <utility>

template<typename Pair>
struct Reverse {
  using second_type = typename Pair::first_type;
  using first_type = typename Pair::second_type;
  second_type second;
  first_type first;
};

struct large_struct {
  int a{0}, b{0}, c{0};
};

std::ostream& operator<<(std::ostream& os, large_struct l) {
  os << "a=" << l.a << ",b=" << l.b << ",c=" << l.c;
  return os;
}

template<typename Pair>
Reverse<Pair> & mutant(Pair& p) {
  return reinterpret_cast<Reverse<Pair>&>(p);
}

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  auto p = std::make_pair(3.14, large_struct{});
  LOG(INFO) << "sizeof(p.first)=" << sizeof(p.first) << ", sizeof(p.second)=" << sizeof(p.second) << ", sizeof(p)=" << sizeof(p);
  LOG(INFO) << "p.first=" << p.first << ", p.second=" << p.second;
  LOG(INFO) << "mutate(p).first = " << mutant(p).first << ", mutate(p).second = " << mutant(p).second;
}
