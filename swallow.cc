#include <iostream>
template<typename... Args>
void swallow(Args &&...) {
}

template<typename... Args>
void foo(Args... args) {
  swallow((std::cout << args << " ", 0)...);
}

int main(int argc, char* argv[]) {
  foo(1,2,3,4);
}
