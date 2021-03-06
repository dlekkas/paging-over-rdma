#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <vector>

int main(void) {
	constexpr int sz = 80 * 1024 * 1024; // 400 MB
	std::vector<int> vec(sz);

	srand(time(nullptr));
	int cnt = 0;
	for (int i = 0; i < sz; i++) {
		cnt += vec[i] + (rand() % 5);
	}
	std::cout << cnt << std::endl;
}
