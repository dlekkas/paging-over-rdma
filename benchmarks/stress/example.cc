#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <vector>
#include <chrono>
#include <string>

// computation that invokes swapping activity
int dummy_computation(const std::vector<int>& v) {
	auto cnt = 0;
	for (const auto& val: v) {
		cnt += val + (rand() % 5);
	}
	return cnt;
}

int main(int argc, char** argv) {
	if (argc != 2) {
		std::cerr << "Usage: " << argv[0] << " <MB>" << std::endl;
		std::exit(1);
	}
	int val = std::stoi(argv[1]);
	int ws_sz = (val / 4) * 1024 * 1024;
	std::vector<int> vec(ws_sz);
	srand(time(nullptr));

	auto t1 = std::chrono::high_resolution_clock::now();
	dummy_computation(vec);
	auto t2 = std::chrono::high_resolution_clock::now();
	auto elapsed_ms = std::chrono::duration_cast<
		std::chrono::milliseconds>(t2 - t1).count();
	std::cout << elapsed_ms << std::endl;
}
