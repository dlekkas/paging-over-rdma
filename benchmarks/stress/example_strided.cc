#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <vector>
#include <chrono>
#include <string>
#include <assert.h>

// computation that invokes swapping activity
int dummy_computation(std::vector<int>& v) {
	auto cnt = 0;
	for (size_t i = 0; i < v.size(); i+=4096) {
		v[i] = i / 4096;
	}
	return cnt;
}

int main(int argc, char** argv) {
	if (argc != 2) {
		std::cerr << "Usage: " << argv[0] << " <num>\n";
		std::exit(1);
	}
	int val = std::stoi(argv[1]);
	int ws_sz = val * 1024 * 4096;
	std::vector<int> vec(ws_sz);

	auto t1 = std::chrono::high_resolution_clock::now();
	dummy_computation(vec);
	auto t2 = std::chrono::high_resolution_clock::now();
	auto elapsed_ms = std::chrono::duration_cast<
		std::chrono::milliseconds>(t2 - t1).count();

	for (size_t i = 0; i < vec.size(); i+=4096) {
		assert(vec[i] == i / 4096);
	}
	std::cout << elapsed_ms << std::endl;
}
