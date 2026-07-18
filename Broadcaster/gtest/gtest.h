#ifndef GTEST_GTEST_H
#define GTEST_GTEST_H

#include <vector>
#include <functional>
#include <iostream>
#include <string>
#include <sstream>

namespace testing {

struct TestInfo { std::function<void()> fn; const char* suite; const char* name; };

inline std::vector<TestInfo>& GetTests() {
	static std::vector<TestInfo> tests;
	return tests;
}

inline void RegisterTest(const char* suite, const char* name, std::function<void()> fn) {
	GetTests().push_back({fn, suite, name});
}

template<typename T>
inline std::string PrintValue(const T& v) { std::ostringstream oss; oss << v; return oss.str(); }

static thread_local bool current_test_failed = false;

inline bool RunAllTests() {
	int failed = 0;
	for (auto &t : GetTests()) {
		current_test_failed = false;
		std::cout << "[ RUN ] " << t.suite << "." << t.name << "\n";
		t.fn();
		if (current_test_failed) {
			std::cout << "[ FAILED ] " << t.suite << "." << t.name << "\n";
			failed++;
		} else {
			std::cout << "[       OK ] " << t.suite << "." << t.name << "\n";
		}
	}
	return failed == 0;
}

} // namespace testing

#define EXPECT_EQ(a,b) \
	do { auto _a = (a); auto _b = (b); if (!((_a) == (_b))) { std::cerr << "EXPECT_EQ failed: " << #a << " vs " << #b << " (" << ::testing::PrintValue(_a) << " vs " << ::testing::PrintValue(_b) << ")\n"; ::testing::current_test_failed = true; } } while(0)

#define EXPECT_TRUE(x) \
	do { if (!(x)) { std::cerr << "EXPECT_TRUE failed: " << #x << "\n"; ::testing::current_test_failed = true; } } while(0)

#define TEST(suite,name) \
	static void GTest_##suite##_##name(); \
	namespace { const int gtest_reg_##suite##_##name = ([](){ ::testing::RegisterTest(#suite, #name, &GTest_##suite##_##name); return 0; })(); } \
	static void GTest_##suite##_##name()

#endif // GTEST_GTEST_H
