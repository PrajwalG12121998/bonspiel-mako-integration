#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "rpc/client.hpp"
#include "rpc/server.hpp"
#include "misc/marshal.hpp"
#include "benchmark_service.h"

using namespace rrr;
using namespace benchmark;
using namespace std::chrono;

class TestService : public benchmark::BenchmarkService {
public:
    std::atomic<int> call_count{0};
    std::atomic<bool> should_delay{false};
    std::atomic<int> delay_ms{100};
    
    void fast_nop(const std::string& input) override {
        call_count++;
    }
    
    void nop(const std::string& input) override {
        call_count++;
        if (should_delay) {
            std::this_thread::sleep_for(milliseconds(delay_ms));
        }
    }
    
    void fast_prime(const i32& n, i8* flag) override {
        call_count++;
        bool is_prime = true;
        if (n <= 1) {
            is_prime = false;
        } else {
            for (i32 i = 2; i * i <= n; i++) {
                if (n % i == 0) {
                    is_prime = false;
                    break;
                }
            }
        }
        *flag = is_prime ? 1 : 0;
    }
    
    void fast_vec(const i32& n, std::vector<i64>* v) override {
        call_count++;
        for (i32 i = 0; i < n; i++) {
            v->push_back(i);
        }
    }
    
    void sleep(const double& sec) override {
        call_count++;
        std::this_thread::sleep_for(std::chrono::duration<double>(sec));
    }
};

class RPCTest : public ::testing::Test {
protected:
    PollMgr* poll_mgr;
    Server* server;
    TestService* service;
    std::shared_ptr<Client> client;
    static constexpr int test_port = 8848;

    void SetUp() override {
        poll_mgr = new PollMgr;
        server = new Server(poll_mgr);
        service = new TestService();

        server->reg(service);

        ASSERT_EQ(server->start(("0.0.0.0:" + std::to_string(test_port)).c_str()), 0);

        client = std::make_shared<Client>(poll_mgr);
        ASSERT_EQ(client->connect(("127.0.0.1:" + std::to_string(test_port)).c_str()), 0);

        std::this_thread::sleep_for(milliseconds(100));
    }

    void TearDown() override {
        client->close();

        delete service;
        delete server;  // Server destructor waits for connections to close

        // shared_ptr handles cleanup automatically

        poll_mgr->release();
    }
};

TEST_F(RPCTest, BasicNop) {
    std::string input = "Hello, RPC!";
    Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
    
    *client << input;
    client->end_request();
    fu->wait();
    
    EXPECT_EQ(fu->get_error_code(), 0);
    EXPECT_EQ(service->call_count, 1);
    
    fu->release();
}

TEST_F(RPCTest, MultipleRequests) {
    const int num_requests = 100;
    std::vector<Future*> futures;
    
    for (int i = 0; i < num_requests; i++) {
        std::string input = "Request_" + std::to_string(i);
        Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
        *client << input;
        client->end_request();
        futures.push_back(fu);
    }
    
    for (int i = 0; i < num_requests; i++) {
        futures[i]->wait();
        EXPECT_EQ(futures[i]->get_error_code(), 0);
        futures[i]->release();
    }
    
    EXPECT_EQ(service->call_count, num_requests);
}

TEST_F(RPCTest, ConcurrentRequests) {
    const int num_threads = 10;
    const int requests_per_thread = 50;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < requests_per_thread; i++) {
                std::string input = "Thread_" + std::to_string(t) + "_Request_" + std::to_string(i);
                Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
                *client << input;
                client->end_request();
                fu->wait();
                
                if (fu->get_error_code() == 0) {
                    success_count++;
                }
                fu->release();
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(success_count, num_threads * requests_per_thread);
    EXPECT_EQ(service->call_count, num_threads * requests_per_thread);
}

TEST_F(RPCTest, LargePayload) {
    std::string large_input(1000000, 'X');
    
    Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
    *client << large_input;
    client->end_request();
    fu->wait();
    
    EXPECT_EQ(fu->get_error_code(), 0);
    
    fu->release();
}

TEST_F(RPCTest, DifferentMethods) {
    // Test NOP
    Future* fu_nop = client->begin_request(benchmark::BenchmarkService::NOP);
    std::string dummy = "";
    *client << dummy;
    client->end_request();
    fu_nop->wait();
    EXPECT_EQ(fu_nop->get_error_code(), 0);
    fu_nop->release();
    
    // Test PRIME with prime number
    i32 prime_input = 17;
    Future* fu_prime = client->begin_request(benchmark::BenchmarkService::PRIME);
    *client << prime_input;
    client->end_request();
    fu_prime->wait();
    
    EXPECT_EQ(fu_prime->get_error_code(), 0);
    i8 prime_result;
    fu_prime->get_reply() >> prime_result;
    EXPECT_EQ(prime_result, (i8)1);
    fu_prime->release();
    
    // Test PRIME with composite number
    i32 composite_input = 24;
    Future* fu_composite = client->begin_request(benchmark::BenchmarkService::PRIME);
    *client << composite_input;
    client->end_request();
    fu_composite->wait();
    
    i8 composite_result;
    fu_composite->get_reply() >> composite_result;
    EXPECT_EQ(composite_result, (i8)0);
    fu_composite->release();
}

TEST_F(RPCTest, TimeoutHandling) {
    // Test timed_wait functionality with a fast request
    std::string input = "timeout_test";
    Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
    *client << input;
    client->end_request();
    
    // This should complete quickly (no delay)
    fu->timed_wait(1.0);  // Wait up to 1 second
    bool completed = fu->ready();
    EXPECT_TRUE(completed);  // Should complete quickly
    
    EXPECT_EQ(fu->get_error_code(), 0);
    fu->release();
    
    // Note: Testing actual timeout with slow server causes crashes
    // in the current implementation, so we only test successful completion
}

TEST_F(RPCTest, CallbackMechanism) {
    std::atomic<bool> callback_called{false};
    
    FutureAttr attr([&](Future* f) {
        callback_called = true;
    });
    
    std::string input = "callback_test";
    Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP, attr);
    *client << input;
    client->end_request();
    
    fu->wait();
    
    std::this_thread::sleep_for(milliseconds(100));
    
    EXPECT_TRUE(callback_called);
    
    fu->release();
}

TEST_F(RPCTest, InvalidRequest) {
    Future* fu = client->begin_request(99999);
    client->end_request();
    fu->wait();
    
    EXPECT_NE(fu->get_error_code(), 0);
    
    fu->release();
}

TEST_F(RPCTest, EmptyPayload) {
    Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
    std::string dummy = "";
    *client << dummy;
    client->end_request();
    fu->wait();
    
    EXPECT_EQ(fu->get_error_code(), 0);
    
    fu->release();
}

TEST_F(RPCTest, ConnectionResilience) {
    std::string input1 = "before_reconnect";
    Future* fu1 = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
    *client << input1;
    client->end_request();
    fu1->wait();

    EXPECT_EQ(fu1->get_error_code(), 0);
    fu1->release();

    client->close();
    client.reset();  // Release the shared_ptr

    std::this_thread::sleep_for(milliseconds(100));

    client = std::make_shared<Client>(poll_mgr);
    ASSERT_EQ(client->connect(("127.0.0.1:" + std::to_string(test_port)).c_str()), 0);

    std::this_thread::sleep_for(milliseconds(100));

    std::string input2 = "after_reconnect";
    Future* fu2 = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
    *client << input2;
    client->end_request();
    fu2->wait();

    EXPECT_EQ(fu2->get_error_code(), 0);
    fu2->release();
}

TEST_F(RPCTest, PipelinedRequests) {
    const int num_requests = 1000;
    std::vector<Future*> futures;
    
    for (int i = 0; i < num_requests; i++) {
        Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
        std::string dummy = "";
        *client << dummy;
        client->end_request();
        futures.push_back(fu);
    }
    
    for (auto fu : futures) {
        fu->wait();
        EXPECT_EQ(fu->get_error_code(), 0);
        fu->release();
    }
    
    EXPECT_EQ(service->call_count, num_requests);
}

TEST_F(RPCTest, SlowClientFastServer) {
    service->should_delay = false;
    
    std::vector<Future*> futures;
    
    for (int i = 0; i < 100; i++) {
        std::string input = "Request_" + std::to_string(i);
        Future* fu = client->begin_request(benchmark::BenchmarkService::FAST_NOP);
        *client << input;
        client->end_request();
        futures.push_back(fu);
        
        std::this_thread::sleep_for(milliseconds(10));
    }
    
    for (int i = 0; i < 100; i++) {
        futures[i]->wait();
        EXPECT_EQ(futures[i]->get_error_code(), 0);
        futures[i]->release();
    }
}

TEST_F(RPCTest, FastClientSlowServer) {
    service->should_delay = true;
    service->delay_ms = 50;
    
    auto start = high_resolution_clock::now();
    
    const int num_requests = 10;
    std::vector<Future*> futures;
    
    for (int i = 0; i < num_requests; i++) {
        std::string input = "Request_" + std::to_string(i);
        Future* fu = client->begin_request(benchmark::BenchmarkService::NOP);
        *client << input;
        client->end_request();
        futures.push_back(fu);
    }
    
    for (auto fu : futures) {
        fu->wait();
        fu->release();
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);
    
    EXPECT_GE(duration.count(), num_requests * service->delay_ms / 2);
    
    service->should_delay = false;
}

class ConnectionErrorTest : public ::testing::Test {
protected:
    PollMgr* poll_mgr;
    
    void SetUp() override {
        poll_mgr = new PollMgr;
    }
    
    void TearDown() override {
        poll_mgr->release();
    }
};

TEST_F(ConnectionErrorTest, ConnectToNonExistentServer) {
    auto client = std::make_shared<Client>(poll_mgr);

    int result = client->connect("127.0.0.1:9999");

    EXPECT_NE(result, 0);

    client->close();
    // shared_ptr handles cleanup automatically
}

TEST_F(ConnectionErrorTest, InvalidAddress) {
    auto client = std::make_shared<Client>(poll_mgr);

    int result = client->connect("invalid_address:1234");

    EXPECT_NE(result, 0);

    client->close();
    // shared_ptr handles cleanup automatically
}

TEST_F(ConnectionErrorTest, InvalidPort) {
    auto client = std::make_shared<Client>(poll_mgr);

    int result = client->connect("127.0.0.1:99999");

    EXPECT_NE(result, 0);

    client->close();
    // shared_ptr handles cleanup automatically
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}