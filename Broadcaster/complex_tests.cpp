#include "gtest/gtest.h"   

#include "broadcaster.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <thread>
#include <vector>

namespace 
{
struct TestEvent
{
    uint64_t id;
};


class CountingListener : public events::Listener<TestEvent>
{
public:

    void onEvent(const TestEvent&) override
    {
        calls.fetch_add(1, std::memory_order_relaxed);
    }

    std::atomic<uint64_t> calls{0};
};


// ------------------------------------------------------------
// Basic concurrent broadcast + unregister
// ------------------------------------------------------------

TEST(BroadcasterThreadTest, ConcurrentBroadcastAndUnregister)
{
    events::Broadcaster<TestEvent> broadcaster;

    constexpr int ListenerCount = 100;

    std::vector<std::unique_ptr<CountingListener>> listeners;

    for (int i = 0; i < ListenerCount; ++i)
    {
        auto listener =
            std::make_unique<CountingListener>();

        broadcaster.registerListener(listener.get());

        listeners.push_back(std::move(listener));
    }


    std::atomic<bool> stop{false};


    std::thread broadcasterThread(
        [&]
        {
            uint64_t id = 0;

            while (!stop.load())
            {
                broadcaster.broadcast({id++});
            }
        });


    std::vector<std::thread> unregisterThreads;


    for (auto& listener : listeners)
    {
        unregisterThreads.emplace_back(
            [&broadcaster, ptr = listener.get()]
            {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(1));

                broadcaster.unregisterListener(ptr);
            });
    }


    for (auto& t : unregisterThreads)
        t.join();


    stop = true;

    broadcasterThread.join();


    for (auto& listener : listeners)
    {
        EXPECT_EQ(
            listener->status(),
            events::ListenerStatus::disconnected);
    }
}


// ------------------------------------------------------------
// Listener unregisters itself from callback
// ------------------------------------------------------------

TEST(BroadcasterThreadTest, SelfUnregisterFromCallback)
{
    events::Broadcaster<TestEvent> broadcaster;


    class SelfRemovingListener :
        public events::Listener<TestEvent>
    {
    public:

        explicit SelfRemovingListener(
            events::Broadcaster<TestEvent>& b)
            : broadcaster(b)
        {
        }


        void onEvent(const TestEvent&) override
        {
            ++calls;

            broadcaster.unregisterListener(this);
        }


        events::Broadcaster<TestEvent>& broadcaster;

        std::atomic<int> calls{0};
    };


    constexpr int Count = 100;


    std::vector<std::unique_ptr<SelfRemovingListener>>
        listeners;


    for (int i = 0; i < Count; ++i)
    {
        auto l =
            std::make_unique<SelfRemovingListener>(
                broadcaster);

        broadcaster.registerListener(l.get());

        listeners.push_back(std::move(l));
    }


    broadcaster.broadcast({1});

    broadcaster.broadcast({2});


    for (auto& listener : listeners)
    {
        EXPECT_EQ(listener->calls.load(), 1);

        EXPECT_EQ(
            listener->status(),
            events::ListenerStatus::disconnected);
    }
}


// ------------------------------------------------------------
// Register during callback
// ------------------------------------------------------------

TEST(BroadcasterThreadTest, RegisterDuringCallback)
{
    events::Broadcaster<TestEvent> broadcaster;


    CountingListener late;


    class RegisteringListener :
        public events::Listener<TestEvent>
    {
    public:

        RegisteringListener(
            events::Broadcaster<TestEvent>& b,
            CountingListener& l)
            :
            broadcaster(b),
            listener(l)
        {
        }


        void onEvent(const TestEvent&) override
        {
            if (!registered)
            {
                registered = true;
                broadcaster.registerListener(&listener);
            }
        }


        events::Broadcaster<TestEvent>& broadcaster;
        CountingListener& listener;

        bool registered = false;
    };


    RegisteringListener first(
        broadcaster,
        late);


    broadcaster.registerListener(&first);


    broadcaster.broadcast({1});


    EXPECT_EQ(
        late.calls.load(),
        0);


    broadcaster.broadcast({2});


    EXPECT_EQ(
        late.calls.load(),
        1);


    broadcaster.unregisterListener(&first);
    broadcaster.unregisterListener(&late);
}


// ------------------------------------------------------------
// Unregister another listener from callback
// ------------------------------------------------------------

TEST(BroadcasterThreadTest, RemoveAnotherListener)
{
    events::Broadcaster<TestEvent> broadcaster;


    CountingListener victim;


    class RemovingListener :
        public events::Listener<TestEvent>
    {
    public:

        RemovingListener(
            events::Broadcaster<TestEvent>& b,
            CountingListener& v)
            :
            broadcaster(b),
            victim(v)
        {
        }


        void onEvent(const TestEvent&) override
        {
            broadcaster.unregisterListener(&victim);
        }


        events::Broadcaster<TestEvent>& broadcaster;
        CountingListener& victim;
    };


    RemovingListener remover(
        broadcaster,
        victim);


    broadcaster.registerListener(&remover);
    broadcaster.registerListener(&victim);


    broadcaster.broadcast({1});


    EXPECT_EQ(
        victim.calls.load(),
        0);


    EXPECT_EQ(
        victim.status(),
        events::ListenerStatus::disconnected);


    broadcaster.unregisterListener(&remover);
}


// ------------------------------------------------------------
// Many register/unregister cycles
// ------------------------------------------------------------

TEST(BroadcasterThreadTest, RegisterUnregisterStress)
{
    events::Broadcaster<TestEvent> broadcaster;


    constexpr int ListenerCount = 64;
    constexpr int Iterations = 10000;


    std::vector<std::unique_ptr<CountingListener>>
        listeners;


    for (int i = 0; i < ListenerCount; ++i)
    {
        listeners.push_back(
            std::make_unique<CountingListener>());
    }


    std::atomic<bool> stop{false};


    std::thread broadcasterThread(
        [&]
        {
            uint64_t id = 0;

            while (!stop.load())
            {
                broadcaster.broadcast({id++});
            }
        });



    std::vector<std::thread> workers;


    for (int i = 0; i < ListenerCount; ++i)
    {
        workers.emplace_back(
            [&broadcaster,
             ptr = listeners[i].get()]
            {
                for (int n = 0; n < Iterations; ++n)
                {
                    while (ptr->status() == events::ListenerStatus::ongoing_disconnect)
                    {
                        std::this_thread::yield();
                    }
                    broadcaster.registerListener(ptr);

                    std::this_thread::yield();

                    broadcaster.unregisterListener(ptr);
                }
            });
    }


    for (auto& t : workers)
        t.join();


    stop = true;

    broadcasterThread.join();


    for (auto& listener : listeners)
    {
        EXPECT_EQ(
            listener->status(),
            events::ListenerStatus::disconnected);
    }
}


// ------------------------------------------------------------
// Randomized lifecycle stress
// ------------------------------------------------------------

TEST(BroadcasterThreadTest, RandomizedStress)
{
    events::Broadcaster<TestEvent> broadcaster;


    constexpr int ListenerCount = 128;


    std::vector<std::unique_ptr<CountingListener>>
        listeners;


    for (int i = 0; i < ListenerCount; ++i)
    {
        listeners.push_back(
            std::make_unique<CountingListener>());
    }


    std::atomic<bool> stop{false};


    std::thread broadcasterThread(
        [&]
        {
            uint64_t id = 0;

            while (!stop.load())
                broadcaster.broadcast({id++});
        });


    std::vector<std::thread> workers;


    for (int t = 0; t < 1; ++t)
    {
        workers.emplace_back(
            [&, seed = t]
            {
                std::mt19937 rng(seed);


                for (int i = 0; i < 50000; ++i)
                {
                    auto index =
                        rng() % ListenerCount;


                    auto* listener =
                        listeners[index].get();


                    if (listener->status() ==
                        events::ListenerStatus::disconnected)
                    {
                        broadcaster.registerListener(listener);
                    }
                    else if (listener->status() ==
                             events::ListenerStatus::connected)
                    {
                        broadcaster.unregisterListener(listener);
                    }
                }
            });
    }


    for (auto& t : workers)
        t.join();


    stop = true;

    broadcasterThread.join();


    for (auto& listener : listeners)
    {
        if (listener->status() ==
            events::ListenerStatus::connected)
        {
            broadcaster.unregisterListener(listener.get());
        }


        EXPECT_EQ(
            listener->status(),
            events::ListenerStatus::disconnected);
    }
}
}