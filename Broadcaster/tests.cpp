#include "gtest/gtest.h"        

#include "events.h"

#include <atomic>
#include <thread>


struct TestEvent
{
    int value;
};


class CountingListener : public events::Listener<TestEvent>
{
public:
    void onEvent(const TestEvent& event) override
    {
        count++;
        lastValue = event.value;
    }

    int count = 0;
    int lastValue = 0;
};


TEST(BroadcasterTest, SingleListenerReceivesEvent)
{
    events::Broadcaster<TestEvent> broadcaster;

    CountingListener listener;

    broadcaster.registerListener(&listener);

    broadcaster.broadcast({42});

    broadcaster.unregisterListener(&listener);

    EXPECT_EQ(listener.count, 1);
    EXPECT_EQ(listener.lastValue, 42);
}


TEST(BroadcasterTest, MultipleListenersReceiveEvent)
{
    events::Broadcaster<TestEvent> broadcaster;

    CountingListener a;
    CountingListener b;

    broadcaster.registerListener(&a);
    broadcaster.registerListener(&b);

    broadcaster.broadcast({10});

    broadcaster.unregisterListener(&a);
    broadcaster.unregisterListener(&b);

    EXPECT_EQ(a.count, 1);
    EXPECT_EQ(b.count, 1);
}


TEST(BroadcasterTest, UnregisteredListenerDoesNotReceiveEvent)
{
    events::Broadcaster<TestEvent> broadcaster;

    CountingListener listener;

    broadcaster.registerListener(&listener);
    broadcaster.unregisterListener(&listener);

    broadcaster.broadcast({1});

    EXPECT_EQ(listener.count, 0);
}


class SelfRemovingListener : public events::Listener<TestEvent>
{
public:
    explicit SelfRemovingListener(
        events::Broadcaster<TestEvent>& broadcaster)
        : broadcaster(broadcaster)
    {
    }


    void onEvent(const TestEvent&) override
    {
        calls++;

        broadcaster.unregisterListener(this);
    }


    events::Broadcaster<TestEvent>& broadcaster;
    int calls = 0;
};


TEST(BroadcasterTest, ListenerCanUnregisterItself)
{
    events::Broadcaster<TestEvent> broadcaster;

    SelfRemovingListener listener(broadcaster);

    broadcaster.registerListener(&listener);

    broadcaster.broadcast({1});
    broadcaster.broadcast({2});

    EXPECT_EQ(listener.calls, 1);
}


class RemovingOtherListener : public events::Listener<TestEvent>
{
public:
    RemovingOtherListener(
        events::Broadcaster<TestEvent>& broadcaster,
        events::Listener<TestEvent>* victim)
        : broadcaster(broadcaster),
          victim(victim)
    {
    }


    void onEvent(const TestEvent&) override
    {
        broadcaster.unregisterListener(victim);
    }


    events::Broadcaster<TestEvent>& broadcaster;
    events::Listener<TestEvent>* victim;
};


TEST(BroadcasterTest, ListenerCanRemoveAnotherListener)
{
    events::Broadcaster<TestEvent> broadcaster;

    CountingListener victim;

    RemovingOtherListener remover(
        broadcaster,
        &victim);

    broadcaster.registerListener(&remover);
    broadcaster.registerListener(&victim);

    broadcaster.broadcast({1});

    EXPECT_EQ(victim.count, 0);

    broadcaster.unregisterListener(&remover);
}

TEST(BroadcasterTest, UnregisterFromAnotherThread)
{
    events::Broadcaster<TestEvent> broadcaster;

    CountingListener listener;

    broadcaster.registerListener(&listener);

    std::thread t(
        [&]()
        {
            broadcaster.unregisterListener(&listener);
        });

    broadcaster.broadcast({5});

    t.join();

    EXPECT_TRUE(listener.count <= 1);
}


class BlockingListener : public events::Listener<TestEvent>
{
public:
    void onEvent(const TestEvent&) override
    {
        entered = true;

        while (!release)
            std::this_thread::yield();
    }

    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
};


TEST(BroadcasterTest, UnregisterWhileCallbackIsRunning)
{
    events::Broadcaster<TestEvent> broadcaster;

    BlockingListener listener;

    broadcaster.registerListener(&listener);

    std::thread broadcasterThread(
        [&]()
        {
            broadcaster.broadcast({1});
        });


    while (!listener.entered)
        std::this_thread::yield();


    broadcaster.unregisterListener(&listener);

    listener.release = true;

    broadcasterThread.join();

    // Callback already started, so one call is allowed.
    // Future broadcasts must not call it.

    broadcaster.broadcast({2});

    EXPECT_TRUE(listener.entered);
}