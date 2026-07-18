#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <vector>
#include <assert.h>

namespace events
{

template<typename Event>
struct Entry;

template<typename Event>
class Listener;

template<typename Event>
struct Connection
{
    typename std::list<Entry<Event>>::iterator iterator;
};

template<typename Event>
struct Entry
{
    Listener<Event>* listener = nullptr;
    Connection<Event> connection;
    std::atomic<bool> active{true};
};

template<typename Event>
class Broadcaster;


template<typename Event>
class Listener
{
public:
    virtual ~Listener() = default;

    virtual void onEvent(const Event& event) = 0;

private:
    template<typename>
    friend class Broadcaster;

    Connection<Event>* m_connection = nullptr;
};


template<typename Event>
class Broadcaster
{
private:
    using ListenerType = Listener<Event>;
    using ConnectionType = Connection<Event>;
    using EntryType = Entry<Event>;


public:
    Broadcaster() = default;

    Broadcaster(const Broadcaster&) = delete;
    Broadcaster& operator=(const Broadcaster&) = delete;

    ~Broadcaster()
    {
        cleanup();
        assert(!m_listeners.size());
        assert(!m_pendingRemove.size());
    }


    void registerListener(ListenerType* listener)
    {
        if (!listener)
            return;

        std::lock_guard lock(m_mutex);

        m_listeners.emplace_back();

        auto iterator = std::prev(m_listeners.end());

        iterator->listener = listener;
        iterator->connection.iterator = iterator;
        iterator->active.store(
            true,
            std::memory_order_release);

        listener->m_connection = &iterator->connection;
    }


    void unregisterListener(ListenerType* listener)
    {
        if (!listener)
            return;

        std::lock_guard lock(m_mutex);

        ConnectionType* connection = listener->m_connection;

        if (!connection)
            return;

        bool wasActive =
            connection->iterator->active.exchange(
                false,
                std::memory_order_acq_rel);

        listener->m_connection = nullptr;

        if (wasActive)
        {
            m_pendingRemove.push_back(connection);
        }

        if (!m_broadcasting)
            cleanup();
    }


    void broadcast(const Event& event)
    {
        std::vector<ConnectionType*> snapshot;

        {
            std::lock_guard lock(m_mutex);

            m_broadcasting = true;

            snapshot.reserve(m_listeners.size());

            for (auto& entry : m_listeners)
            {
                if (entry.active.load(
                        std::memory_order_acquire))
                {
                    snapshot.push_back(
                        &entry.connection);
                }
            }
        }


        for (ConnectionType* connection : snapshot)
        {
            if (connection->iterator->active.load(
                    std::memory_order_acquire))
            {
                // Entry is still alive because cleanup is delayed
                connection->iterator->listener
                    ->onEvent(event);
            }
        }


        {
            std::lock_guard lock(m_mutex);

            m_broadcasting = false;
            cleanup();
        }
    }

private:

    void cleanup()
    {
        for (ConnectionType* connection : m_pendingRemove)
        {
            m_listeners.erase(connection->iterator);
        }

        m_pendingRemove.clear();
    }


    std::mutex m_mutex;

    std::list<EntryType> m_listeners;
    std::vector<ConnectionType*> m_pendingRemove;

    // Protected by m_mutex
    bool m_broadcasting = false;
};

} // namespace events