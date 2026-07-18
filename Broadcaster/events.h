#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <vector>
#include <assert.h>

namespace events
{
template<typename Event>
class Listener;

template<typename Event>
struct Entry
{
    Listener<Event>* listener = nullptr;
    std::atomic<bool> active{true};
};

template<typename Event>
class Broadcaster;

template<typename Event>
class Listener
{
public:
    virtual ~Listener()
    {
        assert(m_connection == nullptr);
    }

    virtual void onEvent(const Event& event) = 0;

private:
    template<typename>
    friend class Broadcaster;

    Entry<Event>* m_connection = nullptr;
};

template<typename Event>
class Broadcaster
{
private:
    using ListenerType = Listener<Event>;
    using EntryType = Entry<Event>;

public:
    Broadcaster() = default;

    Broadcaster(const Broadcaster&) = delete;
    Broadcaster& operator=(const Broadcaster&) = delete;

    ~Broadcaster()
    {
        cleanup();
        assert(!m_listeners.size());
    }

    void registerListener(ListenerType* listener)
    {
        if (!listener)
            return;

        std::lock_guard lock(m_mutex);

        assert(listener->m_connection == nullptr);

        m_listeners.emplace_back();

        auto iterator = std::prev(m_listeners.end());

        iterator->listener = listener;
        iterator->active.store(
            true,
            std::memory_order_release);

        listener->m_connection = &(*iterator);
    }


    void unregisterListener(ListenerType* listener)
    {
        if (!listener)
            return;

        std::lock_guard lock(m_mutex);

        EntryType* connection = listener->m_connection;

        if (!connection)
            return;

        bool wasActive =
            connection->active.exchange(
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
        typename std::list<EntryType>::iterator firstIterator;
        typename std::list<EntryType>::iterator lastIterator;

        {
            std::lock_guard lock(m_mutex);
            cleanup();

			if (!m_listeners.size())
				return;

            m_broadcasting = true;

            firstIterator = m_listeners.begin();
			lastIterator = std::prev(m_listeners.end());
        }

        auto it = firstIterator;
        while (true)
        {
            if (it->active.load(std::memory_order_acquire))
            {
                // TODO: unsafe to destroy listener here.

                it->listener->onEvent(event);
            }

            if (it == lastIterator)
				break;
            ++it;
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
        for (EntryType* connection : m_pendingRemove)
        {
            // TODO: optimize

            m_listeners.remove_if([connection](const EntryType& entry) {
                return &entry == connection;
            });
        }

        m_pendingRemove.clear();
    }


    std::mutex m_mutex;

    // TODO: optimize - less allocations
    std::list<EntryType> m_listeners;
    std::vector<EntryType*> m_pendingRemove;

    // Protected by m_mutex
    bool m_broadcasting = false;
};

} // namespace events