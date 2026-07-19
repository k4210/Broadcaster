#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <assert.h>
#include <ostream>
#include "intrusive_list.h"

namespace events
{

enum class ListenerStatus : uint8_t
{
    disconnected, // wont receive events, can be registered
    connected, // will receive events
    ongoing_disconnect, // wont receive events, cannot be registered yet, still likned to broadcaster
};

inline std::ostream& operator<<(std::ostream& os, ListenerStatus status)
{
    switch (status)
    {
    case ListenerStatus::disconnected:
        return os << "disconnected";
    case ListenerStatus::connected:
        return os << "connected";
    case ListenerStatus::ongoing_disconnect:
        return os << "ongoing_disconnect";
    }
    return os << "unknown";
}

template<typename Event>
class Broadcaster;

template<typename Event>
class Listener : public IntrusiveListNode
{
public:
	virtual ~Listener()
	{
	    assert(status() == ListenerStatus::disconnected);
	}

    ListenerStatus status() const { return m_status.load(std::memory_order_relaxed); }

protected:
    virtual void onEvent(const Event& event) = 0;

	// Keep them short, they are called under lock.
    virtual void onConnect() 
    {
        // add ref
    }
	
    virtual void onDisconnect() 
    {
		// remove ref
    }

private:
    friend Broadcaster<Event>;

	std::atomic<ListenerStatus> m_status = ListenerStatus::disconnected;
};

template<typename Event>
class Broadcaster
{
private:
    using ListenerType = Listener<Event>;

public:
    Broadcaster() = default;

    Broadcaster(const Broadcaster&) = delete;
    Broadcaster& operator=(const Broadcaster&) = delete;

    ~Broadcaster()
    {
        assert(!m_pendingRemove.size());
        assert(!m_listeners.size());
    }

    // NOTES:
	// When Listener is registered during a broadcast, it will not receive the current event.
	// When Listener is unregistered during a broadcast, it will not receive the current event (if not received yet).
	// When Listener is unregistered during a broadcast, it cannot be immediately destroyed (or registered again), it must wait until the broadcast is finished. 

    // THREAD SAFE
	// An unregistered listener can be reqistered only once (it can be unregister, and then registered again). 
    // It is safe to register a listener from any thread, including from within onEvent().
    void registerListener(ListenerType* listener)
    {
        if (!listener)
            return;

        std::lock_guard lock(m_mutex);

        m_listeners.push_back(listener);
        assert(listener->status() == ListenerStatus::disconnected);
        listener->m_status.store(ListenerStatus::connected, std::memory_order_release);

		listener->onConnect();
    }

    // THREAD SAFE
	// A registered listener must be unregistered exactly once before it is destroyed. 
    // It is safe to unregister a listener from any thread, including from within onEvent().
    void unregisterListener(ListenerType* listener)
    {
        if (!listener)
            return;

        std::lock_guard lock(m_mutex);

        assert(listener->status() == ListenerStatus::connected);

        if (m_broadcasting)
        {
            listener->m_status.store(ListenerStatus::ongoing_disconnect, std::memory_order_release);
            m_pendingRemove.push_back(listener);
        }
        else
        {
			disconnect(listener);
        }
    }

    // Can only be called by one thread at a time.
	// Can be called parallel to registerListener() and unregisterListener().
    void broadcast(const Event& event)
    {
		assert(!m_broadcasting);

        ListenerType* firstListener;
        ListenerType* lastListener;

        {
			std::lock_guard lock(m_mutex);
			assert(!m_pendingRemove.size());

			if (m_listeners.empty())
				return;

            assert(!m_broadcasting);
			m_broadcasting = true;

			firstListener = m_listeners.head();
			lastListener = m_listeners.tail();
        }

        for (auto it = firstListener; true; it = static_cast<ListenerType*>(it->next()))
        {
            assert(it);
            if (it->status() == ListenerStatus::connected)
            {
                it->onEvent(event);
            }

            if (it == lastListener)
				break;
        }

        {
            std::lock_guard lock(m_mutex);
            m_broadcasting = false;
            
            for (ListenerType* listener : m_pendingRemove)
            {
                assert(listener->status() == ListenerStatus::ongoing_disconnect);
                disconnect(listener);
            }
            m_pendingRemove.clear();
        }
    }

private:
	void disconnect(ListenerType* listener)
	{
		m_listeners.remove(listener);
		listener->m_status.store(ListenerStatus::disconnected, std::memory_order_release);
        listener->onDisconnect();
	}

    std::mutex m_mutex;
    IntrusiveList<ListenerType> m_listeners;
    std::vector<ListenerType*> m_pendingRemove;
    bool m_broadcasting = false;
};

} // namespace events