#pragma once

#include "broadcaster_base.h"

namespace events
{

template<typename Event>
class Listener : public ListenerBase
{
public:
	virtual ~Listener() = default;

    virtual void onEvent(const Event& event) = 0;
};

template<typename Event>
class Broadcaster : public BroadcasterBase
{
public:
    using ListenerType = Listener<Event>;

    // NOTES:
	// When Listener is registered during a broadcast, it will not receive the current event.
	// When Listener is unregistered during a broadcast, it will not receive the current event (if not received yet).
	// When Listener is unregistered during a broadcast, it cannot be immediately destroyed (or registered again), it must wait until the broadcast is finished. 

    // THREAD SAFE
	// An unregistered listener can be reqistered only once (it can be unregister, and then registered again). 
    // It is safe to register a listener from any thread, including from within onEvent().
    // void registerListener(ListenerType* listener)

    // THREAD SAFE
	// A registered listener must be unregistered exactly once before it is destroyed. 
    // It is safe to unregister a listener from any thread, including from within onEvent().
    // void unregisterListener(ListenerType* listener);

    // Can only be called by one thread at a time.
	// Can be called parallel to registerListener() and unregisterListener().
    void broadcast(const Event& event)
    {
        ListenerBase* firstListener = nullptr;
        ListenerBase* lastListener = nullptr;
		if (!startBroadcast(firstListener, lastListener))
            return;

        for (auto it = firstListener; true; it = static_cast<ListenerBase*>(it->next()))
        {
            assert(it);
            if (it->status() == ListenerStatus::connected)
            {
                static_cast<ListenerType*>(it)->onEvent(event);
            }

            if (it == lastListener)
				break;
        }

        endBroadcast();
    }
};

} // namespace events