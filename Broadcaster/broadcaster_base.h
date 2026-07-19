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

std::ostream& operator<<(std::ostream& os, ListenerStatus status);

class ListenerBase : public IntrusiveListNode
{
public:
	virtual ~ListenerBase();

    ListenerStatus status() const;

protected:
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
    friend class BroadcasterBase;

	std::atomic<ListenerStatus> m_status = ListenerStatus::disconnected;
};

class BroadcasterBase
{
public:
    BroadcasterBase() = default;
    BroadcasterBase(const BroadcasterBase&) = delete;
    BroadcasterBase& operator=(const BroadcasterBase&) = delete;
    ~BroadcasterBase();

    void registerListener(ListenerBase* listener);
    void unregisterListener(ListenerBase* listener);

protected:
	bool startBroadcast(ListenerBase*& outFirst, ListenerBase*& outLast);
	void endBroadcast();
	void disconnect(ListenerBase* listener);

    std::mutex m_mutex;
    IntrusiveList<ListenerBase> m_listeners;
    std::vector<ListenerBase*> m_pendingRemove;
    bool m_broadcasting = false;
};

} // namespace events