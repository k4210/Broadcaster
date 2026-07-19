#include "broadcaster_base.h"

namespace events
{

ListenerBase::~ListenerBase()
{
	assert(status() == ListenerStatus::disconnected);
}

ListenerStatus ListenerBase::status() const 
{ 
	return m_status.load(std::memory_order_relaxed); 
}

std::ostream& operator<<(std::ostream& os, ListenerStatus status)
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

BroadcasterBase::~BroadcasterBase()
{
    assert(!m_pendingRemove.size());
    assert(!m_listeners.size());
}

void BroadcasterBase::registerListener(ListenerBase* listener)
{
    if (!listener)
        return;

    std::lock_guard lock(m_mutex);

    m_listeners.push_back(listener);
    assert(listener->status() == ListenerStatus::disconnected);
    listener->m_status.store(ListenerStatus::connected, std::memory_order_release);

	listener->onConnect();
}

void BroadcasterBase::unregisterListener(ListenerBase* listener)
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

bool BroadcasterBase::startBroadcast(ListenerBase*& outFirst, ListenerBase*& outLast)
{
    assert(!m_broadcasting);

	std::lock_guard lock(m_mutex);
    assert(!m_pendingRemove.size());

    if (m_listeners.empty())
			return false;

	m_broadcasting = true;

    outFirst = m_listeners.head();
	outLast = m_listeners.tail();

    return true;
}

void BroadcasterBase::endBroadcast()
{
    std::lock_guard lock(m_mutex);
    m_broadcasting = false;
            
    for (auto listener : m_pendingRemove)
    {
        assert(listener->status() == ListenerStatus::ongoing_disconnect);
        disconnect(listener);
    }
    m_pendingRemove.clear();
}

void BroadcasterBase::disconnect(ListenerBase* listener)
{
	m_listeners.remove(listener);
	listener->m_status.store(ListenerStatus::disconnected, std::memory_order_release);
    listener->onDisconnect();
}

} // namespace events