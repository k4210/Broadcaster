#pragma once

#include <assert.h>

class IntrusiveListNode
{
public:
    IntrusiveListNode() = default;
    ~IntrusiveListNode()
    {
		assert(!m_prev && !m_next);
    }

    void link_before(IntrusiveListNode* node)
    {
        assert(node);
		assert(!m_prev && !m_next);
        assert(node != this);

        m_prev = node->m_prev;
        m_next = node;
        if (m_prev)
            m_prev->m_next = this;
        node->m_prev = this;
    }

    void link_after(IntrusiveListNode* node)
    {
		assert(node);
		assert(!m_prev && !m_next);
        assert(node != this);

        m_next = node->m_next;
        m_prev = node;
        if (m_next)
            m_next->m_prev = this;
        node->m_next = this;
    }

    void unlink()
    {
        if (m_prev)
            m_prev->m_next = m_next;
        if (m_next)
            m_next->m_prev = m_prev;
        m_prev = nullptr;
        m_next = nullptr;
    }

    IntrusiveListNode* prev() { return m_prev; }
    IntrusiveListNode* next() { return m_next; }
    const IntrusiveListNode* prev() const { return m_prev; }
    const IntrusiveListNode* next() const { return m_next; }

private:
    IntrusiveListNode* m_prev = nullptr;
    IntrusiveListNode* m_next = nullptr;
};

template<typename T>
class IntrusiveList
{
public:
    IntrusiveList() : m_head(nullptr), m_tail(nullptr), m_size(0) {}

    ~IntrusiveList()
    {
        clear();
    }

    void push_back(T* node)
    {
        assert(node);
        assert(!node->prev() && !node->next());

        if (!m_head)
        {
            m_head = node;
            m_tail = node;
            node->unlink();
        }
        else
        {
            node->link_after(m_tail);
            m_tail = node;
        }
        ++m_size;
    }

    void remove(T* node)
    {
        assert(node);
        assert(node->prev() || node == m_head);
        assert(node->next() || node == m_tail);

        if (node == m_head)
            m_head = static_cast<T*>(node->next());
        if (node == m_tail)
            m_tail = static_cast<T*>(node->prev());
        node->unlink();
        --m_size;
    }

    void clear()
    {
        auto it = m_head;
        while (it)
        {
            auto next = static_cast<T*>(it->next());
            remove(it);
            it = next;
        }
    }

    T* begin() { return m_head; }
    T* end() { return nullptr; }
    const T* begin() const { return m_head; }
    const T* end() const { return nullptr; }

    bool empty() const { return m_size == 0; }
    size_t size() const { return m_size; }

	T* head() { return m_head; }
	T* tail() { return m_tail; }

    const T* head() const { return m_head; }
	const T* tail() const { return m_tail; }

private:
    T* m_head;
    T* m_tail;
    size_t m_size;
};