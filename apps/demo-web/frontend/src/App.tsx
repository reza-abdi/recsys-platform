import { useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'

import { api, type Action, type Product } from './api/client'
import styles from './App.module.css'

function sessionId(): string {
  const key = 'recsys-demo-session-id'
  const existing = window.localStorage.getItem(key)
  if (existing) return existing
  const created = `web-session-${crypto.randomUUID()}`
  window.localStorage.setItem(key, created)
  return created
}

export function App() {
  const [selectedUser, setSelectedUser] = useState<number | null>(null)
  const [, setTimeline] = useState<
    Array<{ id: string; label: string; status: 'accepted' | 'waiting' | 'failed' }>
  >([])
  const currentSession = useMemo(sessionId, [])
  const users = useQuery({ queryKey: ['users'], queryFn: api.users })
  const products = useQuery({
    queryKey: ['products'],
    queryFn: () => api.products(0),
  })

  const updateTimeline = (
    id: string,
    status: 'accepted' | 'waiting' | 'failed',
  ) => {
    setTimeline((current) =>
      current.map((event) => (event.id === id ? { ...event, status } : event)),
    )
  }

  const sendAction = async (
    product: Product,
    action: Action,
    correlation?: { requestId: string; impressionId: string },
  ) => {
    if (selectedUser === null) {
      setTimeline((current) => [
        {
          id: crypto.randomUUID(),
          label: 'Select a user before sending an event',
          status: 'failed',
        },
        ...current,
      ])
      return
    }
    const temporaryId = crypto.randomUUID()
    setTimeline((current) => [
      {
        id: temporaryId,
        label: `${action.toUpperCase()} · ${product.product_name}`,
        status: 'waiting',
      },
      ...current,
    ])
    try {
      const accepted = await api.event(
        {
          user_id: selectedUser,
          product_id: product.product_id,
          action,
          session_id: currentSession,
          request_id: correlation?.requestId,
          impression_id: correlation?.impressionId,
          quantity: 1,
        },
        `web-${temporaryId}`,
      )
      setTimeline((current) =>
        current.map((event) =>
          event.id === temporaryId
            ? { ...event, id: accepted.event_id, status: 'accepted' }
            : event,
        ),
      )
    } catch {
      updateTimeline(temporaryId, 'failed')
    }
  }

  return (
    <main className={styles.shell}>
      <header className={styles.header}>
        <div>
          <p className={styles.eyebrow}>LIVE PERSONALIZATION LAB</p>
          <h1>RecSys Store</h1>
          <p className={styles.subtitle}>
            Every interaction becomes a realtime feature.
          </p>
        </div>
        <div className={styles.controls}>
          <label>
            Active user
            <select
              aria-label="Active user"
              value={selectedUser ?? ''}
              onChange={(event) => setSelectedUser(Number(event.target.value))}
            >
              <option value="" disabled>
                Choose a user
              </option>
              {users.data?.items.map((user) => (
                <option key={user.user_id} value={user.user_id}>
                  User {user.user_id} · {user.segment ?? 'active'}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>

      <section aria-labelledby="catalog-title">
        <div className={styles.sectionHeading}>
          <div>
            <p className={styles.eyebrow}>LIVE POSTGRES CATALOG</p>
            <h2 id="catalog-title">Popular products</h2>
          </div>
          <span>{products.data?.total ?? 0} active</span>
        </div>
        {products.isLoading && <p>Loading live catalog…</p>}
        {products.error && (
          <p className={styles.error}>{products.error.message}</p>
        )}
        <div className={styles.productGrid}>
          {products.data?.items.map((product) => (
            <article className={styles.productCard} key={product.product_id}>
              <div className={styles.productVisual} aria-hidden="true">
                <span>
                  {product.category_code ?? `CAT ${product.category_id}`}
                </span>
              </div>
              <div className={styles.productBody}>
                <p>{product.brand_name ?? `Brand ${product.brand_id}`}</p>
                <h3>{product.product_name}</h3>
                <strong>${product.current_price.toFixed(2)}</strong>
                <div className={styles.actions}>
                  <button onClick={() => void sendAction(product, 'view')}>
                    View
                  </button>
                </div>
              </div>
            </article>
          ))}
        </div>
      </section>
    </main>
  )
}
