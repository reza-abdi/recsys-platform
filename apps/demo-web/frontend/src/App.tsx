import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'

import { api } from './api/client'
import styles from './App.module.css'

export function App() {
  const [selectedUser, setSelectedUser] = useState<number | null>(null)
  const users = useQuery({ queryKey: ['users'], queryFn: api.users })
  const products = useQuery({
    queryKey: ['products'],
    queryFn: () => api.products(0),
  })

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
              </div>
            </article>
          ))}
        </div>
      </section>
    </main>
  )
}
