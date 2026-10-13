import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'

import { api } from './api/client'
import styles from './App.module.css'

export function App() {
  const [selectedUser, setSelectedUser] = useState<number | null>(null)
  const users = useQuery({ queryKey: ['users'], queryFn: api.users })

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
    </main>
  )
}
