import styles from './App.module.css'

export function App() {
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
      </header>
    </main>
  )
}
