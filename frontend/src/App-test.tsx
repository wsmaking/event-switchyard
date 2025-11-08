function App() {
  return (
    <div style={{
      padding: '40px',
      backgroundColor: '#1a1a2e',
      color: 'white',
      minHeight: '100vh',
      fontFamily: 'sans-serif'
    }}>
      <h1 style={{ fontSize: '48px', marginBottom: '20px' }}>✅ React is Working!</h1>
      <p style={{ fontSize: '24px', color: '#4ade80' }}>
        If you can see this green text, the frontend is rendering correctly.
      </p>
      <div style={{
        marginTop: '40px',
        padding: '20px',
        backgroundColor: '#0f172a',
        borderRadius: '8px',
        border: '2px solid #3b82f6'
      }}>
        <h2 style={{ color: '#3b82f6' }}>System Status</h2>
        <ul style={{ marginTop: '10px', lineHeight: '2' }}>
          <li>✓ Vite Dev Server: Running</li>
          <li>✓ React: Loaded</li>
          <li>✓ Rendering: Success</li>
        </ul>
      </div>
    </div>
  );
}

export default App;
