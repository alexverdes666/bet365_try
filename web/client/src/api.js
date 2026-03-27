import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json'
  }
});

// Attach token to every request
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Handle 401 responses
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      // Only redirect if not already on login page
      if (window.location.pathname !== '/login') {
        // Don't auto-redirect, let the auth context handle it
      }
    }
    return Promise.reject(error);
  }
);

export default api;

// Utility: convert odds to decimal (handles both fractional "5/6" and already-decimal "1.83")
export function fractionalToDecimal(odds) {
  if (!odds) return null;
  if (typeof odds === 'number') return odds;
  const s = String(odds).trim();
  if (!s) return null;
  if (s.includes('/')) {
    const parts = s.split('/');
    const num = parseFloat(parts[0]);
    const den = parseFloat(parts[1]);
    if (den === 0) return null;
    return (num / den) + 1;
  }
  const val = parseFloat(s);
  return isNaN(val) ? null : val;
}

// Utility: format odds for display
export function formatOdds(odds) {
  const decimal = fractionalToDecimal(odds);
  if (decimal === null) return '-';
  return decimal.toFixed(2);
}

// Utility: parse timestamp "20260324091611" to Date
export function parseTimestamp(ts) {
  if (!ts || ts.length < 14) return null;
  const year = ts.substring(0, 4);
  const month = ts.substring(4, 6);
  const day = ts.substring(6, 8);
  const hour = ts.substring(8, 10);
  const minute = ts.substring(10, 12);
  const second = ts.substring(12, 14);
  return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}`);
}

// Utility: format time for display
export function formatTime(ts) {
  const date = parseTimestamp(ts);
  if (!date) return '--:--';
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

// Sport icon mapping (English + Spanish names from bet365.es)
export function getSportIcon(sportName) {
  if (!sportName) return '\uD83C\uDFC6';
  const n = sportName.toLowerCase();
  if (n.includes('futbol') || n.includes('f\u00FAtbol') || n.includes('soccer') || n.includes('football')) return '\u26BD';
  if (n.includes('balon') || n.includes('basket')) return '\uD83C\uDFC0';
  if (n.includes('tenis') && !n.includes('mesa')) return '\uD83C\uDFBE';
  if (n.includes('b\u00E9isbol') || n.includes('baseball')) return '\u26BE';
  if (n.includes('hockey')) return '\uD83C\uDFD2';
  if (n.includes('cricket') || n.includes('cr\u00EDquet')) return '\uD83C\uDFCF';
  if (n.includes('rugby')) return '\uD83C\uDFC9';
  if (n.includes('volei') || n.includes('volleyball') || n.includes('v\u00F3ley')) return '\uD83C\uDFD0';
  if (n.includes('golf')) return '\u26F3';
  if (n.includes('dardo') || n.includes('darts')) return '\uD83C\uDFAF';
  if (n.includes('mesa') || n.includes('table tennis')) return '\uD83C\uDFD3';
  if (n.includes('esport') || n.includes('virtual')) return '\uD83C\uDFAE';
  if (n.includes('balonmano') || n.includes('handball')) return '\uD83E\uDD3E';
  if (n.includes('caballo') || n.includes('horse') || n.includes('galgo') || n.includes('greyhound')) return '\uD83C\uDFC7';
  if (n.includes('softball')) return '\uD83E\uDD4E';
  if (n.includes('futbol sala') || n.includes('futsal')) return '\u26BD';
  return '\uD83C\uDFC6';
}
