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

// Utility: convert fractional odds to decimal
export function fractionalToDecimal(fractional) {
  if (!fractional || typeof fractional !== 'string') return 1;
  const parts = fractional.split('/');
  if (parts.length !== 2) return parseFloat(fractional) || 1;
  const num = parseFloat(parts[0]);
  const den = parseFloat(parts[1]);
  if (den === 0) return 1;
  return (num / den) + 1;
}

// Utility: format decimal odds for display
export function formatOdds(fractional) {
  const decimal = fractionalToDecimal(fractional);
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

// Sport icon mapping
export function getSportIcon(sportName) {
  const icons = {
    'Soccer': '\u26BD',
    'Football': '\uD83C\uDFC8',
    'Basketball': '\uD83C\uDFC0',
    'Tennis': '\uD83C\uDFBE',
    'Baseball': '\u26BE',
    'Hockey': '\uD83C\uDFD2',
    'Cricket': '\uD83C\uDFCF',
    'Rugby': '\uD83C\uDFC9',
    'Volleyball': '\uD83C\uDFD0',
    'Boxing': '\uD83E\uDD4A',
    'MMA': '\uD83E\uDD4A',
    'Golf': '\u26F3',
    'Darts': '\uD83C\uDFAF',
    'Snooker': '\uD83C\uDFB1',
    'Table Tennis': '\uD83C\uDFD3',
    'Esports': '\uD83C\uDFAE',
    'Handball': '\uD83E\uDD3E',
    'Cycling': '\uD83D\uDEB4',
  };
  return icons[sportName] || '\uD83C\uDFC6';
}
