# BetStream - Live Sports Betting Platform

A professional OddsPortal-style betting website built with React, Express, and MongoDB. Displays live event data from the bet365 data streaming API with real-time odds, bet placement, and an admin panel.

## Tech Stack

- **Frontend**: React 18 (Vite), TailwindCSS, React Router v6
- **Backend**: Node.js + Express
- **Database**: MongoDB Atlas
- **Auth**: JWT tokens + bcrypt password hashing
- **Real-time**: WebSocket proxy for live data updates

## Prerequisites

- Node.js 18+
- npm or yarn
- The bet365 data streaming API running (default: `http://localhost:8365`)
- MongoDB Atlas connection (pre-configured in `.env`)

## Quick Start

### 1. Install server dependencies

```bash
cd web
npm install
```

### 2. Install client dependencies

```bash
cd web/client
npm install
```

### 3. Start the backend server

```bash
cd web
npm run dev
```

The server runs on `http://localhost:3000`.

### 4. Start the frontend dev server

```bash
cd web/client
npm run dev
```

The frontend runs on `http://localhost:5173` with API proxy to the backend.

### 5. Open the app

Visit `http://localhost:5173` in your browser.

## Environment Variables

Create or edit `web/.env`:

```env
DATA_API_URL=http://localhost:8365
MONGODB_URI=mongodb+srv://alexverdes666:3A0BQeGpi7dP5Yf8@cluster0.1w8ozov.mongodb.net/bet365?appName=Cluster0
JWT_SECRET=betstream_jwt_secret_key_2026_xK9mP2vL
PORT=3000
```

## Default Accounts

On first run, an admin account is automatically seeded:

- **Email**: `admin@admin.com`
- **Password**: `admin123`

New users register with a starting balance of **$1,000** (virtual money).

## Features

### Public (no auth required)
- Live events homepage grouped by sport
- Sport-specific event listing
- Event detail page with all markets and odds
- Real-time auto-refresh every 5 seconds

### Authenticated Users
- Place bets by clicking odds and entering stakes
- Bet slip (sidebar) with individual and batch bet placement
- My Bets page with history, filters, and statistics
- Profile page with balance and betting summary

### Admin Panel (`/admin`)
- Dashboard with total users, bets, and active events
- User management: view, inspect bets, delete users
- Bet settlement: mark bets as won/lost
- System stats from the data API

## Production Build

```bash
# Build the client
cd web/client
npm run build

# Start the server in production mode (serves the built client)
cd web
npm start
```

## Project Structure

```
web/
├── .env                    # Environment variables
├── package.json            # Server dependencies
├── server/
│   ├── index.js            # Express server + WebSocket proxy
│   ├── seed.js             # Admin user seeder
│   ├── routes/
│   │   ├── auth.js         # Login, register, user info
│   │   ├── events.js       # Proxy to bet365 data API
│   │   ├── bets.js         # Place bets, bet history
│   │   └── admin.js        # Admin dashboard, user mgmt
│   ├── models/
│   │   ├── User.js         # User schema
│   │   └── Bet.js          # Bet schema
│   └── middleware/
│       └── auth.js         # JWT authentication
├── client/
│   ├── package.json        # Client dependencies
│   ├── vite.config.js      # Vite config with API proxy
│   ├── tailwind.config.js  # TailwindCSS theme
│   ├── index.html          # HTML entry
│   └── src/
│       ├── main.jsx        # React entry
│       ├── App.jsx         # Router layout
│       ├── api.js          # Axios instance + utilities
│       ├── index.css       # Tailwind imports + animations
│       ├── context/
│       │   ├── AuthContext.jsx    # Auth state management
│       │   └── BetSlipContext.jsx # Bet slip state
│       ├── pages/
│       │   ├── Home.jsx     # Live events overview
│       │   ├── Sport.jsx    # Sport-specific events
│       │   ├── Event.jsx    # Event detail with markets
│       │   ├── Login.jsx    # Login form
│       │   ├── Register.jsx # Registration form
│       │   ├── MyBets.jsx   # Bet history
│       │   ├── Profile.jsx  # User profile
│       │   └── Admin.jsx    # Admin panel
│       └── components/
│           ├── Header.jsx         # Navigation bar
│           ├── SportSidebar.jsx   # Left sidebar
│           ├── EventRow.jsx       # Event list row
│           ├── OddsButton.jsx     # Clickable odds
│           ├── BetSlip.jsx        # Bet slip panel
│           ├── LiveBadge.jsx      # Live/FT status badge
│           └── ProtectedRoute.jsx # Auth route guard
└── README.md
```

## API Endpoints

### Auth
- `POST /api/auth/register` — Create account
- `POST /api/auth/login` — Sign in
- `GET /api/auth/me` — Current user info

### Events (proxied from data API)
- `GET /api/events` — All live events
- `GET /api/events/sports` — Sports list
- `GET /api/events/sport/:sportId` — Events by sport
- `GET /api/events/event/:eventId` — Single event detail
- `GET /api/events/stats` — System statistics

### Bets (auth required)
- `POST /api/bets` — Place a bet
- `GET /api/bets` — User bet history
- `GET /api/bets/summary` — User betting stats

### Admin (admin role required)
- `GET /api/admin/dashboard` — Overview stats
- `GET /api/admin/users` — List all users
- `GET /api/admin/users/:id/bets` — User's bets
- `DELETE /api/admin/users/:id` — Delete user
- `PATCH /api/admin/bets/:id` — Settle a bet
- `GET /api/admin/stats` — Data API stats
