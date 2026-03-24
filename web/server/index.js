require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const path = require('path');
const WebSocket = require('ws');
const http = require('http');

const authRoutes = require('./routes/auth');
const eventsRoutes = require('./routes/events');
const betsRoutes = require('./routes/bets');
const adminRoutes = require('./routes/admin');
const { seedAdmin } = require('./seed');

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const MONGODB_URI = process.env.MONGODB_URI;

// Middleware
app.use(cors());
app.use(express.json());

// API routes
app.use('/api/auth', authRoutes);
app.use('/api/events', eventsRoutes);
app.use('/api/bets', betsRoutes);
app.use('/api/admin', adminRoutes);

// Serve static files in production
if (process.env.NODE_ENV === 'production') {
  app.use(express.static(path.join(__dirname, '..', 'client', 'dist')));
  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '..', 'client', 'dist', 'index.html'));
  });
}

// WebSocket proxy to data API
const DATA_API_URL = process.env.DATA_API_URL || 'http://localhost:8365';
const WS_URL = DATA_API_URL.replace('http', 'ws') + '/ws';

const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (clientWs) => {
  let dataWs;
  try {
    dataWs = new WebSocket(WS_URL);
  } catch (err) {
    console.log('Could not connect to data API WebSocket:', err.message);
    return;
  }

  dataWs.on('open', () => {
    console.log('Connected to data API WebSocket');
  });

  dataWs.on('message', (data) => {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data.toString());
    }
  });

  dataWs.on('error', (err) => {
    console.log('Data API WebSocket error:', err.message);
  });

  dataWs.on('close', () => {
    console.log('Data API WebSocket closed');
  });

  clientWs.on('close', () => {
    if (dataWs.readyState === WebSocket.OPEN) {
      dataWs.close();
    }
  });

  clientWs.on('message', (msg) => {
    if (dataWs.readyState === WebSocket.OPEN) {
      dataWs.send(msg.toString());
    }
  });
});

// Connect to MongoDB and start server
mongoose.connect(MONGODB_URI)
  .then(async () => {
    console.log('Connected to MongoDB');
    await seedAdmin();
    server.listen(PORT, () => {
      console.log(`BetStream server running on port ${PORT}`);
      console.log(`Data API: ${DATA_API_URL}`);
    });
  })
  .catch((err) => {
    console.error('MongoDB connection error:', err.message);
    process.exit(1);
  });
