const express = require('express');
const router = express.Router();

const DATA_API_URL = process.env.DATA_API_URL || 'http://localhost:8365';

// Helper to fetch from data API
async function fetchFromAPI(endpoint) {
  const url = `${DATA_API_URL}${endpoint}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Data API returned ${response.status}`);
  }
  return response.json();
}

// GET /api/events — all events
router.get('/', async (req, res) => {
  try {
    const data = await fetchFromAPI('/events');
    res.json(data);
  } catch (err) {
    console.error('Error fetching events:', err.message);
    res.status(502).json({ error: 'Could not fetch events from data API' });
  }
});

// GET /api/events/sports — sports list
router.get('/sports', async (req, res) => {
  try {
    const data = await fetchFromAPI('/sports');
    res.json(data);
  } catch (err) {
    console.error('Error fetching sports:', err.message);
    res.status(502).json({ error: 'Could not fetch sports from data API' });
  }
});

// GET /api/events/stats — system stats
router.get('/stats', async (req, res) => {
  try {
    const data = await fetchFromAPI('/stats');
    res.json(data);
  } catch (err) {
    console.error('Error fetching stats:', err.message);
    res.status(502).json({ error: 'Could not fetch stats from data API' });
  }
});

// GET /api/events/sport/:sportId — events by sport
router.get('/sport/:sportId', async (req, res) => {
  try {
    const data = await fetchFromAPI(`/events/${req.params.sportId}`);
    res.json(data);
  } catch (err) {
    console.error('Error fetching sport events:', err.message);
    res.status(502).json({ error: 'Could not fetch events from data API' });
  }
});

// GET /api/events/event/:eventId — single event
router.get('/event/:eventId', async (req, res) => {
  try {
    const data = await fetchFromAPI(`/event/${req.params.eventId}`);
    res.json(data);
  } catch (err) {
    console.error('Error fetching event:', err.message);
    res.status(502).json({ error: 'Could not fetch event from data API' });
  }
});

module.exports = router;
