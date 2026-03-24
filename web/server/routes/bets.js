const express = require('express');
const Bet = require('../models/Bet');
const User = require('../models/User');
const { authenticate } = require('../middleware/auth');

const router = express.Router();

// Helper to convert fractional odds to decimal
function fractionalToDecimal(fractional) {
  if (!fractional || typeof fractional !== 'string') return 1;
  const parts = fractional.split('/');
  if (parts.length !== 2) return parseFloat(fractional) || 1;
  const numerator = parseFloat(parts[0]);
  const denominator = parseFloat(parts[1]);
  if (denominator === 0) return 1;
  return (numerator / denominator) + 1;
}

// POST /api/bets — place a bet
router.post('/', authenticate, async (req, res) => {
  try {
    const { eventId, eventName, marketName, selectionName, odds, stake } = req.body;

    if (!eventId || !eventName || !marketName || !selectionName || !odds || !stake) {
      return res.status(400).json({ error: 'All fields are required' });
    }

    const stakeNum = parseFloat(stake);
    if (isNaN(stakeNum) || stakeNum <= 0) {
      return res.status(400).json({ error: 'Invalid stake amount' });
    }

    // Get fresh user balance
    const user = await User.findById(req.user._id);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    if (stakeNum > user.balance) {
      return res.status(400).json({ error: 'Insufficient balance' });
    }

    const oddsDecimal = fractionalToDecimal(odds);
    const potentialWin = parseFloat((stakeNum * oddsDecimal).toFixed(2));

    // Deduct stake from balance
    user.balance = parseFloat((user.balance - stakeNum).toFixed(2));
    await user.save();

    const bet = await Bet.create({
      userId: user._id,
      eventId,
      eventName,
      marketName,
      selectionName,
      odds,
      oddsDecimal,
      stake: stakeNum,
      potentialWin,
      status: 'pending'
    });

    res.status(201).json({
      bet,
      newBalance: user.balance
    });
  } catch (err) {
    console.error('Place bet error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/bets — user's bet history
router.get('/', authenticate, async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 50;
    const skip = (page - 1) * limit;

    const [bets, total] = await Promise.all([
      Bet.find({ userId: req.user._id })
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit),
      Bet.countDocuments({ userId: req.user._id })
    ]);

    res.json({
      bets,
      total,
      page,
      pages: Math.ceil(total / limit)
    });
  } catch (err) {
    console.error('Get bets error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/bets/summary — user bet stats
router.get('/summary', authenticate, async (req, res) => {
  try {
    const [total, pending, won, lost] = await Promise.all([
      Bet.countDocuments({ userId: req.user._id }),
      Bet.countDocuments({ userId: req.user._id, status: 'pending' }),
      Bet.countDocuments({ userId: req.user._id, status: 'won' }),
      Bet.countDocuments({ userId: req.user._id, status: 'lost' })
    ]);

    const totalStaked = await Bet.aggregate([
      { $match: { userId: req.user._id } },
      { $group: { _id: null, total: { $sum: '$stake' } } }
    ]);

    const totalWon = await Bet.aggregate([
      { $match: { userId: req.user._id, status: 'won' } },
      { $group: { _id: null, total: { $sum: '$potentialWin' } } }
    ]);

    res.json({
      total,
      pending,
      won,
      lost,
      totalStaked: totalStaked[0]?.total || 0,
      totalWon: totalWon[0]?.total || 0
    });
  } catch (err) {
    console.error('Get bet summary error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

module.exports = router;
