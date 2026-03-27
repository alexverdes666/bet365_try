const express = require('express');
const User = require('../models/User');
const Bet = require('../models/Bet');
const { authenticate, requireAdmin } = require('../middleware/auth');

const router = express.Router();

// All admin routes require authentication + admin role
router.use(authenticate, requireAdmin);

// GET /api/admin/dashboard — overview stats
router.get('/dashboard', async (req, res) => {
  try {
    const [totalUsers, totalBets, pendingBets, totalStaked, recentBets] = await Promise.all([
      User.countDocuments(),
      Bet.countDocuments(),
      Bet.countDocuments({ status: 'pending' }),
      Bet.aggregate([
        { $group: { _id: null, total: { $sum: '$stake' } } }
      ]),
      Bet.find()
        .sort({ createdAt: -1 })
        .limit(10)
        .populate('userId', 'name email')
    ]);

    // Try to fetch active events from data API
    let activeEvents = 0;
    try {
      const DATA_API_URL = process.env.DATA_API_URL || 'http://localhost:8365';
      const response = await fetch(`${DATA_API_URL}/events`);
      if (response.ok) {
        const events = await response.json();
        activeEvents = Array.isArray(events) ? events.length : 0;
      }
    } catch (e) {
      // Data API might be unavailable
    }

    res.json({
      totalUsers,
      totalBets,
      pendingBets,
      activeEvents,
      totalStaked: totalStaked[0]?.total || 0,
      recentBets
    });
  } catch (err) {
    console.error('Admin dashboard error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/admin/users — list all users
router.get('/users', async (req, res) => {
  try {
    const users = await User.find()
      .select('-password')
      .sort({ createdAt: -1 });

    // Get bet counts for each user
    const userIds = users.map(u => u._id);
    const betCounts = await Bet.aggregate([
      { $match: { userId: { $in: userIds } } },
      { $group: { _id: '$userId', count: { $sum: 1 }, totalStaked: { $sum: '$stake' } } }
    ]);

    const betCountMap = {};
    betCounts.forEach(bc => {
      betCountMap[bc._id.toString()] = { count: bc.count, totalStaked: bc.totalStaked };
    });

    const usersWithStats = users.map(u => ({
      id: u._id,
      email: u.email,
      name: u.name,
      balance: u.balance,
      role: u.role,
      createdAt: u.createdAt,
      betCount: betCountMap[u._id.toString()]?.count || 0,
      totalStaked: betCountMap[u._id.toString()]?.totalStaked || 0
    }));

    res.json(usersWithStats);
  } catch (err) {
    console.error('Admin users error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/admin/users/:userId/bets — get bets for a specific user
router.get('/users/:userId/bets', async (req, res) => {
  try {
    const bets = await Bet.find({ userId: req.params.userId })
      .sort({ createdAt: -1 })
      .limit(100);

    res.json(bets);
  } catch (err) {
    console.error('Admin user bets error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// DELETE /api/admin/users/:userId — delete a user
router.delete('/users/:userId', async (req, res) => {
  try {
    const user = await User.findById(req.params.userId);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    if (user.role === 'admin') {
      return res.status(400).json({ error: 'Cannot delete admin user' });
    }

    // Delete user's bets
    await Bet.deleteMany({ userId: user._id });
    // Delete user
    await User.findByIdAndDelete(user._id);

    res.json({ message: 'User and their bets deleted' });
  } catch (err) {
    console.error('Admin delete user error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// PATCH /api/admin/bets/:betId — update bet status (settle)
router.patch('/bets/:betId', async (req, res) => {
  try {
    const { status } = req.body;
    if (!['won', 'lost', 'pending'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    const bet = await Bet.findById(req.params.betId);
    if (!bet) {
      return res.status(404).json({ error: 'Bet not found' });
    }

    const oldStatus = bet.status;
    bet.status = status;
    await bet.save();

    // If marking as won, credit winnings to user
    if (status === 'won' && oldStatus !== 'won') {
      await User.findByIdAndUpdate(bet.userId, {
        $inc: { balance: bet.potentialWin }
      });
    }

    // If changing from won to something else, deduct winnings
    if (oldStatus === 'won' && status !== 'won') {
      await User.findByIdAndUpdate(bet.userId, {
        $inc: { balance: -bet.potentialWin }
      });
    }

    res.json(bet);
  } catch (err) {
    console.error('Admin update bet error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/admin/stats — data API system stats
router.get('/stats', async (req, res) => {
  try {
    const DATA_API_URL = process.env.DATA_API_URL || 'http://localhost:8365';
    const response = await fetch(`${DATA_API_URL}/stats`);
    if (!response.ok) {
      throw new Error(`Data API returned ${response.status}`);
    }
    const data = await response.json();
    res.json(data);
  } catch (err) {
    console.error('Admin stats error:', err.message);
    res.status(502).json({ error: 'Could not fetch stats from data API' });
  }
});

module.exports = router;
