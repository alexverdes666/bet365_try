const mongoose = require('mongoose');

const betSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true
  },
  eventId: {
    type: String,
    required: true
  },
  eventName: {
    type: String,
    required: true
  },
  marketName: {
    type: String,
    required: true
  },
  selectionName: {
    type: String,
    required: true
  },
  odds: {
    type: String,
    required: true
  },
  oddsDecimal: {
    type: Number,
    required: true
  },
  stake: {
    type: Number,
    required: true,
    min: 0.01
  },
  potentialWin: {
    type: Number,
    required: true
  },
  status: {
    type: String,
    enum: ['pending', 'won', 'lost'],
    default: 'pending'
  },
  createdAt: {
    type: Date,
    default: Date.now
  }
});

betSchema.index({ userId: 1, createdAt: -1 });

module.exports = mongoose.model('Bet', betSchema);
