require('dotenv').config();
const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');
const User = require('./models/User');

async function seedAdmin() {
  try {
    const existing = await User.findOne({ email: 'admin@admin.com' });
    if (!existing) {
      const hashedPassword = await bcrypt.hash('admin123', 10);
      await User.create({
        email: 'admin@admin.com',
        password: hashedPassword,
        name: 'Administrator',
        balance: 0,
        role: 'admin'
      });
      console.log('Admin user seeded: admin@admin.com / admin123');
    } else {
      console.log('Admin user already exists');
    }
  } catch (err) {
    console.error('Error seeding admin:', err.message);
  }
}

// If run directly
if (require.main === module) {
  mongoose.connect(process.env.MONGODB_URI)
    .then(async () => {
      await seedAdmin();
      console.log('Seeding complete');
      process.exit(0);
    })
    .catch((err) => {
      console.error('MongoDB connection error:', err.message);
      process.exit(1);
    });
}

module.exports = { seedAdmin };
