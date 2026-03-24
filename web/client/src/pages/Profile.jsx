import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../api';

function Profile() {
  const { user, refreshUser } = useAuth();
  const [summary, setSummary] = useState(null);
  const [recentBets, setRecentBets] = useState([]);

  useEffect(() => {
    refreshUser();
    fetchData();
  }, []);

  async function fetchData() {
    try {
      const [summaryRes, betsRes] = await Promise.all([
        api.get('/bets/summary'),
        api.get('/bets?limit=5')
      ]);
      setSummary(summaryRes.data);
      setRecentBets(betsRes.data.bets || []);
    } catch (err) {
      console.error('Error fetching profile data:', err);
    }
  }

  if (!user) return null;

  const statusBadge = (status) => {
    const styles = {
      pending: 'bg-yellow-50 text-yellow-700',
      won: 'bg-green-50 text-green-700',
      lost: 'bg-red-50 text-red-700'
    };
    return styles[status] || 'bg-gray-50 text-gray-700';
  };

  return (
    <div className="max-w-2xl mx-auto">
      <h1 className="text-xl font-bold text-gray-900 mb-6">My Profile</h1>

      {/* Profile card */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
        <div className="flex items-center gap-4 mb-6">
          <div className="w-16 h-16 bg-primary-100 rounded-full flex items-center justify-center">
            <span className="text-2xl font-bold text-primary-600">
              {user.name?.charAt(0)?.toUpperCase()}
            </span>
          </div>
          <div>
            <h2 className="text-lg font-bold text-gray-900">{user.name}</h2>
            <p className="text-sm text-gray-500">{user.email}</p>
            <div className="flex items-center gap-2 mt-1">
              <span className={`text-xs px-2 py-0.5 rounded-full font-medium capitalize ${
                user.role === 'admin' ? 'bg-yellow-100 text-yellow-700' : 'bg-blue-100 text-blue-700'
              }`}>
                {user.role}
              </span>
              <span className="text-xs text-gray-400">
                Member since {new Date(user.createdAt).toLocaleDateString()}
              </span>
            </div>
          </div>
        </div>

        {/* Balance */}
        <div className="bg-gradient-to-r from-dark-800 to-dark-700 rounded-xl p-5 text-white">
          <div className="text-sm text-gray-300 mb-1">Account Balance</div>
          <div className="text-3xl font-extrabold">${user.balance?.toFixed(2)}</div>
          <div className="text-xs text-gray-400 mt-1">Virtual currency</div>
        </div>
      </div>

      {/* Stats */}
      {summary && (
        <div className="grid grid-cols-3 gap-3 mb-6">
          <div className="bg-white rounded-xl border border-gray-200 p-4 text-center">
            <div className="text-2xl font-bold text-gray-900">{summary.total}</div>
            <div className="text-xs text-gray-500 mt-1">Total Bets</div>
          </div>
          <div className="bg-white rounded-xl border border-gray-200 p-4 text-center">
            <div className="text-2xl font-bold text-gray-900">${summary.totalStaked?.toFixed(2)}</div>
            <div className="text-xs text-gray-500 mt-1">Total Staked</div>
          </div>
          <div className="bg-white rounded-xl border border-gray-200 p-4 text-center">
            <div className={`text-2xl font-bold ${(summary.totalWon - summary.totalStaked) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
              ${(summary.totalWon - summary.totalStaked)?.toFixed(2)}
            </div>
            <div className="text-xs text-gray-500 mt-1">Profit / Loss</div>
          </div>
        </div>
      )}

      {/* Recent bets */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200">
          <h3 className="text-sm font-semibold text-gray-800">Recent Bets</h3>
          <Link to="/my-bets" className="text-xs text-primary-600 hover:text-primary-700 font-medium">
            View All
          </Link>
        </div>

        {recentBets.length > 0 ? (
          <div className="divide-y divide-gray-100">
            {recentBets.map(bet => (
              <div key={bet._id} className="px-5 py-3 flex items-center justify-between">
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-medium text-gray-800 truncate">{bet.eventName}</div>
                  <div className="text-xs text-gray-500">{bet.selectionName} &mdash; ${bet.stake?.toFixed(2)}</div>
                </div>
                <span className={`px-2 py-0.5 rounded text-xs font-semibold capitalize ${statusBadge(bet.status)}`}>
                  {bet.status}
                </span>
              </div>
            ))}
          </div>
        ) : (
          <div className="p-8 text-center text-gray-400 text-sm">
            No bets yet. Start betting on live events!
          </div>
        )}
      </div>
    </div>
  );
}

export default Profile;
