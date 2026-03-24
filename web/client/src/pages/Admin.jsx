import { useState, useEffect } from 'react';
import api from '../api';

function Admin() {
  const [tab, setTab] = useState('dashboard');
  const [dashboard, setDashboard] = useState(null);
  const [users, setUsers] = useState([]);
  const [stats, setStats] = useState(null);
  const [selectedUser, setSelectedUser] = useState(null);
  const [userBets, setUserBets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [message, setMessage] = useState(null);

  useEffect(() => {
    if (tab === 'dashboard') fetchDashboard();
    else if (tab === 'users') fetchUsers();
    else if (tab === 'stats') fetchStats();
  }, [tab]);

  async function fetchDashboard() {
    setLoading(true);
    try {
      const res = await api.get('/admin/dashboard');
      setDashboard(res.data);
    } catch (err) {
      console.error('Error fetching dashboard:', err);
    } finally {
      setLoading(false);
    }
  }

  async function fetchUsers() {
    setLoading(true);
    try {
      const res = await api.get('/admin/users');
      setUsers(res.data);
    } catch (err) {
      console.error('Error fetching users:', err);
    } finally {
      setLoading(false);
    }
  }

  async function fetchStats() {
    setLoading(true);
    try {
      const res = await api.get('/admin/stats');
      setStats(res.data);
    } catch (err) {
      setStats({ error: 'Could not fetch stats from data API' });
    } finally {
      setLoading(false);
    }
  }

  async function handleDeleteUser(userId, userName) {
    if (!confirm(`Delete user "${userName}" and all their bets? This cannot be undone.`)) return;
    try {
      await api.delete(`/admin/users/${userId}`);
      setUsers(prev => prev.filter(u => u.id !== userId));
      setMessage({ type: 'success', text: `User "${userName}" deleted` });
      setTimeout(() => setMessage(null), 3000);
    } catch (err) {
      setMessage({ type: 'error', text: err.response?.data?.error || 'Failed to delete user' });
    }
  }

  async function handleViewBets(userId) {
    if (selectedUser === userId) {
      setSelectedUser(null);
      setUserBets([]);
      return;
    }
    try {
      const res = await api.get(`/admin/users/${userId}/bets`);
      setUserBets(res.data);
      setSelectedUser(userId);
    } catch (err) {
      console.error('Error fetching user bets:', err);
    }
  }

  async function handleSettleBet(betId, status) {
    try {
      await api.patch(`/admin/bets/${betId}`, { status });
      setUserBets(prev => prev.map(b => b._id === betId ? { ...b, status } : b));
      setMessage({ type: 'success', text: `Bet marked as ${status}` });
      setTimeout(() => setMessage(null), 3000);
    } catch (err) {
      setMessage({ type: 'error', text: err.response?.data?.error || 'Failed to update bet' });
    }
  }

  const tabs = [
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'users', label: 'Users' },
    { id: 'stats', label: 'System Stats' }
  ];

  return (
    <div className="max-w-5xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-xl font-bold text-gray-900">Admin Panel</h1>
        <span className="text-xs bg-yellow-100 text-yellow-700 px-3 py-1 rounded-full font-semibold">
          Administrator
        </span>
      </div>

      {/* Message */}
      {message && (
        <div className={`px-4 py-3 rounded-lg text-sm mb-4 ${
          message.type === 'success' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'
        }`}>
          {message.text}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 mb-6 bg-gray-100 rounded-lg p-1 w-fit">
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              tab === t.id
                ? 'bg-white text-gray-900 shadow-sm'
                : 'text-gray-600 hover:text-gray-800'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16">
          <div className="w-8 h-8 border-4 border-primary-200 border-t-primary-600 rounded-full animate-spin" />
        </div>
      ) : (
        <>
          {/* Dashboard tab */}
          {tab === 'dashboard' && dashboard && (
            <div>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                <StatCard label="Total Users" value={dashboard.totalUsers} color="blue" />
                <StatCard label="Total Bets" value={dashboard.totalBets} color="green" />
                <StatCard label="Pending Bets" value={dashboard.pendingBets} color="yellow" />
                <StatCard label="Active Events" value={dashboard.activeEvents} color="purple" />
              </div>

              <div className="bg-white rounded-xl border border-gray-200 p-4 mb-6">
                <div className="text-sm text-gray-500 mb-1">Total Amount Staked</div>
                <div className="text-2xl font-bold text-gray-900">${dashboard.totalStaked?.toFixed(2)}</div>
              </div>

              {/* Recent bets */}
              <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
                <div className="px-4 py-3 border-b border-gray-200">
                  <h3 className="text-sm font-semibold text-gray-800">Recent Bets (All Users)</h3>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 text-gray-500 text-xs uppercase">
                      <tr>
                        <th className="text-left px-4 py-2">User</th>
                        <th className="text-left px-4 py-2">Event</th>
                        <th className="text-left px-4 py-2">Selection</th>
                        <th className="text-right px-4 py-2">Stake</th>
                        <th className="text-right px-4 py-2">Potential</th>
                        <th className="text-center px-4 py-2">Status</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {dashboard.recentBets?.map(bet => (
                        <tr key={bet._id}>
                          <td className="px-4 py-2 text-gray-800">
                            {bet.userId?.name || 'Unknown'}
                          </td>
                          <td className="px-4 py-2 text-gray-600 truncate max-w-[200px]">
                            {bet.eventName}
                          </td>
                          <td className="px-4 py-2 text-gray-600">{bet.selectionName}</td>
                          <td className="px-4 py-2 text-right font-medium">${bet.stake?.toFixed(2)}</td>
                          <td className="px-4 py-2 text-right font-medium text-green-600">${bet.potentialWin?.toFixed(2)}</td>
                          <td className="px-4 py-2 text-center">
                            <span className={`px-2 py-0.5 rounded text-xs font-semibold capitalize ${
                              bet.status === 'won' ? 'bg-green-50 text-green-700'
                                : bet.status === 'lost' ? 'bg-red-50 text-red-700'
                                  : 'bg-yellow-50 text-yellow-700'
                            }`}>
                              {bet.status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {(!dashboard.recentBets || dashboard.recentBets.length === 0) && (
                    <div className="p-8 text-center text-gray-400 text-sm">No bets yet</div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Users tab */}
          {tab === 'users' && (
            <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 text-gray-500 text-xs uppercase">
                    <tr>
                      <th className="text-left px-4 py-3">User</th>
                      <th className="text-left px-4 py-3">Role</th>
                      <th className="text-right px-4 py-3">Balance</th>
                      <th className="text-right px-4 py-3">Bets</th>
                      <th className="text-right px-4 py-3">Staked</th>
                      <th className="text-left px-4 py-3">Joined</th>
                      <th className="text-center px-4 py-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {users.map(u => (
                      <>
                        <tr key={u.id} className="hover:bg-gray-50">
                          <td className="px-4 py-3">
                            <div className="font-medium text-gray-800">{u.name}</div>
                            <div className="text-xs text-gray-500">{u.email}</div>
                          </td>
                          <td className="px-4 py-3">
                            <span className={`px-2 py-0.5 rounded text-xs font-semibold capitalize ${
                              u.role === 'admin' ? 'bg-yellow-50 text-yellow-700' : 'bg-blue-50 text-blue-700'
                            }`}>
                              {u.role}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right font-medium">${u.balance?.toFixed(2)}</td>
                          <td className="px-4 py-3 text-right">{u.betCount}</td>
                          <td className="px-4 py-3 text-right">${u.totalStaked?.toFixed(2)}</td>
                          <td className="px-4 py-3 text-gray-500 text-xs">
                            {new Date(u.createdAt).toLocaleDateString()}
                          </td>
                          <td className="px-4 py-3 text-center">
                            <div className="flex items-center justify-center gap-2">
                              <button
                                onClick={() => handleViewBets(u.id)}
                                className="text-xs text-primary-600 hover:text-primary-700 font-medium"
                              >
                                {selectedUser === u.id ? 'Hide' : 'Bets'}
                              </button>
                              {u.role !== 'admin' && (
                                <button
                                  onClick={() => handleDeleteUser(u.id, u.name)}
                                  className="text-xs text-red-600 hover:text-red-700 font-medium"
                                >
                                  Delete
                                </button>
                              )}
                            </div>
                          </td>
                        </tr>
                        {/* Expanded bets row */}
                        {selectedUser === u.id && (
                          <tr key={`${u.id}-bets`}>
                            <td colSpan={7} className="bg-gray-50 px-4 py-3">
                              <div className="text-xs font-semibold text-gray-600 mb-2">
                                Bets by {u.name} ({userBets.length})
                              </div>
                              {userBets.length > 0 ? (
                                <div className="space-y-1">
                                  {userBets.map(bet => (
                                    <div key={bet._id} className="flex items-center gap-3 bg-white rounded-lg px-3 py-2 text-xs">
                                      <div className="flex-1 min-w-0">
                                        <span className="font-medium">{bet.eventName}</span>
                                        <span className="text-gray-400 mx-1">/</span>
                                        <span>{bet.selectionName}</span>
                                      </div>
                                      <span>${bet.stake?.toFixed(2)}</span>
                                      <span className="text-green-600">${bet.potentialWin?.toFixed(2)}</span>
                                      <span className={`px-2 py-0.5 rounded font-semibold capitalize ${
                                        bet.status === 'won' ? 'bg-green-50 text-green-700'
                                          : bet.status === 'lost' ? 'bg-red-50 text-red-700'
                                            : 'bg-yellow-50 text-yellow-700'
                                      }`}>
                                        {bet.status}
                                      </span>
                                      {bet.status === 'pending' && (
                                        <div className="flex gap-1">
                                          <button
                                            onClick={() => handleSettleBet(bet._id, 'won')}
                                            className="px-2 py-0.5 bg-green-100 text-green-700 rounded hover:bg-green-200 font-medium"
                                          >
                                            Won
                                          </button>
                                          <button
                                            onClick={() => handleSettleBet(bet._id, 'lost')}
                                            className="px-2 py-0.5 bg-red-100 text-red-700 rounded hover:bg-red-200 font-medium"
                                          >
                                            Lost
                                          </button>
                                        </div>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <div className="text-gray-400 text-xs">No bets from this user</div>
                              )}
                            </td>
                          </tr>
                        )}
                      </>
                    ))}
                  </tbody>
                </table>
                {users.length === 0 && (
                  <div className="p-8 text-center text-gray-400 text-sm">No users found</div>
                )}
              </div>
            </div>
          )}

          {/* System Stats tab */}
          {tab === 'stats' && (
            <div className="bg-white rounded-xl border border-gray-200 p-6">
              <h3 className="text-sm font-semibold text-gray-800 mb-4">Data API System Stats</h3>
              {stats?.error ? (
                <div className="text-red-500 text-sm">{stats.error}</div>
              ) : stats ? (
                <pre className="bg-gray-50 rounded-lg p-4 text-sm text-gray-700 overflow-x-auto whitespace-pre-wrap font-mono">
                  {JSON.stringify(stats, null, 2)}
                </pre>
              ) : (
                <div className="text-gray-400 text-sm">No stats available</div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function StatCard({ label, value, color }) {
  const colors = {
    blue: 'bg-blue-50 text-blue-700',
    green: 'bg-green-50 text-green-700',
    yellow: 'bg-yellow-50 text-yellow-700',
    purple: 'bg-purple-50 text-purple-700',
    red: 'bg-red-50 text-red-700'
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4">
      <div className={`w-10 h-10 rounded-lg flex items-center justify-center mb-2 ${colors[color]}`}>
        <span className="text-xl font-bold">{value}</span>
      </div>
      <div className="text-xs text-gray-500 font-medium">{label}</div>
      <div className="text-2xl font-bold text-gray-900 mt-1">{value}</div>
    </div>
  );
}

export default Admin;
