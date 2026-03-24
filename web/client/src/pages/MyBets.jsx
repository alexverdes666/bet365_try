import { useState, useEffect } from 'react';
import api from '../api';
import { formatOdds } from '../api';

function MyBets() {
  const [bets, setBets] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all');
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);

  useEffect(() => {
    fetchBets();
    fetchSummary();
  }, [page]);

  async function fetchBets() {
    try {
      const res = await api.get(`/bets?page=${page}&limit=20`);
      setBets(res.data.bets);
      setTotalPages(res.data.pages);
    } catch (err) {
      console.error('Error fetching bets:', err);
    } finally {
      setLoading(false);
    }
  }

  async function fetchSummary() {
    try {
      const res = await api.get('/bets/summary');
      setSummary(res.data);
    } catch (err) {
      console.error('Error fetching summary:', err);
    }
  }

  const filteredBets = filter === 'all'
    ? bets
    : bets.filter(b => b.status === filter);

  const statusBadge = (status) => {
    const styles = {
      pending: 'bg-yellow-50 text-yellow-700 border-yellow-200',
      won: 'bg-green-50 text-green-700 border-green-200',
      lost: 'bg-red-50 text-red-700 border-red-200'
    };
    return styles[status] || 'bg-gray-50 text-gray-700';
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-20">
        <div className="w-10 h-10 border-4 border-primary-200 border-t-primary-600 rounded-full animate-spin mb-4" />
        <p className="text-gray-500 text-sm">Loading your bets...</p>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto">
      <h1 className="text-xl font-bold text-gray-900 mb-4">My Bets</h1>

      {/* Summary cards */}
      {summary && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
          <div className="bg-white rounded-xl border border-gray-200 p-4">
            <div className="text-2xl font-bold text-gray-900">{summary.total}</div>
            <div className="text-xs text-gray-500 mt-1">Total Bets</div>
          </div>
          <div className="bg-white rounded-xl border border-gray-200 p-4">
            <div className="text-2xl font-bold text-yellow-600">{summary.pending}</div>
            <div className="text-xs text-gray-500 mt-1">Pending</div>
          </div>
          <div className="bg-white rounded-xl border border-gray-200 p-4">
            <div className="text-2xl font-bold text-green-600">{summary.won}</div>
            <div className="text-xs text-gray-500 mt-1">Won</div>
          </div>
          <div className="bg-white rounded-xl border border-gray-200 p-4">
            <div className="text-2xl font-bold text-red-600">{summary.lost}</div>
            <div className="text-xs text-gray-500 mt-1">Lost</div>
          </div>
        </div>
      )}

      {/* Financial summary */}
      {summary && (
        <div className="bg-white rounded-xl border border-gray-200 p-4 mb-6 flex gap-6">
          <div>
            <span className="text-xs text-gray-500">Total Staked</span>
            <div className="text-lg font-bold text-gray-800">${summary.totalStaked?.toFixed(2)}</div>
          </div>
          <div>
            <span className="text-xs text-gray-500">Total Won</span>
            <div className="text-lg font-bold text-green-600">${summary.totalWon?.toFixed(2)}</div>
          </div>
          <div>
            <span className="text-xs text-gray-500">P/L</span>
            <div className={`text-lg font-bold ${(summary.totalWon - summary.totalStaked) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
              ${(summary.totalWon - summary.totalStaked)?.toFixed(2)}
            </div>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="flex gap-2 mb-4">
        {['all', 'pending', 'won', 'lost'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors capitalize ${
              filter === f
                ? 'bg-primary-600 text-white'
                : 'bg-white text-gray-600 border border-gray-200 hover:bg-gray-50'
            }`}
          >
            {f}
          </button>
        ))}
      </div>

      {/* Bets list */}
      <div className="space-y-2">
        {filteredBets.map(bet => (
          <div key={bet._id} className="bg-white rounded-xl border border-gray-200 p-4">
            <div className="flex items-start justify-between mb-2">
              <div className="min-w-0 flex-1">
                <div className="text-sm font-semibold text-gray-800">{bet.eventName}</div>
                <div className="text-xs text-gray-500 mt-0.5">
                  {bet.marketName} &mdash; {bet.selectionName}
                </div>
              </div>
              <span className={`px-2.5 py-1 rounded-full text-xs font-semibold border capitalize ${statusBadge(bet.status)}`}>
                {bet.status}
              </span>
            </div>
            <div className="flex items-center gap-4 text-sm">
              <div>
                <span className="text-gray-500">Odds:</span>
                <span className="ml-1 font-semibold">{formatOdds(bet.odds)}</span>
                <span className="text-gray-400 text-xs ml-1">({bet.odds})</span>
              </div>
              <div>
                <span className="text-gray-500">Stake:</span>
                <span className="ml-1 font-semibold">${bet.stake?.toFixed(2)}</span>
              </div>
              <div>
                <span className="text-gray-500">Potential:</span>
                <span className="ml-1 font-semibold text-green-600">${bet.potentialWin?.toFixed(2)}</span>
              </div>
            </div>
            <div className="text-xs text-gray-400 mt-2">
              {new Date(bet.createdAt).toLocaleString()}
            </div>
          </div>
        ))}
      </div>

      {filteredBets.length === 0 && (
        <div className="text-center py-12 text-gray-400">
          <div className="text-3xl mb-3">&#x1F4CB;</div>
          <p className="text-sm font-medium">No bets found</p>
          <p className="text-xs mt-1">
            {filter === 'all' ? 'Place your first bet to see it here!' : `No ${filter} bets`}
          </p>
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex justify-center gap-2 mt-6">
          <button
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page === 1}
            className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Previous
          </button>
          <span className="px-3 py-1.5 text-sm text-gray-600">
            Page {page} of {totalPages}
          </span>
          <button
            onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            disabled={page === totalPages}
            className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}

export default MyBets;
