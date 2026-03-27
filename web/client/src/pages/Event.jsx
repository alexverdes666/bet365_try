import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import api from '../api';
import { formatOdds, getSportIcon, fractionalToDecimal } from '../api';
import LiveBadge from '../components/LiveBadge';
import { useBetSlip } from '../context/BetSlipContext';

function Event() {
  const { eventId } = useParams();
  const [event, setEvent] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedMarkets, setExpandedMarkets] = useState(new Set());
  const [searchFilter, setSearchFilter] = useState('');
  const { addSelection, isSelected } = useBetSlip();

  useEffect(() => {
    setLoading(true);
    fetchEvent();
    const interval = setInterval(fetchEvent, 5000);
    return () => clearInterval(interval);
  }, [eventId]);

  async function fetchEvent() {
    try {
      const res = await api.get(`/events/event/${encodeURIComponent(eventId)}`);
      setEvent(res.data);
      setError(null);
    } catch (err) {
      setError('Could not load event details');
    } finally {
      setLoading(false);
    }
  }

  function toggleMarket(marketId) {
    setExpandedMarkets(prev => {
      const next = new Set(prev);
      if (next.has(marketId)) next.delete(marketId);
      else next.add(marketId);
      return next;
    });
  }

  function expandAll() {
    if (!event?.markets) return;
    setExpandedMarkets(new Set(event.markets.map(m => m.id)));
  }

  function collapseAll() {
    setExpandedMarkets(new Set());
  }

  function handleSelectOdds(market, selection) {
    let selName = selection.name || `Selection ${parseInt(selection.order || 0) + 1}`;
    addSelection({
      eventId: event.id,
      eventName: event.name,
      marketName: market.name || 'Market',
      selectionName: selName,
      selectionId: selection.id,
      odds: selection.odds
    });
  }

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-20">
        <div className="w-10 h-10 border-4 border-primary-200 border-t-primary-600 rounded-full animate-spin mb-4" />
        <p className="text-gray-500 text-sm">Loading event...</p>
      </div>
    );
  }

  if (error || !event) {
    return (
      <div className="text-center py-20">
        <div className="text-4xl mb-4">&#x26A0;&#xFE0F;</div>
        <h3 className="text-lg font-semibold text-gray-800 mb-2">Event Not Found</h3>
        <p className="text-sm text-gray-500 mb-4">{error || 'This event may have ended.'}</p>
        <Link to="/" className="text-primary-600 hover:text-primary-700 text-sm font-medium">Back to Live Events</Link>
      </div>
    );
  }

  const parts = event.name ? event.name.split(/ v(?:s\.?)? /i) : ['Unknown', 'Unknown'];
  const homeTeam = parts[0] || 'Unknown';
  const awayTeam = parts[1] || 'Unknown';
  const scores = event.score ? event.score.split('-') : [];
  const homeScore = scores[0]?.trim();
  const awayScore = scores[1]?.trim();

  // Filter markets by search
  const markets = (event.markets || []).filter(m => {
    if (!searchFilter) return true;
    const q = searchFilter.toLowerCase();
    const name = (m.name || '').toLowerCase();
    const selNames = (m.selections || []).map(s => (s.name || '').toLowerCase()).join(' ');
    return name.includes(q) || selNames.includes(q);
  });

  // Count active markets (with odds)
  const activeMarkets = markets.filter(m => m.suspended !== '1' && m.selections?.some(s => s.odds));
  const totalSelections = markets.reduce((sum, m) => sum + (m.selections?.length || 0), 0);

  return (
    <div className="max-w-4xl mx-auto">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-sm text-gray-500 mb-4">
        <Link to="/" className="hover:text-primary-600 transition-colors">Home</Link>
        <span>/</span>
        {event.sport_id && (
          <>
            <Link to={`/sport/${event.sport_id}`} className="hover:text-primary-600 transition-colors">
              {event.sport_name || 'Sport'}
            </Link>
            <span>/</span>
          </>
        )}
        <span className="text-gray-800 font-medium truncate">{event.name}</span>
      </div>

      {/* Score header */}
      <div className="bg-dark-800 text-white rounded-xl p-6 mb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <span className="text-xl">{getSportIcon(event.sport_name)}</span>
            <span className="text-sm text-gray-400">{event.sport_name}</span>
            {event.competition_name && (
              <span className="text-xs text-gray-500 ml-2">{event.competition_name}</span>
            )}
          </div>
          <LiveBadge minute={event.minute} context={event.update_context} />
        </div>

        <div className="flex items-center justify-center gap-6 py-4">
          <div className="text-center flex-1">
            <h2 className="text-lg font-bold mb-2">{homeTeam}</h2>
          </div>
          <div className="text-center px-6">
            {event.score ? (
              <div className="flex items-center gap-3">
                <span className="text-4xl font-extrabold">{homeScore}</span>
                <span className="text-xl text-gray-400">-</span>
                <span className="text-4xl font-extrabold">{awayScore}</span>
              </div>
            ) : (
              <span className="text-2xl text-gray-400">vs</span>
            )}
            {event.minute && event.update_context !== 'Full time' && (
              <div className="mt-2 text-sm text-gray-400">{event.minute}' min</div>
            )}
          </div>
          <div className="text-center flex-1">
            <h2 className="text-lg font-bold mb-2">{awayTeam}</h2>
          </div>
        </div>
      </div>

      {/* Markets toolbar */}
      <div className="flex items-center justify-between mb-3 gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-bold text-gray-900">Markets</h3>
          <span className="text-xs bg-primary-100 text-primary-700 px-2 py-0.5 rounded-full font-semibold">
            {markets.length} markets
          </span>
          <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full">
            {totalSelections} selections
          </span>
        </div>
        <div className="flex items-center gap-2">
          {markets.length > 5 && (
            <input
              type="text"
              placeholder="Search markets..."
              value={searchFilter}
              onChange={e => setSearchFilter(e.target.value)}
              className="text-sm border border-gray-300 rounded-lg px-3 py-1.5 w-48 focus:outline-none focus:ring-2 focus:ring-primary-300"
            />
          )}
          <button onClick={expandAll} className="text-xs text-primary-600 hover:text-primary-700 font-medium">
            Expand All
          </button>
          <span className="text-gray-300">|</span>
          <button onClick={collapseAll} className="text-xs text-gray-500 hover:text-gray-700 font-medium">
            Collapse All
          </button>
        </div>
      </div>

      {/* Markets list */}
      {markets.length > 0 ? (
        <div className="space-y-2">
          {markets.map(market => {
            const isSuspended = market.suspended === '1';
            const isExpanded = expandedMarkets.has(market.id);
            const selections = market.selections || [];
            const hasOdds = selections.some(s => s.odds);
            const marketName = market.name || market.market_type || 'Market';

            return (
              <div key={market.id} className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                {/* Market header — always visible, clickable */}
                <button
                  onClick={() => toggleMarket(market.id)}
                  className="w-full flex items-center justify-between px-4 py-2.5 bg-gray-50 hover:bg-gray-100 transition-colors text-left"
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <svg className={`w-4 h-4 text-gray-400 transition-transform flex-shrink-0 ${isExpanded ? 'rotate-90' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                    <h4 className="text-sm font-semibold text-gray-800 truncate">{marketName}</h4>
                    <span className="text-xs text-gray-400 flex-shrink-0">{selections.length} sel.</span>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    {isSuspended && (
                      <span className="text-xs bg-red-100 text-red-600 px-2 py-0.5 rounded font-medium">Suspended</span>
                    )}
                    {!isSuspended && hasOdds && !isExpanded && (
                      <div className="flex gap-1">
                        {selections.filter(s => s.odds).slice(0, 3).map((sel, idx) => (
                          <span key={idx} className="text-xs bg-primary-50 text-primary-700 px-2 py-0.5 rounded font-mono font-semibold">
                            {formatOdds(sel.odds)}
                          </span>
                        ))}
                        {selections.filter(s => s.odds).length > 3 && (
                          <span className="text-xs text-gray-400">+{selections.filter(s => s.odds).length - 3}</span>
                        )}
                      </div>
                    )}
                  </div>
                </button>

                {/* Expanded selections */}
                {isExpanded && !isSuspended && (
                  <div className="p-3 border-t border-gray-100">
                    <div className="grid gap-1.5" style={{
                      gridTemplateColumns: selections.length <= 3 ? `repeat(${selections.length}, 1fr)` :
                        selections.length <= 6 ? 'repeat(3, 1fr)' : 'repeat(auto-fill, minmax(120px, 1fr))'
                    }}>
                      {selections.map((sel, idx) => {
                        const selName = sel.name || `#${idx + 1}`;
                        const selected = isSelected(sel.id);
                        const suspended = sel.suspended === '1';
                        const decimal = fractionalToDecimal(sel.odds);
                        const handicapStr = sel.handicap ? ` (${sel.handicap_display || sel.handicap})` : '';

                        return (
                          <button
                            key={sel.id || idx}
                            disabled={suspended || !decimal}
                            onClick={() => handleSelectOdds(market, sel)}
                            className={`py-2 px-3 rounded-lg text-center transition-all duration-150 ${
                              suspended || !decimal
                                ? 'bg-gray-50 text-gray-300 cursor-not-allowed'
                                : selected
                                  ? 'bg-primary-600 text-white shadow-md ring-2 ring-primary-300'
                                  : 'bg-gray-50 text-gray-800 hover:bg-primary-50 hover:text-primary-700 border border-gray-200 hover:border-primary-300'
                            }`}
                          >
                            <div className="text-xs text-gray-500 truncate mb-0.5" title={selName + handicapStr}>
                              {selName}{handicapStr}
                            </div>
                            <div className="text-sm font-bold">
                              {decimal ? formatOdds(sel.odds) : '-'}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8 text-center text-gray-400">
          <p className="text-sm">{searchFilter ? 'No markets match your search' : 'No markets available for this event'}</p>
        </div>
      )}

      {/* Event metadata */}
      <div className="mt-6 bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-6">
        <h4 className="text-sm font-semibold text-gray-800 mb-3">Event Info</h4>
        <div className="grid grid-cols-2 gap-3 text-sm">
          <div>
            <span className="text-gray-500">Event ID:</span>
            <span className="ml-2 text-gray-800 font-mono text-xs">{event.id}</span>
          </div>
          <div>
            <span className="text-gray-500">Sport:</span>
            <span className="ml-2 text-gray-800">{event.sport_name}</span>
          </div>
          <div>
            <span className="text-gray-500">Competition:</span>
            <span className="ml-2 text-gray-800">{event.competition_name || event.competition_id}</span>
          </div>
          <div>
            <span className="text-gray-500">Markets:</span>
            <span className="ml-2 text-gray-800">{event.markets?.length || 0}</span>
          </div>
          <div>
            <span className="text-gray-500">Selections:</span>
            <span className="ml-2 text-gray-800">{totalSelections}</span>
          </div>
          <div>
            <span className="text-gray-500">Score:</span>
            <span className="ml-2 text-gray-800">{event.score || 'N/A'}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Event;
