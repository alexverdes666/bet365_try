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

  function handleSelectOdds(market, selection) {
    let selName = selection.name;
    if (!selName) {
      const columnLabels = getColumnLabels(market);
      selName = columnLabels[parseInt(selection.order)] || `Selection ${parseInt(selection.order) + 1}`;
    }

    addSelection({
      eventId: event.id,
      eventName: event.name,
      marketName: market.name,
      selectionName: selName,
      selectionId: selection.id,
      odds: selection.odds
    });
  }

  function getColumnLabels(market) {
    const cols = parseInt(market.columns) || market.selections?.length || 2;
    if (cols === 3) return ['1', 'X', '2'];
    if (cols === 2) return ['1', '2'];
    return market.selections?.map((_, i) => `${i + 1}`) || [];
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
        <Link to="/" className="text-primary-600 hover:text-primary-700 text-sm font-medium">
          Back to Live Events
        </Link>
      </div>
    );
  }

  const parts = event.name ? event.name.split(/ v(?:s\.?)? /i) : ['Unknown', 'Unknown'];
  const homeTeam = parts[0] || 'Unknown';
  const awayTeam = parts[1] || 'Unknown';
  const scores = event.score ? event.score.split('-') : [];
  const homeScore = scores[0]?.trim();
  const awayScore = scores[1]?.trim();

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
          </div>
          <LiveBadge minute={event.minute} context={event.update_context} />
        </div>

        <div className="flex items-center justify-center gap-6 py-4">
          {/* Home team */}
          <div className="text-center flex-1">
            <h2 className="text-lg font-bold mb-2">{homeTeam}</h2>
          </div>

          {/* Score */}
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

          {/* Away team */}
          <div className="text-center flex-1">
            <h2 className="text-lg font-bold mb-2">{awayTeam}</h2>
          </div>
        </div>

        {event.update_context && (
          <div className="text-center text-sm text-gray-400 mt-2">{event.update_context}</div>
        )}
      </div>

      {/* Markets */}
      <h3 className="text-lg font-bold text-gray-900 mb-3">Markets</h3>

      {event.markets && event.markets.length > 0 ? (
        <div className="space-y-3">
          {event.markets.map(market => {
            const columnLabels = getColumnLabels(market);
            const isSuspended = market.suspended === '1';

            return (
              <div key={market.id} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                {/* Market header */}
                <div className="flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200">
                  <h4 className="text-sm font-semibold text-gray-800">{market.name}</h4>
                  {isSuspended && (
                    <span className="text-xs bg-red-100 text-red-600 px-2 py-0.5 rounded font-medium">
                      Suspended
                    </span>
                  )}
                </div>

                {/* Selections */}
                {!isSuspended && (
                  <div className="p-4">
                    {/* Column labels */}
                    <div className="flex gap-2 mb-2">
                      {market.selections?.map((sel, idx) => (
                        <div key={idx} className="flex-1 text-center text-xs font-semibold text-gray-500 uppercase">
                          {columnLabels[idx] || `#${idx + 1}`}
                        </div>
                      ))}
                    </div>

                    {/* Odds buttons */}
                    <div className="flex gap-2">
                      {market.selections?.map((sel, idx) => {
                        const selName = sel.name || columnLabels[idx] || `Selection ${idx + 1}`;
                        const selected = isSelected(sel.id);
                        const suspended = sel.suspended === '1';
                        const decimal = fractionalToDecimal(sel.odds);

                        return (
                          <button
                            key={sel.id || idx}
                            disabled={suspended}
                            onClick={() => handleSelectOdds(market, sel)}
                            className={`flex-1 py-3 px-4 rounded-lg text-center transition-all duration-150 ${
                              suspended
                                ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                                : selected
                                  ? 'bg-primary-600 text-white shadow-md ring-2 ring-primary-300'
                                  : 'bg-gray-50 text-gray-800 hover:bg-primary-50 hover:text-primary-700 border border-gray-200 hover:border-primary-300'
                            }`}
                          >
                            <div className="text-xs text-gray-500 mb-1">{selName}</div>
                            <div className="text-sm font-bold">{formatOdds(sel.odds)}</div>
                            <div className="text-xs opacity-60 mt-0.5">{sel.odds}</div>
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
          <p className="text-sm">No markets available for this event</p>
        </div>
      )}

      {/* Event metadata */}
      <div className="mt-6 bg-white rounded-xl shadow-sm border border-gray-200 p-4">
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
            <span className="ml-2 text-gray-800 font-mono text-xs">{event.competition_id}</span>
          </div>
          <div>
            <span className="text-gray-500">Markets:</span>
            <span className="ml-2 text-gray-800">{event.markets?.length || 0}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Event;
