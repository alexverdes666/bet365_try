import { Link } from 'react-router-dom';
import LiveBadge from './LiveBadge';
import OddsButton from './OddsButton';
import { formatTime } from '../api';

function EventRow({ event }) {
  // Parse team names from event name "Team A v Team B" or "Team A vs Team B"
  const parts = event.name ? event.name.split(/ v(?:s\.?)? /i) : ['Unknown', 'Unknown'];
  const homeTeam = parts[0] || 'Unknown';
  const awayTeam = parts[1] || 'Unknown';

  // Find Fulltime Result market (or first 3-column market)
  const mainMarket = event.markets?.find(m =>
    m.name === 'Fulltime Result' || m.name === 'Full Time Result' || m.columns === '3' || m.columns === 3
  ) || event.markets?.[0];

  const selections = mainMarket?.selections || [];

  // Parse score
  const scores = event.score ? event.score.split('-') : [];
  const homeScore = scores[0]?.trim();
  const awayScore = scores[1]?.trim();

  return (
    <div className="flex items-center gap-2 py-2.5 px-3 hover:bg-gray-50 transition-colors border-b border-gray-100 last:border-b-0 group">
      {/* Time / Live badge */}
      <div className="w-16 flex-shrink-0 text-center">
        <LiveBadge minute={event.minute} context={event.update_context} />
      </div>

      {/* Teams and score */}
      <Link
        to={`/event/${encodeURIComponent(event.id)}`}
        className="flex-1 min-w-0 flex items-center gap-3 group-hover:text-primary-700 transition-colors"
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-0.5">
            <span className="text-sm font-medium truncate">{homeTeam}</span>
            {homeScore !== undefined && (
              <span className="text-sm font-bold text-dark-800 bg-gray-100 px-1.5 py-0.5 rounded min-w-[24px] text-center">
                {homeScore}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium truncate">{awayTeam}</span>
            {awayScore !== undefined && (
              <span className="text-sm font-bold text-dark-800 bg-gray-100 px-1.5 py-0.5 rounded min-w-[24px] text-center">
                {awayScore}
              </span>
            )}
          </div>
        </div>
      </Link>

      {/* Odds columns */}
      {mainMarket && !mainMarket.suspended && (
        <div className="flex gap-1.5 flex-shrink-0">
          {selections.slice(0, 3).map((sel, idx) => (
            <div key={sel.id || idx} className="w-16">
              <OddsButton
                selection={sel}
                eventId={event.id}
                eventName={event.name}
                marketName={mainMarket.name}
              />
            </div>
          ))}
        </div>
      )}

      {/* Arrow */}
      <Link
        to={`/event/${encodeURIComponent(event.id)}`}
        className="flex-shrink-0 w-6 text-gray-300 hover:text-primary-500 transition-colors"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
      </Link>
    </div>
  );
}

export default EventRow;
