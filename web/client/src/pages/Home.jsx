import { useState, useEffect, useRef } from 'react';
import api from '../api';
import { getSportIcon } from '../api';
import EventRow from '../components/EventRow';

function Home() {
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const prevEventsRef = useRef({});

  useEffect(() => {
    fetchEvents();
    const interval = setInterval(fetchEvents, 5000);
    return () => clearInterval(interval);
  }, []);

  async function fetchEvents() {
    try {
      const res = await api.get('/events');
      const data = Array.isArray(res.data) ? res.data : [];
      setEvents(data);
      setError(null);

      // Track previous odds for flash animation
      const map = {};
      data.forEach(e => { map[e.id] = e; });
      prevEventsRef.current = map;
    } catch (err) {
      if (events.length === 0) {
        setError('Could not connect to the data API. Make sure the bet365 streaming service is running.');
      }
    } finally {
      setLoading(false);
    }
  }

  // Group events by sport
  const groupedBySport = {};
  events.forEach(event => {
    const sportKey = event.sport_name || 'Other';
    if (!groupedBySport[sportKey]) {
      groupedBySport[sportKey] = {
        sportId: event.sport_id,
        sportName: sportKey,
        competitions: {}
      };
    }
    const compKey = event.competition_id || 'default';
    if (!groupedBySport[sportKey].competitions[compKey]) {
      groupedBySport[sportKey].competitions[compKey] = [];
    }
    groupedBySport[sportKey].competitions[compKey].push(event);
  });

  // Sort sports by number of events
  const sortedSports = Object.values(groupedBySport).sort(
    (a, b) => Object.values(b.competitions).flat().length - Object.values(a.competitions).flat().length
  );

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-20">
        <div className="w-10 h-10 border-4 border-primary-200 border-t-primary-600 rounded-full animate-spin mb-4" />
        <p className="text-gray-500 text-sm">Loading live events...</p>
      </div>
    );
  }

  if (error && events.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-center">
        <div className="w-16 h-16 bg-red-50 rounded-full flex items-center justify-center mb-4">
          <svg className="w-8 h-8 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
          </svg>
        </div>
        <h3 className="text-lg font-semibold text-gray-800 mb-2">Connection Error</h3>
        <p className="text-sm text-gray-500 max-w-md">{error}</p>
      </div>
    );
  }

  return (
    <div>
      {/* Page header */}
      <div className="mb-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-gray-900">Live Events</h1>
            <p className="text-sm text-gray-500 mt-0.5">{events.length} events across {sortedSports.length} sports</p>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-400">
            <span className="live-dot w-2 h-2 bg-green-500 rounded-full inline-block" />
            Auto-refreshing
          </div>
        </div>
      </div>

      {/* Odds header for 1/X/2 */}
      <div className="hidden sm:flex items-center gap-2 px-3 py-2 text-xs font-semibold text-gray-500 uppercase tracking-wider">
        <div className="w-16 text-center flex-shrink-0">Status</div>
        <div className="flex-1">Event</div>
        <div className="flex gap-1.5 flex-shrink-0">
          <div className="w-16 text-center">1</div>
          <div className="w-16 text-center">X</div>
          <div className="w-16 text-center">2</div>
        </div>
        <div className="w-6 flex-shrink-0" />
      </div>

      {/* Sport groups */}
      {sortedSports.map(sport => {
        const allEvents = Object.values(sport.competitions).flat();
        return (
          <div key={sport.sportId} className="mb-4">
            {/* Sport header */}
            <div className="flex items-center gap-2 px-3 py-2 bg-dark-800 text-white rounded-t-lg">
              <span className="text-lg">{getSportIcon(sport.sportName)}</span>
              <span className="text-sm font-semibold">{sport.sportName}</span>
              <span className="text-xs bg-white/20 px-2 py-0.5 rounded-full ml-auto">
                {allEvents.length} events
              </span>
            </div>

            {/* Events */}
            <div className="bg-white rounded-b-lg shadow-sm border border-gray-200 border-t-0">
              {Object.entries(sport.competitions).map(([compId, compEvents]) => (
                <div key={compId}>
                  {compEvents.map(event => (
                    <EventRow key={event.id} event={event} />
                  ))}
                </div>
              ))}
            </div>
          </div>
        );
      })}

      {events.length === 0 && !loading && (
        <div className="text-center py-20 text-gray-400">
          <div className="text-4xl mb-4">&#x1F3C6;</div>
          <h3 className="text-lg font-semibold mb-1">No events available</h3>
          <p className="text-sm">Events will appear here when they are live</p>
        </div>
      )}
    </div>
  );
}

export default Home;
