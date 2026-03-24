import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import api from '../api';
import { getSportIcon } from '../api';
import EventRow from '../components/EventRow';

function Sport() {
  const { sportId } = useParams();
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sportName, setSportName] = useState('');

  useEffect(() => {
    setLoading(true);
    fetchEvents();
    const interval = setInterval(fetchEvents, 5000);
    return () => clearInterval(interval);
  }, [sportId]);

  async function fetchEvents() {
    try {
      const res = await api.get(`/events/sport/${sportId}`);
      const data = Array.isArray(res.data) ? res.data : [];
      setEvents(data);
      if (data.length > 0 && data[0].sport_name) {
        setSportName(data[0].sport_name);
      }
    } catch (err) {
      console.error('Error fetching sport events:', err);
    } finally {
      setLoading(false);
    }
  }

  // Group by competition
  const competitions = {};
  events.forEach(event => {
    const compId = event.competition_id || 'default';
    if (!competitions[compId]) {
      competitions[compId] = [];
    }
    competitions[compId].push(event);
  });

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-20">
        <div className="w-10 h-10 border-4 border-primary-200 border-t-primary-600 rounded-full animate-spin mb-4" />
        <p className="text-gray-500 text-sm">Loading events...</p>
      </div>
    );
  }

  return (
    <div>
      {/* Page header */}
      <div className="mb-4">
        <div className="flex items-center gap-3">
          <span className="text-3xl">{getSportIcon(sportName)}</span>
          <div>
            <h1 className="text-xl font-bold text-gray-900">{sportName || `Sport ${sportId}`}</h1>
            <p className="text-sm text-gray-500 mt-0.5">
              {events.length} live events
              {Object.keys(competitions).length > 1 && ` in ${Object.keys(competitions).length} competitions`}
            </p>
          </div>
        </div>
      </div>

      {/* Odds header */}
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

      {/* Competitions */}
      {Object.entries(competitions).map(([compId, compEvents]) => (
        <div key={compId} className="mb-4">
          <div className="px-3 py-2 bg-gray-100 text-xs font-semibold text-gray-600 uppercase tracking-wider rounded-t-lg border border-gray-200 border-b-0">
            {compId !== 'default' ? compId.replace(/_/g, ' ') : 'Main'}
          </div>
          <div className="bg-white rounded-b-lg shadow-sm border border-gray-200 border-t-0">
            {compEvents.map(event => (
              <EventRow key={event.id} event={event} />
            ))}
          </div>
        </div>
      ))}

      {events.length === 0 && (
        <div className="text-center py-20 text-gray-400">
          <div className="text-4xl mb-4">{getSportIcon(sportName)}</div>
          <h3 className="text-lg font-semibold mb-1">No events for this sport</h3>
          <p className="text-sm">Check back later for live events</p>
        </div>
      )}
    </div>
  );
}

export default Sport;
