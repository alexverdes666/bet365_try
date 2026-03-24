import { useState, useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import api from '../api';
import { getSportIcon } from '../api';

function SportSidebar() {
  const [sports, setSports] = useState([]);
  const [eventCounts, setEventCounts] = useState({});
  const [collapsed, setCollapsed] = useState(false);
  const { sportId: activeSportId } = useParams();

  useEffect(() => {
    fetchSports();
    const interval = setInterval(fetchSports, 15000);
    return () => clearInterval(interval);
  }, []);

  async function fetchSports() {
    try {
      // Fetch both sports list and all events for counts
      const [sportsRes, eventsRes] = await Promise.all([
        api.get('/events/sports').catch(() => ({ data: [] })),
        api.get('/events').catch(() => ({ data: [] }))
      ]);

      let sportsList = [];
      const events = Array.isArray(eventsRes.data) ? eventsRes.data : [];

      // Build counts from events
      const counts = {};
      const sportNames = {};
      events.forEach(ev => {
        const sid = ev.sport_id;
        counts[sid] = (counts[sid] || 0) + 1;
        if (ev.sport_name && !sportNames[sid]) {
          sportNames[sid] = ev.sport_name;
        }
      });

      // Use the sports API if available
      if (Array.isArray(sportsRes.data) && sportsRes.data.length > 0) {
        sportsList = sportsRes.data;
      } else {
        // Build from events data
        sportsList = Object.entries(sportNames).map(([id, name]) => ({
          id,
          name
        }));
      }

      // Sort by event count descending
      sportsList.sort((a, b) => (counts[b.id] || 0) - (counts[a.id] || 0));

      setSports(sportsList);
      setEventCounts(counts);
    } catch (err) {
      console.error('Error fetching sports:', err);
    }
  }

  return (
    <>
      {/* Mobile toggle */}
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="lg:hidden fixed bottom-4 left-4 z-40 w-12 h-12 bg-dark-800 text-white rounded-full shadow-lg flex items-center justify-center"
      >
        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>

      {/* Sidebar */}
      <aside className={`
        ${collapsed ? 'translate-x-0' : '-translate-x-full'}
        lg:translate-x-0
        fixed lg:static inset-y-0 left-0 z-30
        w-56 bg-white border-r border-gray-200
        overflow-y-auto transition-transform duration-200
        mt-14 lg:mt-0
        shadow-lg lg:shadow-none
      `}>
        {/* Close button on mobile */}
        <button
          onClick={() => setCollapsed(false)}
          className="lg:hidden absolute top-2 right-2 w-8 h-8 flex items-center justify-center text-gray-400 hover:text-gray-600"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>

        <div className="p-3">
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider px-2 mb-2">
            Sports
          </h3>

          {/* All sports link */}
          <Link
            to="/"
            onClick={() => setCollapsed(false)}
            className={`flex items-center justify-between px-3 py-2 rounded-lg text-sm transition-colors mb-0.5
              ${!activeSportId ? 'bg-primary-50 text-primary-700 font-medium' : 'text-gray-700 hover:bg-gray-50'}`}
          >
            <div className="flex items-center gap-2">
              <span className="text-base">&#x1F3C6;</span>
              <span>All Sports</span>
            </div>
            <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
              {Object.values(eventCounts).reduce((a, b) => a + b, 0)}
            </span>
          </Link>

          {/* Sports list */}
          <div className="space-y-0.5">
            {sports.map(sport => {
              const count = eventCounts[sport.id] || 0;
              const isActive = activeSportId === sport.id;
              const sportName = sport.name || sport.sport_name || 'Unknown';

              return (
                <Link
                  key={sport.id}
                  to={`/sport/${sport.id}`}
                  onClick={() => setCollapsed(false)}
                  className={`flex items-center justify-between px-3 py-2 rounded-lg text-sm transition-colors
                    ${isActive ? 'bg-primary-50 text-primary-700 font-medium' : 'text-gray-700 hover:bg-gray-50'}`}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-base">{getSportIcon(sportName)}</span>
                    <span className="truncate">{sportName}</span>
                  </div>
                  {count > 0 && (
                    <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full flex-shrink-0">
                      {count}
                    </span>
                  )}
                </Link>
              );
            })}
          </div>

          {sports.length === 0 && (
            <div className="text-center text-gray-400 text-sm py-8">
              <div className="text-2xl mb-2">&#x1F50C;</div>
              No sports available
            </div>
          )}
        </div>
      </aside>

      {/* Mobile overlay */}
      {collapsed && (
        <div
          className="lg:hidden fixed inset-0 bg-black/30 z-20 mt-14"
          onClick={() => setCollapsed(false)}
        />
      )}
    </>
  );
}

export default SportSidebar;
