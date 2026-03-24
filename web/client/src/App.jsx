import { Routes, Route } from 'react-router-dom';
import Header from './components/Header';
import SportSidebar from './components/SportSidebar';
import BetSlip from './components/BetSlip';
import ProtectedRoute from './components/ProtectedRoute';
import Home from './pages/Home';
import Sport from './pages/Sport';
import Event from './pages/Event';
import Login from './pages/Login';
import Register from './pages/Register';
import MyBets from './pages/MyBets';
import Profile from './pages/Profile';
import Admin from './pages/Admin';

function App() {
  return (
    <div className="min-h-screen bg-[#FAFAFA] flex flex-col">
      <Header />
      <div className="flex flex-1">
        <SportSidebar />
        <main className="flex-1 p-4 lg:p-6 min-w-0">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/sport/:sportId" element={<Sport />} />
            <Route path="/event/:eventId" element={<Event />} />
            <Route path="/login" element={<Login />} />
            <Route path="/register" element={<Register />} />
            <Route path="/my-bets" element={
              <ProtectedRoute>
                <MyBets />
              </ProtectedRoute>
            } />
            <Route path="/profile" element={
              <ProtectedRoute>
                <Profile />
              </ProtectedRoute>
            } />
            <Route path="/admin" element={
              <ProtectedRoute adminOnly>
                <Admin />
              </ProtectedRoute>
            } />
          </Routes>
        </main>
        <BetSlip />
      </div>
    </div>
  );
}

export default App;
