import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { useBetSlip } from '../context/BetSlipContext';

function Header() {
  const { user, logout } = useAuth();
  const { selections, setIsOpen } = useBetSlip();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <header className="bg-dark-800 text-white shadow-lg sticky top-0 z-50">
      <div className="flex items-center justify-between px-4 lg:px-6 h-14">
        {/* Logo */}
        <Link to="/" className="flex items-center gap-2 hover:opacity-90 transition-opacity">
          <div className="w-8 h-8 bg-primary-500 rounded-lg flex items-center justify-center font-bold text-sm">
            BS
          </div>
          <span className="text-xl font-bold tracking-tight">
            Bet<span className="text-primary-400">Stream</span>
          </span>
        </Link>

        {/* Navigation */}
        <nav className="hidden md:flex items-center gap-1">
          <Link to="/" className="px-3 py-1.5 text-sm text-gray-300 hover:text-white hover:bg-white/10 rounded transition-colors">
            Live Events
          </Link>
          {user && (
            <>
              <Link to="/my-bets" className="px-3 py-1.5 text-sm text-gray-300 hover:text-white hover:bg-white/10 rounded transition-colors">
                My Bets
              </Link>
              <Link to="/profile" className="px-3 py-1.5 text-sm text-gray-300 hover:text-white hover:bg-white/10 rounded transition-colors">
                Profile
              </Link>
            </>
          )}
          {user?.role === 'admin' && (
            <Link to="/admin" className="px-3 py-1.5 text-sm text-yellow-400 hover:text-yellow-300 hover:bg-white/10 rounded transition-colors">
              Admin
            </Link>
          )}
        </nav>

        {/* Right side */}
        <div className="flex items-center gap-3">
          {/* Bet slip toggle button (mobile) */}
          <button
            onClick={() => setIsOpen(true)}
            className="lg:hidden relative px-3 py-1.5 bg-primary-600 hover:bg-primary-700 rounded text-sm font-medium transition-colors"
          >
            Bet Slip
            {selections.length > 0 && (
              <span className="absolute -top-1.5 -right-1.5 w-5 h-5 bg-red-500 rounded-full text-xs flex items-center justify-center font-bold">
                {selections.length}
              </span>
            )}
          </button>

          {user ? (
            <div className="flex items-center gap-3">
              {/* Balance */}
              <div className="hidden sm:flex items-center gap-1.5 bg-dark-700 px-3 py-1.5 rounded-lg">
                <span className="text-xs text-gray-400">Balance</span>
                <span className="text-sm font-semibold text-green-400">
                  ${user.balance?.toFixed(2)}
                </span>
              </div>

              {/* User menu */}
              <div className="flex items-center gap-2">
                <Link to="/profile" className="text-sm text-gray-300 hover:text-white transition-colors hidden sm:block">
                  {user.name}
                </Link>
                <button
                  onClick={handleLogout}
                  className="px-3 py-1.5 text-sm bg-gray-700 hover:bg-gray-600 rounded transition-colors"
                >
                  Logout
                </button>
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <Link
                to="/login"
                className="px-4 py-1.5 text-sm text-gray-300 hover:text-white transition-colors"
              >
                Login
              </Link>
              <Link
                to="/register"
                className="px-4 py-1.5 text-sm bg-primary-600 hover:bg-primary-700 rounded font-medium transition-colors"
              >
                Register
              </Link>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

export default Header;
