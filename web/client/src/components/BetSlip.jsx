import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useBetSlip } from '../context/BetSlipContext';
import { useAuth } from '../context/AuthContext';
import { formatOdds, fractionalToDecimal } from '../api';

function BetSlip() {
  const {
    selections, isOpen, setIsOpen,
    removeSelection, updateStake, clearAll, placeBet, placeAllBets
  } = useBetSlip();
  const { user } = useAuth();
  const navigate = useNavigate();
  const [placingAll, setPlacingAll] = useState(false);
  const [placingId, setPlacingId] = useState(null);
  const [message, setMessage] = useState(null);

  const totalStake = selections.reduce((sum, s) => sum + (parseFloat(s.stake) || 0), 0);
  const totalPotential = selections.reduce((sum, s) => {
    const stake = parseFloat(s.stake) || 0;
    const decimal = fractionalToDecimal(s.odds);
    return sum + (stake * decimal);
  }, 0);

  async function handlePlaceSingle(sel) {
    if (!user) {
      navigate('/login');
      return;
    }
    if (!sel.stake || parseFloat(sel.stake) <= 0) {
      setMessage({ type: 'error', text: 'Enter a valid stake' });
      return;
    }
    setPlacingId(sel.selectionId);
    setMessage(null);
    try {
      await placeBet(sel);
      setMessage({ type: 'success', text: 'Bet placed successfully!' });
      setTimeout(() => setMessage(null), 3000);
    } catch (err) {
      setMessage({ type: 'error', text: err.response?.data?.error || err.message });
    } finally {
      setPlacingId(null);
    }
  }

  async function handlePlaceAll() {
    if (!user) {
      navigate('/login');
      return;
    }
    setPlacingAll(true);
    setMessage(null);
    try {
      const { results, errors } = await placeAllBets();
      if (errors.length > 0) {
        setMessage({ type: 'error', text: errors.map(e => e.error).join(', ') });
      } else {
        setMessage({ type: 'success', text: `${results.length} bet(s) placed!` });
        setTimeout(() => setMessage(null), 3000);
      }
    } catch (err) {
      setMessage({ type: 'error', text: err.message });
    } finally {
      setPlacingAll(false);
    }
  }

  return (
    <>
      {/* Desktop sidebar */}
      <aside className={`
        hidden lg:block w-72 bg-white border-l border-gray-200 overflow-y-auto flex-shrink-0
      `}>
        <BetSlipContent
          selections={selections}
          removeSelection={removeSelection}
          updateStake={updateStake}
          clearAll={clearAll}
          handlePlaceSingle={handlePlaceSingle}
          handlePlaceAll={handlePlaceAll}
          placingAll={placingAll}
          placingId={placingId}
          message={message}
          totalStake={totalStake}
          totalPotential={totalPotential}
          user={user}
          navigate={navigate}
        />
      </aside>

      {/* Mobile overlay */}
      {isOpen && (
        <div className="lg:hidden fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-0 bottom-0 w-80 max-w-full bg-white shadow-xl betslip-enter overflow-y-auto">
            <div className="flex items-center justify-between p-3 border-b border-gray-200">
              <h3 className="font-semibold text-gray-800">Bet Slip</h3>
              <button
                onClick={() => setIsOpen(false)}
                className="w-8 h-8 flex items-center justify-center text-gray-400 hover:text-gray-600 rounded"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <BetSlipContent
              selections={selections}
              removeSelection={removeSelection}
              updateStake={updateStake}
              clearAll={clearAll}
              handlePlaceSingle={handlePlaceSingle}
              handlePlaceAll={handlePlaceAll}
              placingAll={placingAll}
              placingId={placingId}
              message={message}
              totalStake={totalStake}
              totalPotential={totalPotential}
              user={user}
              navigate={navigate}
            />
          </div>
        </div>
      )}
    </>
  );
}

function BetSlipContent({
  selections, removeSelection, updateStake, clearAll,
  handlePlaceSingle, handlePlaceAll, placingAll, placingId,
  message, totalStake, totalPotential, user, navigate
}) {
  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="hidden lg:flex items-center justify-between p-3 border-b border-gray-200">
        <h3 className="font-semibold text-gray-800 text-sm">
          Bet Slip
          {selections.length > 0 && (
            <span className="ml-2 bg-primary-100 text-primary-700 px-2 py-0.5 rounded-full text-xs font-bold">
              {selections.length}
            </span>
          )}
        </h3>
        {selections.length > 0 && (
          <button
            onClick={clearAll}
            className="text-xs text-red-500 hover:text-red-700 transition-colors"
          >
            Clear All
          </button>
        )}
      </div>

      {/* Message */}
      {message && (
        <div className={`mx-3 mt-2 px-3 py-2 rounded text-sm ${
          message.type === 'success' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'
        }`}>
          {message.text}
        </div>
      )}

      {/* Selections */}
      <div className="flex-1 overflow-y-auto">
        {selections.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-gray-400">
            <svg className="w-12 h-12 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
            <p className="text-sm font-medium">No selections</p>
            <p className="text-xs mt-1">Click on odds to add to bet slip</p>
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {selections.map(sel => {
              const decimal = fractionalToDecimal(sel.odds);
              const stake = parseFloat(sel.stake) || 0;
              const potential = stake * decimal;
              const isPlacing = placingId === sel.selectionId;

              return (
                <div key={sel.selectionId} className="p-3">
                  {/* Selection header */}
                  <div className="flex items-start justify-between mb-2">
                    <div className="min-w-0 flex-1">
                      <div className="text-xs text-gray-500 truncate">{sel.eventName}</div>
                      <div className="text-sm font-medium text-gray-800">{sel.selectionName}</div>
                      <div className="text-xs text-gray-500">{sel.marketName}</div>
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0 ml-2">
                      <span className="text-sm font-bold text-primary-600">
                        {formatOdds(sel.odds)}
                      </span>
                      <button
                        onClick={() => removeSelection(sel.selectionId)}
                        className="w-5 h-5 flex items-center justify-center text-gray-400 hover:text-red-500 transition-colors"
                      >
                        <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  </div>

                  {/* Stake input */}
                  <div className="flex items-center gap-2">
                    <div className="relative flex-1">
                      <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400 text-sm">$</span>
                      <input
                        type="number"
                        placeholder="0.00"
                        value={sel.stake}
                        onChange={(e) => updateStake(sel.selectionId, e.target.value)}
                        className="w-full pl-7 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:ring-2 focus:ring-primary-300 focus:border-primary-400 outline-none transition-all"
                        min="0"
                        step="0.01"
                      />
                    </div>
                    <button
                      onClick={() => handlePlaceSingle(sel)}
                      disabled={isPlacing}
                      className="px-3 py-2 bg-green-600 hover:bg-green-700 disabled:bg-gray-300 text-white text-xs font-semibold rounded-lg transition-colors whitespace-nowrap"
                    >
                      {isPlacing ? '...' : 'Place'}
                    </button>
                  </div>

                  {/* Potential win */}
                  {stake > 0 && (
                    <div className="mt-1.5 text-xs text-gray-500 flex justify-between">
                      <span>Potential win:</span>
                      <span className="font-semibold text-green-600">${potential.toFixed(2)}</span>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Footer */}
      {selections.length > 0 && (
        <div className="border-t border-gray-200 p-3 bg-gray-50">
          <div className="flex justify-between text-sm mb-1">
            <span className="text-gray-600">Total Stake:</span>
            <span className="font-semibold">${totalStake.toFixed(2)}</span>
          </div>
          <div className="flex justify-between text-sm mb-3">
            <span className="text-gray-600">Total Potential:</span>
            <span className="font-bold text-green-600">${totalPotential.toFixed(2)}</span>
          </div>

          {user ? (
            <button
              onClick={handlePlaceAll}
              disabled={placingAll || totalStake <= 0}
              className="w-full py-2.5 bg-green-600 hover:bg-green-700 disabled:bg-gray-300 text-white text-sm font-semibold rounded-lg transition-colors"
            >
              {placingAll ? 'Placing Bets...' : `Place All Bets ($${totalStake.toFixed(2)})`}
            </button>
          ) : (
            <button
              onClick={() => navigate('/login')}
              className="w-full py-2.5 bg-primary-600 hover:bg-primary-700 text-white text-sm font-semibold rounded-lg transition-colors"
            >
              Login to Place Bets
            </button>
          )}
        </div>
      )}
    </div>
  );
}

export default BetSlip;
