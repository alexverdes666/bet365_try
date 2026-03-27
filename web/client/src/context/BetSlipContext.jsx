import { createContext, useContext, useState, useCallback } from 'react';
import api from '../api';
import { useAuth } from './AuthContext';

const BetSlipContext = createContext(null);

export function BetSlipProvider({ children }) {
  const [selections, setSelections] = useState([]);
  const [isOpen, setIsOpen] = useState(false);
  const { user, updateBalance } = useAuth();

  const addSelection = useCallback((selection) => {
    // selection: { eventId, eventName, marketName, selectionName, odds, selectionId }
    setSelections(prev => {
      // Don't add duplicate selections
      const exists = prev.find(s =>
        s.eventId === selection.eventId &&
        s.marketName === selection.marketName &&
        s.selectionId === selection.selectionId
      );
      if (exists) {
        // Remove if clicking same selection (toggle)
        return prev.filter(s => s.selectionId !== selection.selectionId);
      }
      // Replace if same event+market but different selection
      const filtered = prev.filter(s =>
        !(s.eventId === selection.eventId && s.marketName === selection.marketName)
      );
      return [...filtered, { ...selection, stake: '' }];
    });
    setIsOpen(true);
  }, []);

  const removeSelection = useCallback((selectionId) => {
    setSelections(prev => prev.filter(s => s.selectionId !== selectionId));
  }, []);

  const updateStake = useCallback((selectionId, stake) => {
    setSelections(prev => prev.map(s =>
      s.selectionId === selectionId ? { ...s, stake } : s
    ));
  }, []);

  const clearAll = useCallback(() => {
    setSelections([]);
  }, []);

  const placeBet = useCallback(async (selection) => {
    if (!user) throw new Error('Must be logged in');

    const stake = parseFloat(selection.stake);
    if (isNaN(stake) || stake <= 0) throw new Error('Invalid stake');
    if (stake > user.balance) throw new Error('Insufficient balance');

    const res = await api.post('/bets', {
      eventId: selection.eventId,
      eventName: selection.eventName,
      marketName: selection.marketName,
      selectionName: selection.selectionName,
      odds: selection.odds,
      stake
    });

    updateBalance(res.data.newBalance);
    removeSelection(selection.selectionId);

    return res.data;
  }, [user, updateBalance, removeSelection]);

  const placeAllBets = useCallback(async () => {
    const results = [];
    const errors = [];

    for (const sel of selections) {
      try {
        const result = await placeBet(sel);
        results.push(result);
      } catch (err) {
        errors.push({ selection: sel, error: err.response?.data?.error || err.message });
      }
    }

    return { results, errors };
  }, [selections, placeBet]);

  const isSelected = useCallback((selectionId) => {
    return selections.some(s => s.selectionId === selectionId);
  }, [selections]);

  return (
    <BetSlipContext.Provider value={{
      selections,
      isOpen,
      setIsOpen,
      addSelection,
      removeSelection,
      updateStake,
      clearAll,
      placeBet,
      placeAllBets,
      isSelected
    }}>
      {children}
    </BetSlipContext.Provider>
  );
}

export function useBetSlip() {
  const context = useContext(BetSlipContext);
  if (!context) {
    throw new Error('useBetSlip must be used within BetSlipProvider');
  }
  return context;
}
