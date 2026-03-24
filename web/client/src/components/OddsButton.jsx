import { useBetSlip } from '../context/BetSlipContext';
import { formatOdds } from '../api';

function OddsButton({ selection, eventId, eventName, marketName }) {
  const { addSelection, isSelected } = useBetSlip();
  const selected = isSelected(selection.id);

  if (selection.suspended === '1') {
    return (
      <button
        disabled
        className="w-full py-2 px-3 text-center bg-gray-100 text-gray-400 rounded text-sm font-medium cursor-not-allowed"
      >
        <svg className="w-4 h-4 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
        </svg>
      </button>
    );
  }

  const decimalOdds = formatOdds(selection.odds);

  function handleClick() {
    // Determine selection name from order
    let selName = selection.name;
    if (!selName) {
      const names = ['1', 'X', '2'];
      selName = names[parseInt(selection.order)] || `Selection ${selection.order}`;
    }

    addSelection({
      eventId,
      eventName,
      marketName,
      selectionName: selName,
      selectionId: selection.id,
      odds: selection.odds
    });
  }

  return (
    <button
      onClick={handleClick}
      className={`w-full py-2 px-3 text-center rounded text-sm font-semibold transition-all duration-150
        ${selected
          ? 'bg-primary-600 text-white shadow-sm ring-2 ring-primary-300'
          : 'bg-gray-50 text-gray-800 hover:bg-primary-50 hover:text-primary-700 border border-gray-200 hover:border-primary-300'
        }`}
    >
      {decimalOdds}
    </button>
  );
}

export default OddsButton;
