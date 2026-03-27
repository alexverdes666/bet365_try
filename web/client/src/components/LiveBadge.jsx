function LiveBadge({ minute, context }) {
  const isLive = context !== 'Full time' && context !== 'Ended' && minute;
  const isFinished = context === 'Full time' || context === 'Ended';

  if (isFinished) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600">
        FT
      </span>
    );
  }

  if (isLive) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-semibold bg-red-50 text-red-600">
        <span className="live-dot w-1.5 h-1.5 bg-red-500 rounded-full inline-block" />
        {minute}'
      </span>
    );
  }

  if (context) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-orange-50 text-orange-600">
        {context}
      </span>
    );
  }

  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-600">
      <span className="live-dot w-1.5 h-1.5 bg-blue-500 rounded-full inline-block" />
      LIVE
    </span>
  );
}

export default LiveBadge;
