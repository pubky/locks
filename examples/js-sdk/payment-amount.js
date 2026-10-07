const MAX_ATOMIC_AMOUNT = (1n << 64n) - 1n;

function fractionDigits(asset) {
  switch (asset) {
    case 'BTC': return 0; // Creator and reader UI use satoshis.
    case 'USD': return 2;
    default: throw new Error('payment price currency must be BTC or USD');
  }
}

export function paymentAmountToAtomic(amount, asset) {
  const digits = fractionDigits(asset);
  if (typeof amount !== 'string' || !/^\d+(\.\d+)?$/.test(amount) || amount.length > 27) {
    throw new Error('payment amount must be a positive decimal string');
  }
  const [whole, fraction = ''] = amount.split('.');
  if (fraction.length > digits) throw new Error(`payment amount supports at most ${digits} decimal places`);
  const atomic = BigInt(whole + fraction.padEnd(digits, '0'));
  if (atomic === 0n || atomic > MAX_ATOMIC_AMOUNT) throw new Error('payment amount is outside the supported range');
  return atomic.toString();
}

export function formatPaymentAmount(amount, asset) {
  const digits = fractionDigits(asset);
  if (typeof amount !== 'string' || !/^\d+$/.test(amount) || amount.length > 20) {
    throw new Error('invalid atomic payment amount');
  }
  const atomic = BigInt(amount);
  if (atomic === 0n || atomic > MAX_ATOMIC_AMOUNT) throw new Error('payment amount is outside the supported range');
  const padded = atomic.toString().padStart(digits + 1, '0');
  const value = digits ? `${padded.slice(0, -digits)}.${padded.slice(-digits)}` : padded;
  return asset === 'BTC' ? `₿${value}` : `${value} ${asset}`;
}
