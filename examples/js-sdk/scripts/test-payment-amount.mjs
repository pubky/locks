import assert from 'node:assert/strict';
import { paymentAmountToAtomic, formatPaymentAmount } from '../payment-amount.js';
import { buildCreatorLockPolicy } from '../creator-lock-policy.js';

for (const [asset, input, atomic, display] of [
  ['BTC', '50', '50', '₿50'],
  ['USD', '0.05', '5', '0.05 USD'],
  ['USD', '184467440737095516.15', '18446744073709551615', '184467440737095516.15 USD'],
]) {
  const policy = buildCreatorLockPolicy({ lockType: 'paykit-payment', criterionId: 'payment',
    amount: input, asset, recipientPubky: 'authenticated-creator', paykitSetupComplete: true });
  assert.equal(policy.criteria[0].params.amount, atomic);
  assert.equal(policy.criteria[0].params.asset, asset);
  assert.equal(formatPaymentAmount(atomic, asset), display);
}
for (const [amount, asset] of [
  ['0.5', 'BTC'], ['0.001', 'USD'], ['1', 'USDT'],
  ['18446744073709551616', 'BTC'], ['1', 'ETH'], ['1e2', 'USD'],
  ['0', 'USD'], ['-1', 'USD'], ['1 ', 'USD'], [1, 'USD'],
]) assert.throws(() => paymentAmountToAtomic(amount, asset), /payment/);
assert.throws(() => formatPaymentAmount('18446744073709551616', 'USD'), /range/);
console.log('Payment denomination, precision, and range tests passed.');
