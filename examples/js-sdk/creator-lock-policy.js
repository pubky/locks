const DEV_STATIC = 'dev-static';
const PAYKIT_PAYMENT = 'paykit-payment';

export function buildCreatorLockPolicy({
  lockType = DEV_STATIC,
  criterionId,
  devStaticSatisfied = true,
  amountSats,
  recipientPubky,
  paykitSetupComplete = false,
} = {}) {
  const normalizedCriterionId = criterionId?.trim();
  if (!normalizedCriterionId) throw new Error('criterion ID is required');

  let criterion;
  if (lockType === DEV_STATIC) {
    if (typeof devStaticSatisfied !== 'boolean') {
      throw new Error('dev-static satisfied must be a boolean');
    }
    criterion = {
      criterion_id: normalizedCriterionId,
      verifier_type: DEV_STATIC,
      params: { satisfied: devStaticSatisfied },
    };
  } else if (lockType === PAYKIT_PAYMENT) {
    if (!paykitSetupComplete) {
      throw new Error('complete Paykit setup for the authenticated creator before publishing');
    }
    if (typeof recipientPubky !== 'string' || !recipientPubky) {
      throw new Error('paykit-payment requires the authenticated creator recipient');
    }
    if (
      typeof amountSats !== 'string'
      || !amountSats
      || !/^\d+$/.test(amountSats)
      || !/[1-9]/.test(amountSats)
    ) {
      throw new Error('paykit-payment amount must be a positive decimal integer string');
    }
    criterion = {
      criterion_id: normalizedCriterionId,
      verifier_type: PAYKIT_PAYMENT,
      params: {
        recipient_pubky: recipientPubky,
        amount: amountSats,
        asset: 'BTC',
      },
    };
  } else {
    throw new Error(`unsupported lock type: ${lockType}`);
  }

  return {
    criteria: [criterion],
    lockLogic: { type: 'all', criteria: [normalizedCriterionId] },
  };
}
