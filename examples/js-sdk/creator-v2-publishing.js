import { CreateContentLockV2RequestBuilder } from '../../locks-sdk/bindings/js/pkg/locks_sdk_wasm.js';

function baseBuilder({ primaryResource, secondaryResources = [], accessTtlSeconds = 3600, lockServer }) {
  let builder = new CreateContentLockV2RequestBuilder()
    .primaryResource(primaryResource)
    .verificationWindowSeconds(300)
    .accessPolicy({ requested_credential_ttl_seconds: accessTtlSeconds })
    .lockServer({ override: lockServer });
  for (const resource of secondaryResources) builder = builder.secondaryResource(resource);
  return builder;
}

export function buildGraphOnlyV2(options) {
  return baseBuilder(options)
    .graph({
      predicates: [{ type: 'follows', follower: 'reader', followed: 'creator' }],
    })
    .build();
}

export function buildBtcPaymentOnlyV2({ creatorPubky, amountSats, minimumConfirmations = 1, ...options }) {
  return baseBuilder(options)
    .payment({
      recipient_pubky: creatorPubky,
      amount: String(amountSats),
      asset: 'BTC',
      minimum_confirmations: minimumConfirmations,
    })
    .build();
}

export function buildMixedV2(options) {
  return baseBuilder(options)
    .payment({
      recipient_pubky: options.creatorPubky,
      amount: String(options.amountSats),
      asset: 'BTC',
      minimum_confirmations: options.minimumConfirmations ?? 1,
    })
    .graph({
      predicates: [{ type: 'follows', follower: 'reader', followed: 'creator' }],
    })
    .build();
}

export async function publishV2(session, request) {
  return session.creator.createContentLock(request);
}
