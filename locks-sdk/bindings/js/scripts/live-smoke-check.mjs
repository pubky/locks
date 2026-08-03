const required = [
  ['LOCKS_LIVE_LOCK_SERVER', 'Lock Server Pubky with a browser-usable PKARR endpoint'],
  ['LOCKS_LIVE_PKARR_RELAY', 'PKARR relay URL; local pubky-testnet uses http://localhost:15411'],
  ['LOCKS_LIVE_CREATOR', 'Creator Pubky that publishes /pub/locks.app/config.json'],
  ['LOCKS_LIVE_CONTENT_LOCK_RESOURCE', 'Canonical pubky.../pub/locks.app/<lock_id>.json resource'],
  ['LOCKS_LIVE_DEMO_ORIGIN', 'Origin allowed by creator_authority_acquisition.legacy_connect.allowed_return_origins'],
];

console.log('SDK live browser smoke prerequisite check');
const missing = [];
for (const [name, description] of required) {
  const value = process.env[name];
  if (value && value.trim()) {
    console.log(`READY   ${name}: ${value}`);
  } else {
    missing.push(name);
    console.log(`MISSING ${name}: ${description}`);
  }
}

console.log('\nManual smoke sequence once prerequisites exist:');
console.log('1. npm --prefix locks-sdk/bindings/js run build');
console.log('2. python3 -m http.server 8080 --directory locks-sdk/bindings/js');
console.log('3. Open http://localhost:8080/demo/ from the allowed origin / configured browser context.');
console.log('4. Build LocksOptions and add LOCKS_LIVE_PKARR_RELAY with addPkarrRelay().');
console.log('5. Verify Locks.forServerWithOptions/createConnectUrl/exchangeFrontendSessionCode against the live Lock Server.');
console.log('6. Verify Locks.forCreatorWithOptions and Locks.readContentLockWithOptions using the creator and content-lock resource.');
console.log('7. Verify submitProofBundle -> lookupVerificationTask -> issueAccessCredential -> proxyReadGuardedResource for a known satisfiable proof.');

if (missing.length > 0) {
  console.log(`\nLive smoke is not runnable yet: ${missing.length} prerequisite(s) missing.`);
  console.log('This command is informational and exits 0 because live credentials/endpoints are intentionally not required for normal SDK verification.');
} else {
  console.log('\nAll live-smoke prerequisites are present. Run the manual smoke sequence above.');
}
