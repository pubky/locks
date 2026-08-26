#!/usr/bin/env node
import net from 'node:net';

const host = process.env.ELECTRUM_HOST ?? 'fulcrum';
const port = Number.parseInt(process.env.ELECTRUM_PORT ?? '50001', 10);
const attempts = Number.parseInt(process.env.ELECTRUM_READY_ATTEMPTS ?? '180', 10);
if (!/^[A-Za-z0-9.-]+$/.test(host) || !Number.isSafeInteger(port) || port < 1 || port > 65535) {
  throw new Error('invalid Electrum readiness configuration');
}

function probe() {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    let buffer = '';
    const timeout = setTimeout(() => socket.destroy(new Error('timeout')), 2_000);
    socket.setEncoding('utf8');
    socket.on('connect', () => {
      socket.write('{"jsonrpc":"2.0","id":1,"method":"server.version","params":["pubky-locks-compose","1.4"]}\n');
    });
    socket.on('data', (chunk) => {
      buffer += chunk;
      if (buffer.length > 16_384) socket.destroy(new Error('oversized response'));
      const newline = buffer.indexOf('\n');
      if (newline === -1) return;
      try {
        const response = JSON.parse(buffer.slice(0, newline));
        if (
          response?.id !== 1
          || response.error != null
          || !Array.isArray(response.result)
          || response.result.length !== 2
          || !response.result.every((value) => typeof value === 'string' && value.length > 0)
        ) {
          throw new Error('invalid response');
        }
        clearTimeout(timeout);
        socket.end();
        resolve();
      } catch (error) {
        socket.destroy(error);
      }
    });
    socket.on('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

let ready = false;
for (let attempt = 0; attempt < attempts; attempt += 1) {
  try {
    await probe();
    ready = true;
    break;
  } catch {
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }
}
if (!ready) throw new Error('Electrum readiness timed out');
console.log('Electrum protocol ready');
