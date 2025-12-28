const WebSocket = require('ws');

const ws = new WebSocket('ws://127.0.0.1:3000/ws?user_id=test_user_1');

ws.on('open', function open() {
  console.log('Connected to server');

  // Subscribe to BTC/USD
  const msg = JSON.stringify({
    action: 'subscribe',
    symbol: 'BTC/USD'
  });
  console.log('Sending:', msg);
  ws.send(msg);
});

ws.on('message', function message(data) {
  console.log('Received:', data.toString());
});

ws.on('error', console.error);
ws.on('close', () => console.log('Disconnected'));
