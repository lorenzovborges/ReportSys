const config = {
  _id: 'rs0',
  members: [
    { _id: 0, host: 'mongo1:27017', priority: 2 },
    { _id: 1, host: 'mongo2:27017', priority: 1 },
    { _id: 2, host: 'mongo3:27017', priority: 0, hidden: true, votes: 0 }
  ]
};

try {
  rs.initiate(config);
} catch (error) {
  print('rs.initiate failed, trying status check:', error.message);
}

let ready = false;
for (let attempt = 0; attempt < 60; attempt += 1) {
  try {
    const status = rs.status();
    if (status.ok === 1) {
      ready = true;
      break;
    }
  } catch (error) {
    // keep waiting
  }
  sleep(2000);
}

if (!ready) {
  throw new Error('Replica set rs0 did not become ready in time');
}

print('Replica set initialized and ready');
