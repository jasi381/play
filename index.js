const express = require('express');
const fs = require('fs');
const path = require('path');
const { PubSub, v1 } = require('@google-cloud/pubsub');
const { google } = require('googleapis');

// ============ NOTIFICATION TYPES ============
const NOTIFICATION_TYPES = {
  1: 'SUBSCRIPTION_RECOVERED',
  2: 'SUBSCRIPTION_RENEWED',
  3: 'SUBSCRIPTION_CANCELED',
  4: 'SUBSCRIPTION_PURCHASED',
  5: 'SUBSCRIPTION_ON_HOLD',
  6: 'SUBSCRIPTION_IN_GRACE_PERIOD',
  7: 'SUBSCRIPTION_RESTARTED',
  8: 'SUBSCRIPTION_PRICE_CHANGE_CONFIRMED',
  9: 'SUBSCRIPTION_DEFERRED',
  10: 'SUBSCRIPTION_PAUSED',
  11: 'SUBSCRIPTION_PAUSE_SCHEDULE_CHANGED',
  12: 'SUBSCRIPTION_REVOKED',
  13: 'SUBSCRIPTION_EXPIRED',
  20: 'SUBSCRIPTION_PENDING_PURCHASE_CANCELED'
};

// ============ TIME HELPERS ============
const toIST = (date) => {
  if (!date) return null;
  const d = new Date(date);
  return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) + ' IST';
};

const millisToIST = (millis) => {
  if (!millis) return null;
  const d = new Date(parseInt(millis));
  return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) + ' IST';
};

const app = express();
const PORT = process.env.PORT || 3000;

// Separate storage files for push and pull
const PUSH_DATA_FILE = path.join(__dirname, 'push-data.json');
const PULL_DATA_FILE = path.join(__dirname, 'pull-data.json');

// Google Cloud Pub/Sub config (set via environment variables)
const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT_ID;
const SUBSCRIPTION_NAME = process.env.PUBSUB_SUBSCRIPTION || 'play-subscription';

app.use(express.json());

// ============ STORAGE HELPERS ============
const loadData = (file) => {
  if (fs.existsSync(file)) {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  }
  return [];
};

const saveData = (file, data) => {
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
};

// Load existing data
let pushStore = loadData(PUSH_DATA_FILE);
let pullStore = loadData(PULL_DATA_FILE);

// ============ PUSH ENDPOINTS (Google sends notifications here) ============

// POST - receive push notification from Google Play
app.post('/push', (req, res) => {
  const id = Date.now().toString();

  // Google sends base64 encoded message in req.body.message.data
  let decodedData = req.body;
  if (req.body?.message?.data) {
    try {
      const buffer = Buffer.from(req.body.message.data, 'base64');
      decodedData = JSON.parse(buffer.toString('utf8'));
    } catch (e) {
      decodedData = req.body;
    }
  }

  const entry = {
    id,
    type: 'push',
    data: decodedData,
    rawPayload: req.body,
    createdAt: new Date().toISOString()
  };

  pushStore.push(entry);
  saveData(PUSH_DATA_FILE, pushStore);

  // Must respond with 200/204 to acknowledge receipt
  res.status(200).json({ success: true, id });
});

// GET - get all push notifications
app.get('/push', (req, res) => {
  res.json(pushStore);
});

// GET - get specific push notification by id
app.get('/push/:id', (req, res) => {
  const entry = pushStore.find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Not found' });
  res.json(entry);
});

// DELETE - remove push notification by id
app.delete('/push/:id', (req, res) => {
  const index = pushStore.findIndex(e => e.id === req.params.id);
  if (index === -1) return res.status(404).json({ error: 'Not found' });
  pushStore.splice(index, 1);
  saveData(PUSH_DATA_FILE, pushStore);
  res.status(204).send();
});

// ============ PULL ENDPOINTS (Pull from Google Pub/Sub) ============

// POST - manually trigger pull from Google Pub/Sub (synchronous pull)
app.post('/pull', async (req, res) => {
  if (!PROJECT_ID) {
    return res.status(400).json({
      error: 'GCP_PROJECT_ID environment variable not set',
      hint: 'Set GOOGLE_CLOUD_PROJECT or GCP_PROJECT_ID env var'
    });
  }

  try {
    const subClient = new v1.SubscriberClient();
    const subscriptionPath = `projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION_NAME}`;

    // Pull messages using v1 API
    const [response] = await subClient.pull({
      subscription: subscriptionPath,
      maxMessages: 10,
    });

    const messages = response.receivedMessages || [];

    if (messages.length === 0) {
      await subClient.close();
      return res.json({ message: 'No new messages', pulled: 0 });
    }

    const pulledEntries = [];
    const ackIds = [];

    for (const msg of messages) {
      const id = Date.now().toString() + '-' + msg.message.messageId;

      let decodedData;
      try {
        const dataStr = msg.message.data.toString('utf8');
        decodedData = JSON.parse(dataStr);
      } catch (e) {
        decodedData = msg.message.data.toString('utf8');
      }

      const entry = {
        id,
        type: 'pull',
        messageId: msg.message.messageId,
        data: decodedData,
        attributes: msg.message.attributes,
        publishTime: msg.message.publishTime,
        pulledAt: new Date().toISOString()
      };

      pullStore.push(entry);
      pulledEntries.push(entry);
      ackIds.push(msg.ackId);
    }

    // Acknowledge messages so they don't get pulled again
    if (ackIds.length > 0) {
      await subClient.acknowledge({
        subscription: subscriptionPath,
        ackIds: ackIds,
      });
    }

    await subClient.close();
    saveData(PULL_DATA_FILE, pullStore);

    res.json({
      message: `Pulled ${pulledEntries.length} messages`,
      pulled: pulledEntries.length,
      entries: pulledEntries
    });

  } catch (error) {
    console.error('Pull error:', error);
    res.status(500).json({
      error: 'Failed to pull messages',
      details: error.message
    });
  }
});

// POST - start continuous pull listener (streaming)
let pullListener = null;

app.post('/pull/start', (req, res) => {
  if (!PROJECT_ID) {
    return res.status(400).json({
      error: 'GCP_PROJECT_ID environment variable not set'
    });
  }

  if (pullListener) {
    return res.json({ message: 'Pull listener already running' });
  }

  try {
    const pubsub = new PubSub({ projectId: PROJECT_ID });
    const subscription = pubsub.subscription(SUBSCRIPTION_NAME);

    pullListener = subscription.on('message', (message) => {
      const id = Date.now().toString() + '-' + message.id;

      let decodedData;
      try {
        decodedData = JSON.parse(message.data.toString('utf8'));
      } catch (e) {
        decodedData = message.data.toString('utf8');
      }

      const entry = {
        id,
        type: 'pull-stream',
        messageId: message.id,
        data: decodedData,
        attributes: message.attributes,
        publishTime: message.publishTime,
        pulledAt: new Date().toISOString()
      };

      pullStore.push(entry);
      saveData(PULL_DATA_FILE, pullStore);

      message.ack();
      console.log(`Received and stored message: ${message.id}`);
    });

    subscription.on('error', (error) => {
      console.error('Subscription error:', error);
    });

    res.json({ message: 'Pull listener started', subscription: SUBSCRIPTION_NAME });

  } catch (error) {
    res.status(500).json({ error: 'Failed to start listener', details: error.message });
  }
});

// POST - stop continuous pull listener
app.post('/pull/stop', (req, res) => {
  if (!pullListener) {
    return res.json({ message: 'No pull listener running' });
  }

  pullListener.removeAllListeners();
  pullListener = null;
  res.json({ message: 'Pull listener stopped' });
});

// GET - get all pulled notifications
app.get('/pull', (req, res) => {
  res.json(pullStore);
});

// GET - get specific pulled notification by id
app.get('/pull/:id', (req, res) => {
  const entry = pullStore.find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Not found' });
  res.json(entry);
});

// DELETE - remove pulled notification by id
app.delete('/pull/:id', (req, res) => {
  const index = pullStore.findIndex(e => e.id === req.params.id);
  if (index === -1) return res.status(404).json({ error: 'Not found' });
  pullStore.splice(index, 1);
  saveData(PULL_DATA_FILE, pullStore);
  res.status(204).send();
});

// ============ LEGACY /data ENDPOINTS (keep for backwards compatibility) ============

const DATA_FILE = path.join(__dirname, 'data.json');
let store = loadData(DATA_FILE);

app.post('/data', (req, res) => {
  const id = Date.now().toString();
  const entry = { id, data: req.body, createdAt: new Date().toISOString() };
  store.push(entry);
  saveData(DATA_FILE, store);
  res.status(201).json(entry);
});

app.get('/data', (req, res) => {
  res.json(store);
});

app.get('/data/:id', (req, res) => {
  const entry = store.find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Not found' });
  res.json(entry);
});

app.delete('/data/:id', (req, res) => {
  const index = store.findIndex(e => e.id === req.params.id);
  if (index === -1) return res.status(404).json({ error: 'Not found' });
  store.splice(index, 1);
  saveData(DATA_FILE, store);
  res.status(204).send();
});

// ============ GOOGLE PLAY API CLIENT ============

const getPlayApiClient = async () => {
  const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/androidpublisher']
  });
  const authClient = await auth.getClient();
  return google.androidpublisher({ version: 'v3', auth: authClient });
};

// Fetch subscription details using v2 API
const getSubscriptionDetailsV2 = async (packageName, purchaseToken) => {
  try {
    const client = await getPlayApiClient();
    const result = await client.purchases.subscriptionsv2.get({
      packageName: packageName,
      token: purchaseToken
    });
    return result.data;
  } catch (error) {
    console.error('Error fetching subscription v2:', error.message);
    return null;
  }
};

// Fetch subscription details using v1 API (fallback)
const getSubscriptionDetailsV1 = async (packageName, subscriptionId, purchaseToken) => {
  try {
    const client = await getPlayApiClient();
    const result = await client.purchases.subscriptions.get({
      packageName: packageName,
      subscriptionId: subscriptionId,
      token: purchaseToken
    });
    return result.data;
  } catch (error) {
    console.error('Error fetching subscription v1:', error.message);
    return null;
  }
};

// ============ SUBSCRIPTION DETAILS STORAGE ============
const SUBSCRIPTIONS_FILE = path.join(__dirname, 'subscriptions-data.json');
let subscriptionsStore = loadData(SUBSCRIPTIONS_FILE);

// ============ SUBSCRIPTION ENDPOINTS ============

// GET - get all enriched subscription details
app.get('/subscriptions', (req, res) => {
  res.json(subscriptionsStore);
});

// POST - fetch and enrich all pulled notifications with full subscription details
app.post('/subscriptions/fetch', async (req, res) => {
  try {
    const results = [];
    const errors = [];

    for (const notification of pullStore) {
      const data = notification.data;

      // Skip if not a subscription notification
      if (!data?.subscriptionNotification) {
        continue;
      }

      const subNotif = data.subscriptionNotification;
      const packageName = data.packageName;
      const purchaseToken = subNotif.purchaseToken;
      const subscriptionId = subNotif.subscriptionId;
      const notificationType = subNotif.notificationType;

      // Check if already processed
      const existing = subscriptionsStore.find(s =>
        s.messageId === notification.messageId
      );
      if (existing) {
        results.push({ ...existing, status: 'already_processed' });
        continue;
      }

      // Fetch full details from Google Play API
      let subscriptionDetails = await getSubscriptionDetailsV2(packageName, purchaseToken);
      let apiVersion = 'v2';

      if (!subscriptionDetails) {
        subscriptionDetails = await getSubscriptionDetailsV1(packageName, subscriptionId, purchaseToken);
        apiVersion = 'v1';
      }

      // Extract user info and subscription status
      let userAccountId = null;
      let userProfileId = null;
      let subscriptionState = null;
      let expiryTime = null;
      let expiryTimeIST = null;
      let startTime = null;
      let startTimeIST = null;

      if (subscriptionDetails) {
        if (apiVersion === 'v2') {
          const externalIds = subscriptionDetails.externalAccountIdentifiers || {};
          userAccountId = externalIds.obfuscatedExternalAccountId || null;
          userProfileId = externalIds.obfuscatedExternalProfileId || null;
          subscriptionState = subscriptionDetails.subscriptionState || null;

          const lineItems = subscriptionDetails.lineItems || [];
          if (lineItems.length > 0) {
            expiryTime = lineItems[0].expiryTime || null;
            expiryTimeIST = toIST(expiryTime);
          }
          startTime = subscriptionDetails.startTime || null;
          startTimeIST = toIST(startTime);
        } else {
          userAccountId = subscriptionDetails.obfuscatedExternalAccountId || null;
          userProfileId = subscriptionDetails.obfuscatedExternalProfileId || null;
          expiryTime = subscriptionDetails.expiryTimeMillis || null;
          expiryTimeIST = millisToIST(expiryTime);
          startTime = subscriptionDetails.startTimeMillis || null;
          startTimeIST = millisToIST(startTime);
        }
      }

      const enrichedEntry = {
        id: Date.now().toString() + '-' + notification.messageId,
        messageId: notification.messageId,

        // Notification info
        notification: {
          type: notificationType,
          typeName: NOTIFICATION_TYPES[notificationType] || 'UNKNOWN',
          eventTime: data.eventTimeMillis,
          eventTimeIST: millisToIST(data.eventTimeMillis)
        },

        // Package & subscription
        packageName: packageName,
        subscriptionId: subscriptionId,
        purchaseToken: purchaseToken,

        // User identification
        user: {
          obfuscatedAccountId: userAccountId,
          obfuscatedProfileId: userProfileId
        },

        // Subscription status
        subscription: {
          state: subscriptionState,
          startTime: startTime,
          startTimeIST: startTimeIST,
          expiryTime: expiryTime,
          expiryTimeIST: expiryTimeIST
        },

        // Full API response
        apiResponse: subscriptionDetails,
        apiVersion: apiVersion,

        // Metadata
        pulledAt: notification.pulledAt,
        processedAt: new Date().toISOString(),
        processedAtIST: toIST(new Date())
      };

      subscriptionsStore.push(enrichedEntry);
      results.push({ ...enrichedEntry, status: 'processed' });
    }

    saveData(SUBSCRIPTIONS_FILE, subscriptionsStore);

    res.json({
      message: `Processed ${results.length} subscription notifications`,
      total: results.length,
      errors: errors.length,
      subscriptions: results
    });

  } catch (error) {
    console.error('Subscription fetch error:', error);
    res.status(500).json({
      error: 'Failed to fetch subscription details',
      details: error.message
    });
  }
});

// GET - get specific subscription by id
app.get('/subscriptions/:id', (req, res) => {
  const entry = subscriptionsStore.find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Not found' });
  res.json(entry);
});

// POST - process a single purchase token
app.post('/subscriptions/lookup', async (req, res) => {
  const { packageName, purchaseToken, subscriptionId } = req.body;

  if (!packageName || !purchaseToken) {
    return res.status(400).json({
      error: 'Missing required fields',
      required: ['packageName', 'purchaseToken']
    });
  }

  try {
    let subscriptionDetails = await getSubscriptionDetailsV2(packageName, purchaseToken);
    let apiVersion = 'v2';

    if (!subscriptionDetails && subscriptionId) {
      subscriptionDetails = await getSubscriptionDetailsV1(packageName, subscriptionId, purchaseToken);
      apiVersion = 'v1';
    }

    if (!subscriptionDetails) {
      return res.status(404).json({ error: 'Subscription not found or API error' });
    }

    // Extract details
    let userAccountId = null;
    let userProfileId = null;
    let subscriptionState = null;
    let expiryTime = null;
    let startTime = null;

    if (apiVersion === 'v2') {
      const externalIds = subscriptionDetails.externalAccountIdentifiers || {};
      userAccountId = externalIds.obfuscatedExternalAccountId;
      userProfileId = externalIds.obfuscatedExternalProfileId;
      subscriptionState = subscriptionDetails.subscriptionState;
      const lineItems = subscriptionDetails.lineItems || [];
      if (lineItems.length > 0) {
        expiryTime = lineItems[0].expiryTime;
      }
      startTime = subscriptionDetails.startTime;
    } else {
      userAccountId = subscriptionDetails.obfuscatedExternalAccountId;
      userProfileId = subscriptionDetails.obfuscatedExternalProfileId;
      expiryTime = subscriptionDetails.expiryTimeMillis;
      startTime = subscriptionDetails.startTimeMillis;
    }

    res.json({
      packageName,
      subscriptionId,
      user: {
        obfuscatedAccountId: userAccountId,
        obfuscatedProfileId: userProfileId
      },
      subscription: {
        state: subscriptionState,
        startTime: startTime,
        startTimeIST: apiVersion === 'v2' ? toIST(startTime) : millisToIST(startTime),
        expiryTime: expiryTime,
        expiryTimeIST: apiVersion === 'v2' ? toIST(expiryTime) : millisToIST(expiryTime)
      },
      apiVersion,
      apiResponse: subscriptionDetails,
      processedAtIST: toIST(new Date())
    });

  } catch (error) {
    console.error('Lookup error:', error);
    res.status(500).json({
      error: 'Failed to lookup subscription',
      details: error.message
    });
  }
});

// DELETE - clear subscriptions store
app.delete('/subscriptions', (req, res) => {
  subscriptionsStore = [];
  saveData(SUBSCRIPTIONS_FILE, subscriptionsStore);
  res.status(204).send();
});

// ============ STATUS ENDPOINT ============

app.get('/status', (req, res) => {
  res.json({
    status: 'ok',
    pullListenerActive: !!pullListener,
    counts: {
      push: pushStore.length,
      pull: pullStore.length,
      subscriptions: subscriptionsStore.length,
      data: store.length
    },
    config: {
      projectId: PROJECT_ID ? '***configured***' : 'NOT SET',
      subscription: SUBSCRIPTION_NAME
    }
  });
});

app.listen(PORT, () => {
  console.log(`API running at http://localhost:${PORT}`);
  console.log(`Push endpoint: POST /push`);
  console.log(`Pull endpoint: POST /pull (manual) or POST /pull/start (streaming)`);
});