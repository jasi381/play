const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

app.use(express.json());

// Load existing data or start fresh
let store = [];
if (fs.existsSync(DATA_FILE)) {
  store = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
}

const saveData = () => {
  fs.writeFileSync(DATA_FILE, JSON.stringify(store, null, 2));
};

// POST - store any data
app.post('/data', (req, res) => {
  const id = Date.now().toString();
  const entry = { id, data: req.body, createdAt: new Date().toISOString() };
  store.push(entry);
  saveData();
  res.status(201).json(entry);
});

// GET - get all data
app.get('/data', (req, res) => {
  res.json(store);
});

// GET - get specific entry by id
app.get('/data/:id', (req, res) => {
  const entry = store.find(e => e.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Not found' });
  res.json(entry);
});

// DELETE - remove entry by id
app.delete('/data/:id', (req, res) => {
  const index = store.findIndex(e => e.id === req.params.id);
  if (index === -1) return res.status(404).json({ error: 'Not found' });
  store.splice(index, 1);
  saveData();
  res.status(204).send();
});

app.listen(PORT, () => {
  console.log(`API running at http://localhost:${PORT}`);
});