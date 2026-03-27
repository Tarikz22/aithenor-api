const express = require('express');
const cors = require('cors');
const getEngineOutputHandler = require('./api/engine-output');
const analyzeHandler = require('./api/analyze');

const app = express();

app.get('/api/engine-output', (req, res) => getEngineOutputHandler(req, res));
app.use(cors({
  origin: [
    'https://app.aithenor.com',
    'https://www.app.aithenor.com',
    'http://localhost:3000'
  ],
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.urlencoded({ extended: true, limit: '1mb' }));
app.use(express.json({ limit: '1mb' }));

app.post('/api/analyze', (req, res) => analyzeHandler(req, res));

app.get('/', (req, res) => {
  res.send('Aithenor API is running');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
