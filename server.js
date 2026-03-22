const express = require('express');
const cors = require('cors');
const fileUpload = require('express-fileupload');
const analyzeModule = require('./api/analyze');
const analyzeHandler = analyzeModule.default || analyzeModule;

const app = express();

app.use(cors({
  origin: [
    'https://app.aithenor.com',
    'https://www.app.aithenor.com',
    'http://localhost:3000'
  ],
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: '10mb' }));

app.use(fileUpload({
  useTempFiles: false,
  limits: { fileSize: 10 * 1024 * 1024 }
}));

app.post('/api/analyze', (req, res) => analyzeHandler(req, res));

app.get('/', (req, res) => {
  res.send('Aithenor API is running');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
