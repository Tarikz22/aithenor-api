const express = require('express');
const analyzeHandler = require('./api/analyze').default;

const app = express();

app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: '10mb' }));

app.post('/api/analyze', (req, res) => analyzeHandler(req, res));

app.get('/', (req, res) => {
  res.send('Aithenor API is running');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
