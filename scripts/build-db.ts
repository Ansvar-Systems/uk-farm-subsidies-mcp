import { createDatabase } from '../src/db.js';
import { mkdirSync } from 'fs';

mkdirSync('data', { recursive: true });
const db = createDatabase('data/database.db');
console.log('Database schema created at data/database.db');
db.close();
