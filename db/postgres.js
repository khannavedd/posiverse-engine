require("dotenv").config();
const { Pool } = require("pg");

// This service reads/writes the same Cloud SQL database as posiverseApi
// (InStock, OutboxEvent, ProcessedOutboxEvent live there — there's no
// separate "inventory" database). Same dual-mode connection pattern as
// posiverseApi/DB/postgres.js: on Cloud Run, INSTANCE_CONNECTION_NAME
// is set and we connect over the mounted Unix socket; locally we fall
// back to DB_HOST (public IP + SSL, or 127.0.0.1 via the Cloud SQL Auth
// Proxy with no SSL since the proxy already encrypts the tunnel).
const isLocalHost = ["localhost", "127.0.0.1"].includes(process.env.DB_HOST);

const pool = process.env.INSTANCE_CONNECTION_NAME
  ? new Pool({
      host: `/cloudsql/${process.env.INSTANCE_CONNECTION_NAME}`,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
    })
  : new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: isLocalHost ? false : { rejectUnauthorized: false },
    });

module.exports = pool;
