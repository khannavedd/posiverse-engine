const express = require("express");
const bodyParser = require("body-parser");
require("dotenv").config();

const InventoryRoutes = require("./routes/Inventory/inventory");

const app = express();
const PORT = process.env.PORT || 8081;

app.use(bodyParser.json({ limit: "10mb" }));

// Mounted at the root, not "/internal/inventory" — this is its own
// service with its own URL, unlike when this handler briefly lived
// inside posiverseApi alongside unrelated routes. Cloud Run's URL
// itself is the "internal" boundary; the push-secret query param on
// the Pub/Sub subscription is what actually guards it.
app.use("/", InventoryRoutes);

app.get("/health", (req, res) => res.status(200).send("ok"));

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send("Something broke!");
});

app.listen(PORT, () => {
  console.log(`posiverse-engine listening on port ${PORT}`);
});
