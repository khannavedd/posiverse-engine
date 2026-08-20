const { onSaleCreateUpdateInStock } = require("../../controllers/Sales/onSaleCreateUpdateInStock");

const router = require("express").Router();

// Called by a Pub/Sub push subscription on Sale's own "sale-events"
// topic — separate from Purchase's "purchase-events" topic/route, by
// explicit decision. Same shared-secret pattern as
// routes/Inventory/inventory.js, but its own dedicated secret
// (SALES_PUSH_SECRET) so the two push endpoints can be rotated/revoked
// independently:
//   gcloud pubsub topics create sale-events
//   gcloud pubsub subscriptions create instock-on-sale-created \
//     --topic=sale-events \
//     --push-endpoint="https://<this-service-url>/on-sale-create-update-instock?token=<SALES_PUSH_SECRET>"
// Skips the check if SALES_PUSH_SECRET isn't set (local dev).
function requireSalesPushSecret(req, res, next) {
  const expected = process.env.SALES_PUSH_SECRET;
  if (expected && req.query.token !== expected) {
    return res.status(403).json({ success: false, message: "Forbidden" });
  }
  next();
}

router.post("/on-sale-create-update-instock", requireSalesPushSecret, onSaleCreateUpdateInStock);

module.exports = router;
