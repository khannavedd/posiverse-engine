const { onSaleCreateUpdateInStock } = require("../../controllers/Sales/onSaleCreateUpdateInStock");
const { onCustomerDueEvent } = require("../../controllers/Sales/customerDue");

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

router.post("/on-sale-create-update-instock", onSaleCreateUpdateInStock);

// Second, independent consumer on the SAME "sale-events" topic — needs
// its own Pub/Sub subscription pointed at this route (same topic, new
// subscription, same pattern Purchase's InStock/vendorDue pair already
// uses on "purchase-events"). Handles both a regular sale's DueAmount
// and a "Receive Payment" sale's TotalAmount (see
// controllers/Sales/customerDue.js) — both are just Sale rows on this
// same topic, told apart by TransactionType.Code.
//   gcloud pubsub subscriptions create customer-due-on-sale-events \
//     --topic=sale-events \
//     --push-endpoint="https://<this-service-url>/on-customer-due-event?token=<SALES_PUSH_SECRET>"
router.post("/on-customer-due-event", onCustomerDueEvent);

module.exports = router;
