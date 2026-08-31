const { onInventoryCreateUpdateInStock } = require("../../controllers/Inventory/onInventoryCreateUpdateInStock");
const { onPurchaseWriteUpdateVendorDue } = require("../../controllers/Inventory/vendorDue");

const router = require("express").Router();

// Called by a Pub/Sub push subscription, not a logged-in user. A
// shared secret carried as a query param (that's how you configure a
// static token on a Pub/Sub push endpoint URL) keeps random requests
// from triggering an InStock adjustment:
//   gcloud pubsub subscriptions create instock-on-purchase-created \
//     --topic=purchase-events \
//     --push-endpoint="https://<this-service-url>/on-purchase-created?token=<INVENTORY_PUSH_SECRET>"
// Skips the check if INVENTORY_PUSH_SECRET isn't set (local dev).
function requireInventoryPushSecret(req, res, next) {
  const expected = process.env.INVENTORY_PUSH_SECRET;
  if (expected && req.query.token !== expected) {
    return res.status(403).json({ success: false, message: "Forbidden" });
  }
  next();
}

router.post("/on-inventory-create-update-instock" , onInventoryCreateUpdateInStock);
router.post("/on-inventory-write-update-vendor-due", onPurchaseWriteUpdateVendorDue);

module.exports = router;
