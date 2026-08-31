const pool = require("../../db/postgres");

// Accepts BOTH the new InventoryCreated/InventoryUpdated spelling and
// the old PurchaseCreated/PurchaseUpdated one (DEC-026). Being tolerant
// here is what lets the two services deploy in any order: a message
// published by the pre-rename API can still be in flight when this
// deploys, and dropping it would silently lose a stock movement.
// The old spelling can be removed once no pre-rename API is running.
function isInventoryWrite(eventType) {
  return eventType === "InventoryCreated" || eventType === "InventoryUpdated" ||
         eventType === "PurchaseCreated" || eventType === "PurchaseUpdated";
}

// Same reason: the payload key moved from `purchase` to `inventory`.
function documentFrom(data) {
  return data?.inventory ?? data?.purchase ?? null;
}

const CONSUMER_NAME = "vendor-due-on-purchase-write";

// Pub/Sub push endpoint target — see routes/Inventory/vendorDue.js.
// Subscribed to the same "purchase-events" topic as inventory.js, but
// a separate consumer for a separate concern (Vendor.DueAmount, not
// InStock) — same split the reference posible-inventory-engine uses
// (onCreateInventoryUpdateInStockQty.js vs.
// onWriteInventoryUpdateVendorOutStandingAmount.js are two different
// files reacting to the same event).
//
// Unlike the InStock consumer — which only reacts to PurchaseCreated
// today, because reconciling stock quantities on an edit needs a real
// per-item delta — this one handles PurchaseCreated AND
// PurchaseUpdated from day one: applying a due-amount delta is the
// same operation either way, just with a zero "before" on create.
//
// Message shape: { eventType, beforeData, afterData, InventoryID }.
// afterData.purchase (and beforeData.purchase, when present) include
// VendorID and DueAmount alongside the fields the InStock consumer
// uses — see posiverseApi's Utils/publishEvent.js.
module.exports.onPurchaseWriteUpdateVendorDue = async (req, res) => {
  const messageId = req.body?.message?.messageId;
  if (!messageId) {
    console.error("onPurchaseWriteUpdateVendorDue: request has no message.messageId — acking without processing", req.body);
    return res.status(200).send();
  }

  let event;
  try {
    const raw = req.body?.message?.data;
    if (!raw) {
      console.error(`onPurchaseWriteUpdateVendorDue: message ${messageId} has no data — acking without processing`);
      return res.status(200).send();
    }
    event = JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
  } catch (error) {
    console.error(`onPurchaseWriteUpdateVendorDue: couldn't decode/parse message ${messageId} — acking without processing`, error);
    return res.status(200).send();
  }

  if (!isInventoryWrite(event.eventType)) {
    return res.status(200).send();
  }

  // pool.connect() itself can reject (e.g. Cloud SQL unreachable) —
  // that has to be inside this try too. It used to sit above it, so a
  // connection failure was an unhandled rejection that crashed the
  // whole Cloud Run instance instead of just failing this one request
  // (Node exits on an uncaught rejection in an async handler nothing
  // else catches) — every other in-flight and future request on that
  // instance died with it until Cloud Run restarted the container.
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    // Idempotency guard, same pattern as inventory.js — one dedupe
    // insert per message, keyed on Pub/Sub's own messageId.
    try {
      await client.query(
        `INSERT INTO "ProcessedOutboxEvent" ("MessageID", "Consumer") VALUES ($1, $2)`,
        [messageId, CONSUMER_NAME]
      );
    } catch (dupeError) {
      if (dupeError.code === "23505") {
        await client.query("ROLLBACK");
        return res.status(200).send();
      }
      throw dupeError;
    }

    const after = documentFrom(event.afterData);
    if (!after?.VendorID) {
      console.error(`onPurchaseWriteUpdateVendorDue: message ${messageId} has no usable purchase to apply — acking anyway`, event);
      await client.query("COMMIT");
      return res.status(200).send();
    }

    if (event.eventType === "PurchaseCreated") {
      const newDue = Number(after.DueAmount) || 0;
      if (newDue > 0) {
        await client.query(
          `UPDATE "Vendor" SET "DueAmount" = "DueAmount" + $1, "UpdatedAt" = now() WHERE "VendorID" = $2`,
          [newDue, after.VendorID]
        );
      }
    } else {
      // PurchaseUpdated — undo what the OLD version of this purchase
      // contributed, then apply what the NEW version contributes, to
      // whichever vendor owns it now (which may differ from before).
      // Ported straight from what used to be inline in
      // Controllers/Inventory.js's updateInventory.
      const before = documentFrom(event.beforeData);
      const oldDue = Number(before?.DueAmount) || 0;
      const newDue = Number(after.DueAmount) || 0;
      const oldVendorId = before?.VendorID;

      if (oldVendorId && oldVendorId === after.VendorID) {
        const delta = newDue - oldDue;
        if (delta !== 0) {
          await client.query(
            `UPDATE "Vendor" SET "DueAmount" = "DueAmount" + $1, "UpdatedAt" = now() WHERE "VendorID" = $2`,
            [delta, after.VendorID]
          );
        }
      } else {
        if (oldVendorId && oldDue !== 0) {
          await client.query(
            `UPDATE "Vendor" SET "DueAmount" = "DueAmount" - $1, "UpdatedAt" = now() WHERE "VendorID" = $2`,
            [oldDue, oldVendorId]
          );
        }
        if (newDue !== 0) {
          await client.query(
            `UPDATE "Vendor" SET "DueAmount" = "DueAmount" + $1, "UpdatedAt" = now() WHERE "VendorID" = $2`,
            [newDue, after.VendorID]
          );
        }
      }
    }

    await client.query("COMMIT");
    return res.status(200).send();
  } catch (error) {
    // client may never have been assigned if pool.connect() itself is
    // what threw — nothing to roll back or release in that case.
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error(`onPurchaseWriteUpdateVendorDue: rollback failed for message ${messageId}`, rollbackError);
      }
    }
    console.error(`onPurchaseWriteUpdateVendorDue: failed processing message ${messageId}`, error);
    // Real 500 — tells Pub/Sub to retry, since this is a genuine
    // failure to apply the due-amount change, not a malformed/
    // duplicate/irrelevant message.
    return res.status(500).json({ success: false, message: "Error updating vendor due amount" });
  } finally {
    if (client) client.release();
  }
};
