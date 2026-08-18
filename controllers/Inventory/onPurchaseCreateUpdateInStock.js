const crypto = require("crypto");
const pool = require("../../db/postgres");

const CONSUMER_NAME = "instock-on-purchase-created";

// Pub/Sub push endpoint target — see routes/Inventory/inventory.js.
// Subscribed to the "purchase-events" topic, published by
// posiverseApi's Utils/publishEvent.js. One Pub/Sub message covers
// one whole Purchase (InventoryID = the Purchase's own ID) — this
// handler loops over every line item itself, the same way the
// reference posible-inventory-engine's
// onCreateInventoryUpdateInStockQty.js loops over inventory.LineItems,
// rather than expecting a separate message per item.
//
// A Pub/Sub push request body looks like:
//   { message: { data: "<base64 JSON>", messageId, publishTime, attributes }, subscription }
// The decoded JSON (message.data) is:
//   { eventType, beforeData, afterData, InventoryID }
// where InventoryID is the Purchase's own PurchaseID, afterData is
// { purchase: { PurchaseID, StoreID, TransactionTypeID, TransactionNo, TransactionDate },
//   items: [{ productId, qty, unitCost, mrp, retailPrice, ... }, ...] },
// and beforeData is the same shape for PurchaseUpdated (null for
// PurchaseCreated).
//
// Handles both PurchaseCreated and PurchaseUpdated by reducing them to
// the same operation: a per-product qty DELTA between beforeData.items
// (empty on create) and afterData.items, signed by the purchase's
// TransactionType — "Direction"/"UpdateStock" on TransactionType exist
// specifically for this (see migration 012's comment: "signs stock
// math later"), so this reads that row instead of assuming every event
// on this topic adds stock. A type with UpdateStock = false (e.g. a
// transaction that shouldn't touch inventory at all) is skipped
// entirely; Direction 'in' adds the delta, 'out' subtracts it, and
// 'neutral' has no stock effect.
//
// No variant/anchor special-casing needed — InStock is keyed on
// whatever ProductID a line item actually references, and
// Controllers/Purchase.js already requires a real productId per item
// (the app's ProductPickerModal only ever lets a purchase line resolve
// to an actual sellable row — a variant's own ProductID for a style
// that has variants, or the anchor's own ProductID for a simple
// product with none). So a variant's stock updates exactly like any
// other product's, through the same INSERT ... ON CONFLICT below.
//
// Idempotency is keyed on Pub/Sub's own message.messageId (the outer
// envelope field, NOT anything inside the decoded payload) rather
// than a custom eventId. One message now covers the whole purchase,
// so one dedupe insert guards the entire item loop — a redelivery
// hits the primary key before any item is touched, not partway
// through.
module.exports.onPurchaseCreateUpdateInStock = async (req, res) => {
  const messageId = req.body?.message?.messageId;
  if (!messageId) {
    console.error("onPurchaseCreated: request has no message.messageId — acking without processing", req.body);
    return res.status(200).send();
  }

  let event;
  try {
    const raw = req.body?.message?.data;
    if (!raw) {
      console.error(`onPurchaseCreated: message ${messageId} has no data — acking without processing`);
      return res.status(200).send();
    }
    event = JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
  } catch (error) {
    console.error(`onPurchaseCreated: couldn't decode/parse message ${messageId} — acking without processing`, error);
    return res.status(200).send();
  }

  if (event.eventType !== "PurchaseCreated" && event.eventType !== "PurchaseUpdated") {
    return res.status(200).send();
  }

  // pool.connect() itself can reject (e.g. Cloud SQL unreachable) —
  // that has to be inside this try too, or a connection failure is an
  // unhandled rejection that crashes the whole Cloud Run instance
  // instead of just failing this one request (Node exits on an
  // uncaught rejection nothing else catches) — every other in-flight
  // and future request on that instance dies with it until Cloud Run
  // restarts the container.
  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    // Idempotency guard — Pub/Sub is at-least-once delivery, so the
    // same message can legitimately arrive twice. Insert first, keyed
    // on (MessageID, Consumer); a duplicate delivery hits the primary
    // key and this catches the 23505 to ack without redoing any of
    // this purchase's InStock adjustments.
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

    const { purchase, items } = event.afterData || {};
    const beforeItems = event.beforeData?.items || [];
    if (!purchase?.StoreID || !purchase?.TransactionTypeID || !Array.isArray(items)) {
      console.error(`onPurchaseCreated: message ${messageId} has no usable purchase/items to apply — acking anyway`, event);
      await client.query("COMMIT");
      return res.status(200).send();
    }

    // TransactionTypeID never changes between a Purchase's create and
    // any later edit (see Controllers/Purchase.js's updatePurchase
    // comment), so one lookup covers both before- and after-items.
    const txnTypeResult = await client.query(
      `SELECT "Direction", "UpdateStock" FROM "TransactionType" WHERE "TransactionTypeID" = $1`,
      [purchase.TransactionTypeID]
    );
    const txnType = txnTypeResult.rows[0];

    if (!txnType || !txnType.UpdateStock) {
      console.log(
        `onPurchaseCreated: message ${messageId}'s TransactionType (${purchase.TransactionTypeID}) doesn't update stock — skipping InStock`
      );
      await client.query("COMMIT");
      return res.status(200).send();
    }

    if (txnType.Direction !== "in" && txnType.Direction !== "out") {
      // 'neutral' (or anything unrecognized) — this TransactionType is
      // configured to leave stock alone even though UpdateStock is
      // true (e.g. a Stock Adjustment/Transfer that nets to zero here
      // and is handled by its own logic elsewhere, once that exists).
      await client.query("COMMIT");
      return res.status(200).send();
    }

    // Per-product qty delta between what this purchase used to say and
    // what it says now. On create, beforeItems is [] so every item's
    // "before" qty is 0 and the delta is just its full qty — same
    // result the old always-add version produced, just derived
    // generally instead of as a special case.
    const qtyByProduct = new Map();
    for (const item of beforeItems) {
      if (!item?.productId) continue;
      const entry = qtyByProduct.get(item.productId) || { before: 0, after: 0 };
      entry.before += Number(item.qty) || 0;
      qtyByProduct.set(item.productId, entry);
    }
    for (const item of items) {
      if (!item?.productId) continue;
      const entry = qtyByProduct.get(item.productId) || { before: 0, after: 0 };
      entry.after += Number(item.qty) || 0;
      qtyByProduct.set(item.productId, entry);
    }

    for (const [productId, { before, after }] of qtyByProduct) {
      const delta = after - before;
      if (delta === 0) continue; // untouched by this create/edit — nothing to apply

      const signedDelta = txnType.Direction === "out" ? -delta : delta;

      await client.query(
        `INSERT INTO "InStock"
          ("InStockID", "StoreID", "ProductID", "OpeningQty", "InStockQty",
           "LastTransactionTypeID", "LastTransactionNo", "LastTransactionDate", "LastTransactionQty",
           "Action", "ActionOn", "CreatedAt", "UpdatedAt")
         VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $4, 'NEW', now(), now(), now())
         ON CONFLICT ("StoreID", "ProductID") DO UPDATE SET
           "InStockQty" = "InStock"."InStockQty" + EXCLUDED."InStockQty",
           "LastTransactionTypeID" = EXCLUDED."LastTransactionTypeID",
           "LastTransactionNo" = EXCLUDED."LastTransactionNo",
           "LastTransactionDate" = EXCLUDED."LastTransactionDate",
           "LastTransactionQty" = EXCLUDED."LastTransactionQty",
           "Action" = 'EDIT',
           "ActionOn" = now(),
           "UpdatedAt" = now()`,
        [
          crypto.randomUUID(),
          purchase.StoreID,
          productId,
          signedDelta,
          purchase.TransactionTypeID,
          purchase.TransactionNo,
          purchase.TransactionDate,
        ]
      );
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
        console.error(`onPurchaseCreated: rollback failed for message ${messageId}`, rollbackError);
      }
    }
    console.error(`onPurchaseCreated: failed processing message ${messageId}`, error);
    // A real 500 here — unlike the acked-anyway cases above — tells
    // Pub/Sub to retry this message later instead of dropping it,
    // since this is a genuine failure to apply the stock change, not
    // a malformed/duplicate/irrelevant message.
    return res.status(500).json({ success: false, message: "Error applying purchase to InStock" });
  } finally {
    if (client) client.release();
  }
};
