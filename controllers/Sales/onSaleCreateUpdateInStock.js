const crypto = require("crypto");
const pool = require("../../db/postgres");

const CONSUMER_NAME = "instock-on-sale-created";

// Pub/Sub push endpoint target — see routes/Sales/sales.js. Subscribed
// to its OWN "sale-events" topic (separate from Purchase's
// "purchase-events" topic and this consumer's Purchase counterpart,
// controllers/Inventory/onInventoryCreateUpdateInStock.js) — explicit
// decision to keep Sale's and Purchase's InStock pipelines fully
// independent rather than branching one consumer on eventType.
//
// Message shape (posiverseApi's Utils/publishEvent.js's
// publishSaleEvent): { eventType, beforeData, afterData, InventoryID }
// where InventoryID is the Sale's own SaleID, afterData is
// { sale: { SaleID, StoreID, CustomerID, TransactionTypeID, InvoiceNumber, SaleDate },
//   items: [{ productId, qty, unitPrice, ... }, ...] }, and beforeData
// is the same shape for a future SaleUpdated (null for SaleCreated —
// there's no sale-edit flow yet, so only SaleCreated is published
// today, but this handler is written the same before/after-delta way
// as the Purchase consumer so SaleUpdated can be added later without
// reworking this logic).
//
// Same TransactionType-aware signing as the Purchase consumer: SALE is
// seeded with Direction 'out' (see Controllers/Registration.js), so a
// sale's qty subtracts from InStock instead of adding.
module.exports.onSaleCreateUpdateInStock = async (req, res) => {
  const messageId = req.body?.message?.messageId;
  if (!messageId) {
    console.error("onSaleCreated: request has no message.messageId — acking without processing", req.body);
    return res.status(200).send();
  }

  let event;
  try {
    const raw = req.body?.message?.data;
    if (!raw) {
      console.error(`onSaleCreated: message ${messageId} has no data — acking without processing`);
      return res.status(200).send();
    }
    event = JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
  } catch (error) {
    console.error(`onSaleCreated: couldn't decode/parse message ${messageId} — acking without processing`, error);
    return res.status(200).send();
  }

  if (event.eventType !== "SaleCreated" && event.eventType !== "SaleUpdated") {
    return res.status(200).send();
  }

  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    // Idempotency guard — same pattern as the Purchase consumer, keyed
    // on Pub/Sub's own messageId + this consumer's own name so a
    // redelivery on this topic can never collide with the Purchase
    // consumer's dedupe rows on its topic.
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

    const { sale, items } = event.afterData || {};
    const beforeItems = event.beforeData?.items || [];
    if (!sale?.StoreID || !sale?.TransactionTypeID || !Array.isArray(items)) {
      console.error(`onSaleCreated: message ${messageId} has no usable sale/items to apply — acking anyway`, event);
      await client.query("COMMIT");
      return res.status(200).send();
    }

    const txnTypeResult = await client.query(
      `SELECT "Direction", "UpdateStock" FROM "TransactionType" WHERE "TransactionTypeID" = $1`,
      [sale.TransactionTypeID]
    );
    const txnType = txnTypeResult.rows[0];

    if (!txnType || !txnType.UpdateStock) {
      console.log(
        `onSaleCreated: message ${messageId}'s TransactionType (${sale.TransactionTypeID}) doesn't update stock — skipping InStock`
      );
      await client.query("COMMIT");
      return res.status(200).send();
    }

    if (txnType.Direction !== "in" && txnType.Direction !== "out") {
      await client.query("COMMIT");
      return res.status(200).send();
    }

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
      if (delta === 0) continue;

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
          sale.StoreID,
          productId,
          signedDelta,
          sale.TransactionTypeID,
          sale.InvoiceNumber,
          sale.SaleDate,
        ]
      );
    }

    await client.query("COMMIT");
    return res.status(200).send();
  } catch (error) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error(`onSaleCreated: rollback failed for message ${messageId}`, rollbackError);
      }
    }
    console.error(`onSaleCreated: failed processing message ${messageId}`, error);
    return res.status(500).json({ success: false, message: "Error applying sale to InStock" });
  } finally {
    if (client) client.release();
  }
};
