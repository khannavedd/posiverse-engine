const pool = require("../../db/postgres");

const CONSUMER_NAME = "customer-due-on-sale-events";

// Pub/Sub push endpoint target — see routes/Sales/sales.js. Subscribed
// to the SAME "sale-events" topic onSaleCreateUpdateInStock.js already
// listens on (a second, independent consumer on one topic — same split
// Purchase's InStock/vendorDue pair already uses on "purchase-events").
//
// Customer.OutstandingBalance is NOT allocated per-Sale — same "one
// running balance" shape Vendor.DueAmount already has. Two different
// KINDS of Sale affect it, told apart by TransactionType.Code (same
// pattern onSaleCreateUpdateInStock.js already uses to check
// Direction/UpdateStock):
//
//   Code = 'SALE' (a regular goods sale) — its DueAmount ADDS to what's
//   owed, same as before.
//
//   Code = 'RECEIVE_PAYMENT' (see Controllers/Sale.js's
//   recordCustomerPayment — a payment collection IS a Sale row, not a
//   separate ledger) — its TotalAmount SUBTRACTS from what's owed and
//   adds to OutstandingAmountReceived. A cancelled payment (Status =
//   'cancelled') is treated as contributing 0 rather than relying on
//   DueAmount being zeroed the way cancelSale does for a regular sale
//   — a payment record's own DueAmount is always 0 to begin with, so
//   that trick doesn't apply here; Status is checked directly instead
//   (see saleSnapshot() in posiverseApi's Utils/publishEvent.js, which
//   now includes both TotalAmount and Status for exactly this).
module.exports.onCustomerDueEvent = async (req, res) => {
  const messageId = req.body?.message?.messageId;
  if (!messageId) {
    console.error("onCustomerDueEvent: request has no message.messageId — acking without processing", req.body);
    return res.status(200).send();
  }

  let event;
  try {
    const raw = req.body?.message?.data;
    if (!raw) {
      console.error(`onCustomerDueEvent: message ${messageId} has no data — acking without processing`);
      return res.status(200).send();
    }
    event = JSON.parse(Buffer.from(raw, "base64").toString("utf8"));
  } catch (error) {
    console.error(`onCustomerDueEvent: couldn't decode/parse message ${messageId} — acking without processing`, error);
    return res.status(200).send();
  }

  if (event.eventType !== "SaleCreated" && event.eventType !== "SaleUpdated") {
    return res.status(200).send();
  }

  let client;
  try {
    client = await pool.connect();
    await client.query("BEGIN");

    // Idempotency guard, same pattern/table as vendorDue.js and
    // onSaleCreateUpdateInStock.js — its own Consumer name, so a
    // redelivery here can never collide with either of those two
    // consumers' own dedupe rows on this same topic.
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

    const after = event.afterData?.sale;
    if (!after) {
      console.error(`onCustomerDueEvent: message ${messageId} has no usable sale to apply — acking anyway`, event);
      await client.query("COMMIT");
      return res.status(200).send();
    }

    const txnTypeResult = await client.query(
      `SELECT "Code" FROM "TransactionType" WHERE "TransactionTypeID" = $1`,
      [after.TransactionTypeID]
    );
    const code = txnTypeResult.rows[0]?.Code;
    const isPaymentReceipt = code === "RECEIVE_PAYMENT";

    // A SALE_RETURN settled as "credit to account" carries no payment
    // rows — the money moves on the customer's balance instead, which
    // is this consumer's job. A return settled as a cash refund already
    // handed the money back at the till and must NOT also reduce the
    // balance, or the customer is credited twice.
    //
    // Told apart by PaymentMethod, which createSaleReturn sets to
    // "Credit to account" for exactly this purpose. Reading the marker
    // the writer set beats re-deriving it from the absence of payment
    // rows, which would silently misclassify a refund whose payment
    // insert failed.
    const isCreditReturn = code === "SALE_RETURN" && after.PaymentMethod === "Credit to account";

    if (isCreditReturn) {
      const before = event.beforeData?.sale;
      const oldEffective = before && before.Status !== "cancelled" ? Number(before.TotalAmount) || 0 : 0;
      const newEffective = after.Status !== "cancelled" ? Number(after.TotalAmount) || 0 : 0;
      const delta = newEffective - oldEffective;

      // Reduces what's owed, exactly like a payment — returning goods
      // you haven't paid for yet settles part of the debt. A customer
      // who owed nothing ends up with a negative balance, which the app
      // already renders as "In credit".
      if (delta !== 0 && after.CustomerID) {
        await client.query(
          `UPDATE "Customer"
           SET "OutstandingBalance" = "OutstandingBalance" - $1, "UpdatedAt" = now()
           WHERE "CustomerID" = $2`,
          [delta, after.CustomerID]
        );
      }
      await client.query("COMMIT");
      return res.status(200).send();
    }

    if (isPaymentReceipt) {
      const before = event.beforeData?.sale;
      // Cancelled contributes 0, regardless of TotalAmount — a payment
      // record's own DueAmount is always 0, so there's nothing else to
      // zero out on cancel the way a regular sale's DueAmount is.
      const oldEffective = before && before.Status !== "cancelled" ? Number(before.TotalAmount) || 0 : 0;
      const newEffective = after.Status !== "cancelled" ? Number(after.TotalAmount) || 0 : 0;
      const oldCustomerId = before?.CustomerID;
      const newCustomerId = after.CustomerID;

      if (!before || oldCustomerId === newCustomerId) {
        // SaleCreated (no before), or SaleUpdated with the customer
        // unchanged — one delta covers both: delta = newEffective -
        // oldEffective (0 when there's no before). Positive delta =
        // more received: balance goes down, lifetime-received goes up
        // by the same amount; negative delta (payment reduced or
        // cancelled) unwinds both the same way.
        const delta = newEffective - oldEffective;
        if (delta !== 0 && newCustomerId) {
          await client.query(
            `UPDATE "Customer"
             SET "OutstandingBalance" = "OutstandingBalance" - $1,
                 "OutstandingAmountReceived" = "OutstandingAmountReceived" + $1,
                 "UpdatedAt" = now()
             WHERE "CustomerID" = $2`,
            [delta, newCustomerId]
          );
        }
      } else {
        // Customer reassigned on edit — reverse what the old customer
        // was credited, apply fresh to the new one.
        if (oldCustomerId && oldEffective !== 0) {
          await client.query(
            `UPDATE "Customer"
             SET "OutstandingBalance" = "OutstandingBalance" + $1,
                 "OutstandingAmountReceived" = "OutstandingAmountReceived" - $1,
                 "UpdatedAt" = now()
             WHERE "CustomerID" = $2`,
            [oldEffective, oldCustomerId]
          );
        }
        if (newCustomerId && newEffective !== 0) {
          await client.query(
            `UPDATE "Customer"
             SET "OutstandingBalance" = "OutstandingBalance" - $1,
                 "OutstandingAmountReceived" = "OutstandingAmountReceived" + $1,
                 "UpdatedAt" = now()
             WHERE "CustomerID" = $2`,
            [newEffective, newCustomerId]
          );
        }
      }
    } else if (event.eventType === "SaleCreated") {
      const newDue = Number(after.DueAmount) || 0;
      if (after.CustomerID && newDue > 0) {
        await client.query(
          `UPDATE "Customer" SET "OutstandingBalance" = "OutstandingBalance" + $1, "UpdatedAt" = now() WHERE "CustomerID" = $2`,
          [newDue, after.CustomerID]
        );
      }
    } else {
      // SaleUpdated on a regular sale — same before/after-delta
      // approach as vendorDue.js, including the customer-changed-on-
      // edit branch. Cancellation is handled upstream: cancelSale
      // zeroes DueAmount itself (see Controllers/Sale.js), so it falls
      // out of this same delta math with no extra check needed here.
      const before = event.beforeData?.sale;
      const oldDue = Number(before?.DueAmount) || 0;
      const newDue = Number(after.DueAmount) || 0;
      const oldCustomerId = before?.CustomerID;
      const newCustomerId = after.CustomerID;

      if (oldCustomerId && oldCustomerId === newCustomerId) {
        const delta = newDue - oldDue;
        if (delta !== 0) {
          await client.query(
            `UPDATE "Customer" SET "OutstandingBalance" = "OutstandingBalance" + $1, "UpdatedAt" = now() WHERE "CustomerID" = $2`,
            [delta, newCustomerId]
          );
        }
      } else {
        if (oldCustomerId && oldDue !== 0) {
          await client.query(
            `UPDATE "Customer" SET "OutstandingBalance" = "OutstandingBalance" - $1, "UpdatedAt" = now() WHERE "CustomerID" = $2`,
            [oldDue, oldCustomerId]
          );
        }
        if (newCustomerId && newDue !== 0) {
          await client.query(
            `UPDATE "Customer" SET "OutstandingBalance" = "OutstandingBalance" + $1, "UpdatedAt" = now() WHERE "CustomerID" = $2`,
            [newDue, newCustomerId]
          );
        }
      }
    }

    await client.query("COMMIT");
    return res.status(200).send();
  } catch (error) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error(`onCustomerDueEvent: rollback failed for message ${messageId}`, rollbackError);
      }
    }
    console.error(`onCustomerDueEvent: failed processing message ${messageId}`, error);
    return res.status(500).json({ success: false, message: "Error updating customer due amount" });
  } finally {
    if (client) client.release();
  }
};
