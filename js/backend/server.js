require("dotenv").config();

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const path = require("path");

const app = express();
const PORT = Number(process.env.PORT || 8080);
const FRONTEND_ROOT = path.resolve(__dirname, "..", "..");
const FRONTEND_INDEX_FILE = path.join(FRONTEND_ROOT, "index.html");
const FRONTEND_CSS_DIR = path.join(FRONTEND_ROOT, "css");
const FRONTEND_JS_DIR = path.join(FRONTEND_ROOT, "js");

app.use(cors());
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

const pool = mysql.createPool({
  host: process.env.MYSQLHOST || "localhost",
  user: process.env.MYSQLUSER || "root",
  password: process.env.MYSQLPASSWORD || "",
  database: process.env.MYSQLDATABASE || "erp_sankha",
  port: Number(process.env.MYSQLPORT || 3306),
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

console.log("DB ENV CHECK:", {
  host: process.env.MYSQLHOST || "localhost",
  user: process.env.MYSQLUSER || "root",
  database: process.env.MYSQLDATABASE || "erp_sankha",
  port: process.env.MYSQLPORT || 3306
});

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:", reason);
});

function format3(value) {
  const n = Number(value || 0);
  return Number.isNaN(n) ? "0.000" : n.toFixed(3);
}

function normalizeReturnType(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "RETURN_TO_STOCK") return "RETURN_TO_STOCK";
  if (clean === "DAMAGED_RETURN") return "DAMAGED_RETURN";
  return "";
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

const PARTY_TYPES = [
  "CUSTOMER",
  "SUPPLIER",
  "KARIGAR",
  "CUSTOMER_SUPPLIER",
  "BULLION_PARTY",
  "INTERNAL"
];

const TRANSACTION_TYPES = [
  "OPENING_BALANCE",
  "SALE_INVOICE",
  "SALE_RETURN",
  "PURCHASE_INVOICE",
  "PURCHASE_RETURN",
  "PAYMENT_RECEIVED",
  "PAYMENT_GIVEN",
  "ADVANCE_RECEIVED",
  "ADVANCE_GIVEN",
  "CASH_ADJUSTMENT",
  "METAL_RECEIVED",
  "METAL_GIVEN",
  "METAL_ADJUSTMENT",
  "METAL_SETTLEMENT_RECEIVED",
  "METAL_SETTLEMENT_GIVEN",
  "KARIGAR_ISSUE",
  "KARIGAR_RECEIVE",
  "KARIGAR_LABOUR",
  "KARIGAR_LOSS_ADJUSTMENT",
  "RATE_DIFF_ADJUSTMENT",
  "INTERNAL_TRANSFER"
];

function normalizePartyType(value) {
  const clean = String(value || "").trim().toUpperCase();
  return PARTY_TYPES.includes(clean) ? clean : "";
}

function normalizeTransactionType(value) {
  const clean = String(value || "").trim().toUpperCase();
  return TRANSACTION_TYPES.includes(clean) ? clean : "";
}

function normalizeMetalType(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "GOLD") return "GOLD";
  if (clean === "SILVER") return "SILVER";
  return "";
}

function normalizeCashEntryType(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "DEBIT") return "DEBIT";
  if (clean === "CREDIT") return "CREDIT";
  return "";
}

function normalizeMetalEntryType(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "IN") return "IN";
  if (clean === "OUT") return "OUT";
  return "";
}

function normalizeTransactionStatus(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "DRAFT") return "DRAFT";
  if (clean === "CANCELLED") return "CANCELLED";
  return "POSTED";
}

function normalizeSettlementType(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "CASH") return "CASH";
  if (clean === "METAL") return "METAL";
  if (clean === "ADJUSTMENT") return "ADJUSTMENT";
  if (clean === "MIXED") return "MIXED";
  return "";
}

function getDefaultCashEntryType(transactionType) {
  switch (transactionType) {
    case "SALE_INVOICE":
    case "PURCHASE_RETURN":
    case "PAYMENT_GIVEN":
    case "ADVANCE_GIVEN":
      return "DEBIT";
    case "PURCHASE_INVOICE":
    case "SALE_RETURN":
    case "PAYMENT_RECEIVED":
    case "ADVANCE_RECEIVED":
    case "KARIGAR_LABOUR":
      return "CREDIT";
    default:
      return "";
  }
}

function getDefaultMetalEntryType(transactionType) {
  switch (transactionType) {
    case "METAL_RECEIVED":
    case "METAL_SETTLEMENT_RECEIVED":
    case "KARIGAR_ISSUE":
      return "IN";
    case "METAL_GIVEN":
    case "METAL_SETTLEMENT_GIVEN":
    case "KARIGAR_RECEIVE":
      return "OUT";
    default:
      return "";
  }
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function normalizeProcessLotNo(value) {
  return String(value || "").trim();
}

function normalizeKarigarName(value) {
  return String(value || "").trim();
}

function normalizeProcessLotRow(row) {
  return {
    ...row,
    raw_weight: toNumber(row.raw_weight),
    loss_weight: toNumber(row.loss_weight),
    final_weight: toNumber(row.final_weight)
  };
}

function normalizeKarigarWorkRow(row) {
  return {
    ...row,
    issue_weight: toNumber(row.issue_weight),
    receive_weight: toNumber(row.receive_weight),
    loss_weight: toNumber(row.loss_weight),
    labour_amount: toNumber(row.labour_amount)
  };
}

function normalizeExpenseRow(row) {
  return {
    ...row,
    amount: toNumber(row.amount)
  };
}

function normalizeCompanySettingsRow(row) {
  const base = {
    ownerEmail: "",
    top_title: "",
    company_name: "",
    gstin: "",
    account_no: "",
    ifsc: "",
    address: "",
    declaration: "",
    upi_id: "",
    upi_name: "",
    business_state: "Odisha",
    default_bill_type: "GST",
    default_tax_type: "CGST_SGST",
    default_rate_per_gram: 0,
    default_mc_rate: 0,
    subscription_plan: "basic",
    subscription_status: "active",
    subscription_start_date: "",
    subscription_end_date: ""
  };

  if (!row) return base;

  return {
    ownerEmail: String(row.owner_email || "").trim(),
    top_title: String(row.top_title || "").trim(),
    company_name: String(row.company_name || "").trim(),
    gstin: String(row.gstin || "").trim(),
    account_no: String(row.account_no || "").trim(),
    ifsc: String(row.ifsc || "").trim(),
    address: String(row.address || "").trim(),
    declaration: String(row.declaration || "").trim(),
    upi_id: String(row.upi_id || "").trim(),
    upi_name: String(row.upi_name || "").trim(),
    business_state: String(row.business_state || "Odisha").trim() || "Odisha",
    default_bill_type: String(row.default_bill_type || "GST").trim() || "GST",
    default_tax_type: String(row.default_tax_type || "CGST_SGST").trim() || "CGST_SGST",
    default_rate_per_gram: toNumber(row.default_rate_per_gram),
    default_mc_rate: toNumber(row.default_mc_rate),
    subscription_plan: String(row.subscription_plan || "basic").trim() || "basic",
    subscription_status: String(row.subscription_status || "active").trim() || "active",
    subscription_start_date: row.subscription_start_date || "",
    subscription_end_date: row.subscription_end_date || ""
  };
}

function mapInvoiceDraftPayload(draftRow, itemRows) {
  const safeDraft = draftRow || null;
  const items = Array.isArray(itemRows) ? itemRows : [];
  const pendingItems = items
    .filter((item) => String(item.item_stage || "").trim().toUpperCase() === "PENDING")
    .map((item) => ({
      id: item.id,
      barcode: item.barcode || "",
      productName: item.product_name || "",
      sku: item.sku || "",
      weight: toNumber(item.weight),
      purity: item.purity || "",
      size: item.size || "",
      lot: item.lot_number || "",
      company_id: item.company_id
    }));
  const processedItems = items
    .filter((item) => String(item.item_stage || "").trim().toUpperCase() === "READY")
    .map((item) => ({
      id: item.id,
      barcode: item.barcode || "",
      productName: item.product_name || "",
      sku: item.sku || "",
      weight: toNumber(item.weight),
      purity: item.purity || "",
      size: item.size || "",
      lot: item.lot_number || "",
      customerName: safeDraft?.customer_name || "",
      mobile: safeDraft?.mobile || "",
      invoiceDate: safeDraft?.invoice_date || "",
      invoiceNumber: safeDraft?.invoice_number || "",
      company_id: item.company_id
    }));

  return {
    draft: safeDraft
      ? {
          id: safeDraft.id,
          customerName: safeDraft.customer_name || "",
          mobile: safeDraft.mobile || "",
          invoiceNumber: safeDraft.invoice_number || "",
          invoiceDate: safeDraft.invoice_date || "",
          status: safeDraft.status || "DRAFT",
          company_id: safeDraft.company_id,
          created_by: safeDraft.created_by,
          updated_at: safeDraft.updated_at,
          created_at: safeDraft.created_at
        }
      : null,
    pendingItems,
    processedItems,
    items: [...pendingItems, ...processedItems]
  };
}

function parseBooleanLike(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "true" || normalized === "1" || normalized === "yes" || normalized === "on";
}

function getTodayDateOnly() {
  return new Date().toISOString().slice(0, 10);
}

function buildVoucherNo(transactionType) {
  const prefix = String(transactionType || "TXN")
    .replace(/[^A-Z]/g, "")
    .slice(0, 4) || "TXN";
  return `${prefix}-${Date.now()}`;
}

function getRequestedCompanyId(req) {
  const raw =
    req.query.companyId ??
    req.body.companyId ??
    req.body.company_id ??
    req.params.companyId ??
    null;

  if (raw === null || raw === undefined || raw === "") return null;

  const parsed = Number(raw);
  return Number.isNaN(parsed) ? null : parsed;
}

function getRequestedUserId(req) {
  const raw =
    req.body.userId ??
    req.body.user_id ??
    req.body.createdBy ??
    req.query.userId ??
    null;

  if (raw === null || raw === undefined || raw === "") return null;

  const parsed = Number(raw);
  return Number.isNaN(parsed) ? null : parsed;
}

function getActingUserId(req) {
  const raw =
    req.body.actingUserId ??
    req.body.userId ??
    req.body.user_id ??
    req.query.actingUserId ??
    req.query.userId ??
    null;

  if (raw === null || raw === undefined || raw === "") return null;

  const parsed = Number(raw);
  return Number.isNaN(parsed) ? null : parsed;
}

function isSuperAdminUser(user) {
  if (!user) return false;
  return (
    String(user.role || "").trim().toLowerCase() === "superadmin" ||
    String(user.email || "").trim().toLowerCase() === "grudrapratap0@gmail.com"
  );
}

function isApprovedAdminUser(user) {
  if (!user) return false;
  return (
    String(user.role || "").trim().toLowerCase() === "admin" &&
    String(user.status || "").trim().toLowerCase() === "approved"
  );
}

const APPROVABLE_ROLES = [
  "Admin",
  "Billing",
  "Invoice",
  "Stock",
  "Sticker",
  "Process",
  "Expense",
  "Transaction"
];

function normalizeApprovedRole(value) {
  const clean = String(value || "").trim().toLowerCase();
  return APPROVABLE_ROLES.find((role) => role.toLowerCase() === clean) || null;
}

function sendAccessError(res, access) {
  return res.status(access.status || 403).json({
    success: false,
    message: access.message || "Access denied"
  });
}

async function resolveAccessContext(
  req,
  {
    requireActingUser = true,
    requireCompanyScope = false,
    allowSuperAdminAll = true
  } = {}
) {
  const requestedCompanyId = getRequestedCompanyId(req);
  const actingUserId = getActingUserId(req);

  if (!requireActingUser) {
    return {
      ok: true,
      actingUser: null,
      actingUserId,
      requestedCompanyId,
      actingCompanyId: null,
      isSuperAdmin: false,
      isApprovedAdmin: false,
      companyScope: requestedCompanyId
    };
  }

  if (actingUserId === null) {
    return {
      ok: false,
      status: 401,
      message: "actingUserId or userId is required"
    };
  }

  const actingUser = await findUserById(actingUserId);

  if (!actingUser) {
    return {
      ok: false,
      status: 401,
      message: "Acting user not found"
    };
  }

  const isSuperAdmin = isSuperAdminUser(actingUser);
  const isApprovedAdmin = isApprovedAdminUser(actingUser);
  const actingCompanyId =
    actingUser.company_id === null || actingUser.company_id === undefined
      ? null
      : Number(actingUser.company_id);

  if (isSuperAdmin) {
    if (requireCompanyScope && requestedCompanyId === null) {
      return {
        ok: false,
        status: 400,
        message: "companyId is required"
      };
    }

    return {
      ok: true,
      actingUser,
      actingUserId,
      requestedCompanyId,
      actingCompanyId,
      isSuperAdmin,
      isApprovedAdmin,
      companyScope: requestedCompanyId
    };
  }

  if (String(actingUser.status || "").trim().toLowerCase() !== "approved") {
    return {
      ok: false,
      status: 403,
      message: "Only approved users can access protected data"
    };
  }

  if (actingCompanyId === null || Number.isNaN(actingCompanyId)) {
    return {
      ok: false,
      status: 403,
      message: "Company scope is missing from your account"
    };
  }

  if (requestedCompanyId !== null && Number(requestedCompanyId) !== actingCompanyId) {
    return {
      ok: false,
      status: 403,
      message: "You cannot access data from another company"
    };
  }

  if (requireCompanyScope && actingCompanyId === null) {
    return {
      ok: false,
      status: 400,
      message: "companyId is required"
    };
  }

  return {
    ok: true,
    actingUser,
    actingUserId,
    requestedCompanyId,
    actingCompanyId,
    isSuperAdmin,
    isApprovedAdmin,
    companyScope: actingCompanyId
  };
}

async function requireSuperAdminAccess(req, res) {
  const access = await resolveAccessContext(req, {
    requireActingUser: true,
    requireCompanyScope: false,
    allowSuperAdminAll: true
  });

  if (!access.ok) {
    sendAccessError(res, access);
    return null;
  }

  if (!access.isSuperAdmin) {
    sendAccessError(res, {
      status: 403,
      message: "Only the SuperAdmin has access"
    });
    return null;
  }

  return access;
}

async function validateInvoiceSaveRequest(connection, invoiceNumber, items, companyId) {
  const cleanInvoiceNumber = String(invoiceNumber || "").trim();

  if (!cleanInvoiceNumber) {
    return {
      ok: false,
      status: 400,
      message: "Invoice number missing"
    };
  }

  if (!Array.isArray(items) || items.length === 0) {
    return {
      ok: false,
      status: 400,
      message: "Items missing"
    };
  }

  const [existingInvoices] = await connection.query(
    `
    SELECT id
    FROM sales_history
    WHERE invoice_number = ? AND company_id = ?
    LIMIT 1
    `,
    [cleanInvoiceNumber, companyId]
  );

  if (existingInvoices.length > 0) {
    return {
      ok: false,
      status: 400,
      message: "This invoice number already exists in this company"
    };
  }

  for (const item of items) {
    const barcode = String(item.barcode || "").trim();

    if (!barcode) continue;

    const [stockRows] = await connection.query(
      `
      SELECT id, status, company_id
      FROM stock
      WHERE barcode = ? AND company_id = ?
      LIMIT 1
      `,
      [barcode, companyId]
    );

    if (!stockRows.length) {
      return {
        ok: false,
        status: 400,
        message: `Barcode ${barcode} was not found in this company's stock`
      };
    }

    const stockRow = stockRows[0];
    const stockStatus = String(stockRow.status || "").trim().toUpperCase();

    if (stockStatus !== "IN_STOCK") {
      return {
        ok: false,
        status: 400,
        message: `Barcode ${barcode} is not in sellable stock`
      };
    }
  }

  return {
    ok: true,
    invoiceNumber: cleanInvoiceNumber
  };
}

async function getCurrentInvoiceDraft(connection, companyId, userId) {
  const [rows] = await connection.query(
    `
    SELECT *
    FROM invoice_drafts
    WHERE company_id = ?
      AND created_by = ?
      AND UPPER(COALESCE(status, 'DRAFT')) = 'DRAFT'
    ORDER BY id DESC
    LIMIT 1
    `,
    [companyId, userId]
  );

  return rows[0] || null;
}

async function getOrCreateCurrentInvoiceDraft(connection, companyId, userId) {
  const existingDraft = await getCurrentInvoiceDraft(connection, companyId, userId);
  if (existingDraft) return existingDraft;

  const [insertResult] = await connection.query(
    `
    INSERT INTO invoice_drafts
    (
      company_id,
      customer_name,
      mobile,
      invoice_number,
      invoice_date,
      status,
      created_by,
      updated_by,
      created_at,
      updated_at
    )
    VALUES (?, '', '', '', NULL, 'DRAFT', ?, ?, NOW(), NOW())
    `,
    [companyId, userId, userId]
  );

  const [rows] = await connection.query(
    `
    SELECT *
    FROM invoice_drafts
    WHERE id = ?
    LIMIT 1
    `,
    [insertResult.insertId]
  );

  return rows[0] || null;
}

async function getInvoiceDraftItems(connection, draftId) {
  const [rows] = await connection.query(
    `
    SELECT *
    FROM invoice_draft_items
    WHERE draft_id = ?
    ORDER BY id ASC
    `,
    [draftId]
  );

  return rows;
}

async function getInvoiceDraftPayload(connection, draftId) {
  if (!draftId) {
    return mapInvoiceDraftPayload(null, []);
  }

  const [draftRows] = await connection.query(
    `
    SELECT *
    FROM invoice_drafts
    WHERE id = ?
    LIMIT 1
    `,
    [draftId]
  );

  const draftRow = draftRows[0] || null;
  const itemRows = draftRow ? await getInvoiceDraftItems(connection, draftId) : [];
  return mapInvoiceDraftPayload(draftRow, itemRows);
}

async function setSaleStatusAndSyncStock(connection, invoiceNumber, companyId, saleStatus) {
  const cleanInvoiceNumber = String(invoiceNumber || "").trim();

  const [saleRows] = await connection.query(
    `
    SELECT id
    FROM sales_history
    WHERE invoice_number = ? AND company_id = ?
    LIMIT 1
    `,
    [cleanInvoiceNumber, companyId]
  );

  if (!saleRows.length) {
    return {
      ok: false,
      status: 404,
      message: "Sale not found"
    };
  }

  const [itemRows] = await connection.query(
    `
    SELECT barcode
    FROM sales_items
    WHERE invoice_number = ? AND company_id = ?
    `,
    [cleanInvoiceNumber, companyId]
  );

  await connection.query(
    `
    UPDATE sales_history
    SET status = ?
    WHERE invoice_number = ? AND company_id = ?
    `,
    [saleStatus, cleanInvoiceNumber, companyId]
  );

  for (const item of itemRows) {
    const barcode = String(item.barcode || "").trim();

    if (!barcode) continue;

    if (saleStatus === "DELETED") {
      await connection.query(
        `
        UPDATE stock
        SET status = 'IN_STOCK',
            invoice_number = '',
            sold_at = NULL
        WHERE barcode = ? AND company_id = ?
        `,
        [barcode, companyId]
      );
      continue;
    }

    await connection.query(
      `
      UPDATE stock
      SET status = 'SOLD',
          invoice_number = ?,
          sold_at = COALESCE(sold_at, NOW())
      WHERE barcode = ? AND company_id = ?
      `,
      [cleanInvoiceNumber, barcode, companyId]
    );
  }

  return {
    ok: true
  };
}

async function getLatestSaleItemByBarcode(connection, barcode, companyId) {
  const [rows] = await connection.query(
    `
    SELECT
      si.id,
      si.invoice_number,
      si.customer_name,
      si.product_name,
      si.sku,
      si.purity,
      si.size,
      si.weight,
      si.lot_number,
      si.item_status,
      si.return_type,
      si.returned_at,
      si.return_id,
      si.return_transaction_id,
      sh.id AS sale_id,
      sh.customer_name AS sale_customer_name,
      sh.mobile AS sale_mobile,
      sh.gst_number AS sale_gst_number,
      sh.payment_mode,
      sh.payment_status,
      sh.total_amount,
      sh.total_weight,
      sh.rate_per_gram,
      sh.mc_rate,
      sh.subtotal,
      sh.status AS sale_status,
      sh.invoice_date,
      sh.created_at AS sale_created_at
    FROM sales_items si
    LEFT JOIN sales_history sh
      ON sh.invoice_number = si.invoice_number
     AND sh.company_id = si.company_id
    WHERE si.barcode = ? AND si.company_id = ?
    ORDER BY si.id DESC
    LIMIT 1
    `,
    [barcode, companyId]
  );

  return rows.length ? rows[0] : null;
}

async function getCompanySettingsForCompany(connection, companyId) {
  const [rows] = await connection.query(
    `
    SELECT *
    FROM company_settings
    WHERE company_id = ?
    ORDER BY id DESC
    LIMIT 1
    `,
    [companyId]
  );

  return rows[0] || null;
}

function estimateReturnLineAmount(saleItem, saleRow) {
  const weight = toNumber(saleItem?.weight);
  const purity = toNumber(saleItem?.purity) > 0 ? toNumber(saleItem?.purity) : 100;
  const pureWeight = weight * (purity / 100);
  const rate = toNumber(saleRow?.rate_per_gram);
  const mcRate = toNumber(saleRow?.mc_rate);
  const estimatedSubtotal = pureWeight * rate + pureWeight * mcRate;
  const saleSubtotal = toNumber(saleRow?.subtotal);
  const saleTotalAmount = toNumber(saleRow?.total_amount);

  if (saleSubtotal > 0 && saleTotalAmount > 0) {
    return estimatedSubtotal * (saleTotalAmount / saleSubtotal);
  }

  return estimatedSubtotal;
}

async function postReturnToTransactionFoundation(connection, payload) {
  const companyId = Number(payload.companyId);
  const createdBy = payload.createdBy ?? null;
  const saleItem = payload.saleItem || null;
  const saleRow = payload.saleRow || null;
  const invoiceNumber = String(payload.invoiceNumber || saleItem?.invoice_number || "").trim();
  const customerName = String(payload.customerName || saleItem?.customer_name || saleRow?.customer_name || "").trim();
  const mobile = String(payload.mobile || saleRow?.mobile || "").trim();
  const gstNo = String(payload.gstNo || saleRow?.gst_number || "").trim();
  const returnType = normalizeReturnType(payload.returnType);
  const returnReason = String(payload.returnReason || "").trim();
  const returnDate = String(payload.returnDate || getTodayDateOnly()).trim();
  const productName = String(payload.productName || saleItem?.product_name || "").trim();
  const barcode = String(payload.barcode || saleItem?.barcode || "").trim();
  const lotNumber = String(payload.lotNumber || saleItem?.lot_number || "").trim();
  const grossWeight = toNumber(payload.weight || saleItem?.weight);
  const purity = toNumber(saleItem?.purity);
  const lineAmount = estimateReturnLineAmount(saleItem, saleRow);

  const party = await findOrCreateBillingParty(connection, {
    companyId,
    createdBy,
    partyName: customerName || "RETURN CUSTOMER",
    mobile,
    gstNo
  });

  const voucherNo = `RET-${invoiceNumber || Date.now()}-${barcode || Date.now()}`;
  const [txnInsert] = await connection.query(
    `
    INSERT INTO transaction_master
    (
      company_id, voucher_no, voucher_date, transaction_type, party_id, party_type,
      status, reference_no, invoice_no, source_module, payment_mode, payment_status,
      remarks, note, created_by
    )
    VALUES (?, ?, ?, 'SALE_RETURN', ?, ?, 'POSTED', ?, ?, 'return', ?, ?, ?, ?, ?)
    `,
    [
      companyId,
      voucherNo,
      returnDate || null,
      party.id,
      party.party_type || "CUSTOMER",
      barcode || invoiceNumber,
      invoiceNumber,
      String(saleRow?.payment_mode || "").trim(),
      returnType,
      "Auto-posted from return module",
      `${returnType} | ${returnReason || "No reason"}`,
      createdBy
    ]
  );

  const transactionId = txnInsert.insertId;

  await connection.query(
    `
    INSERT INTO transaction_lines
    (
      transaction_id, line_no, item_name, barcode, lot_no, purity,
      gross_weight, fine_weight, qty, rate_per_gram, making_charge,
      line_amount, remarks
    )
    VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      transactionId,
      productName,
      barcode,
      lotNumber,
      purity,
      grossWeight,
      calculateFineWeight(grossWeight, purity),
      1,
      toNumber(saleRow?.rate_per_gram),
      toNumber(saleRow?.mc_rate),
      lineAmount,
      returnType === "DAMAGED_RETURN" ? "Damaged return adjustment" : "Return to stock adjustment"
    ]
  );

  await connection.query(
    `
    INSERT INTO invoice_transaction_link
    (company_id, invoice_no, transaction_id, link_type, remarks, created_by)
    VALUES (?, ?, ?, 'SALE_RETURN', ?, ?)
    `,
    [companyId, invoiceNumber, transactionId, "Return transaction posting", createdBy]
  );

  if (lotNumber) {
    await connection.query(
      `
      INSERT INTO lot_transaction_link
      (company_id, lot_no, process_lot_no, transaction_id, link_type, remarks, created_by)
      VALUES (?, ?, ?, ?, 'SALE_RETURN', ?, ?)
      `,
      [companyId, lotNumber, lotNumber, transactionId, "Return linked to lot", createdBy]
    );
  }

  await createCashLedgerEntry(connection, {
    companyId,
    partyId: party.id,
    transactionId,
    entryDate: returnDate,
    entryType: "CREDIT",
    debitAmount: 0,
    creditAmount: lineAmount,
    referenceType: "SALE_RETURN",
    referenceNo: voucherNo,
    remarks: `Return credit for ${invoiceNumber || barcode}`,
    createdBy
  });

  await recalcPartyBalanceSummary(connection, companyId, party.id, transactionId);

  return {
    transactionId,
    partyId: party.id,
    estimatedAmount: lineAmount
  };
}

async function getReturnSummaryRows(companyId) {
  const whereClause = companyId !== null ? "WHERE company_id = ?" : "";
  const params = companyId !== null ? [companyId] : [];

  const [rows] = await pool.query(
    `
    SELECT
      COUNT(*) AS total_returns,
      SUM(CASE WHEN UPPER(COALESCE(return_type, '')) = 'RETURN_TO_STOCK' THEN 1 ELSE 0 END) AS return_to_stock_count,
      SUM(CASE WHEN UPPER(COALESCE(return_type, '')) = 'DAMAGED_RETURN' THEN 1 ELSE 0 END) AS damaged_return_count,
      SUM(CASE WHEN DATE(return_date) = CURDATE() THEN 1 ELSE 0 END) AS today_returns
    FROM return_history
    ${whereClause}
    `,
    params
  );

  const [recentRows] = await pool.query(
    `
    SELECT
      id,
      barcode,
      invoice_number,
      customer_name,
      product_name,
      return_type,
      return_reason,
      return_date,
      company_id,
      created_by,
      created_at
    FROM return_history
    ${whereClause}
    ORDER BY id DESC
    LIMIT 10
    `,
    params
  );

  return {
    totalReturns: Number(rows[0]?.total_returns || 0),
    returnToStockCount: Number(rows[0]?.return_to_stock_count || 0),
    damagedReturnCount: Number(rows[0]?.damaged_return_count || 0),
    todayReturns: Number(rows[0]?.today_returns || 0),
    recentReturns: recentRows
  };
}

function normalizeMaterialMovementType(value) {
  const clean = String(value || "").trim().toUpperCase();
  if (clean === "OPENING") return "OPENING";
  if (clean === "IN" || clean === "STOCK_IN") return "IN";
  if (clean === "OUT" || clean === "STOCK_OUT" || clean === "USED") return "OUT";
  if (clean === "ADJUSTMENT" || clean === "ADJUST") return "ADJUSTMENT";
  return "";
}

function getMaterialStockStatus(currentStock, lowStockLevel) {
  const current = Number(currentStock || 0);
  const lowLevel = Number(lowStockLevel || 0);

  if (current <= 0) return "OUT_OF_STOCK";
  if (current <= lowLevel) return "LOW_STOCK";
  return "IN_STOCK";
}

async function syncMaterialStockBalance(connection, materialId, companyId = null) {
  const materialParams = [materialId];
  const materialFilter = companyId !== null ? "AND company_id = ?" : "";

  if (companyId !== null) {
    materialParams.push(companyId);
  }

  const [materialRows] = await connection.query(
    `
    SELECT id, low_stock_level
    FROM material_stock_items
    WHERE id = ?
    ${materialFilter}
    LIMIT 1
    `,
    materialParams
  );

  if (!materialRows.length) return null;

  const movementParams = [materialId];
  const movementFilter = companyId !== null ? "AND company_id = ?" : "";

  if (companyId !== null) {
    movementParams.push(companyId);
  }

  const [balanceRows] = await connection.query(
    `
    SELECT
      SUM(CASE WHEN movement_type = 'OPENING' THEN qty ELSE 0 END) AS opening_total,
      SUM(CASE WHEN movement_type = 'IN' THEN qty ELSE 0 END) AS total_in,
      SUM(CASE WHEN movement_type = 'OUT' THEN qty ELSE 0 END) AS total_out,
      SUM(CASE WHEN movement_type = 'ADJUSTMENT' THEN qty ELSE 0 END) AS total_adjustment
    FROM material_stock_movements
    WHERE material_id = ?
    ${movementFilter}
    `,
    movementParams
  );

  const openingTotal = Number(balanceRows[0]?.opening_total || 0);
  const totalIn = Number(balanceRows[0]?.total_in || 0);
  const totalOut = Number(balanceRows[0]?.total_out || 0);
  const totalAdjustment = Number(balanceRows[0]?.total_adjustment || 0);
  const currentStock = openingTotal + totalIn + totalAdjustment - totalOut;
  const status = getMaterialStockStatus(currentStock, materialRows[0].low_stock_level);

  await connection.query(
    `
    UPDATE material_stock_items
    SET opening_stock = ?,
        current_stock = ?,
        status = ?,
        updated_at = NOW()
    WHERE id = ?
    `,
    [openingTotal, currentStock, status, materialId]
  );

  return {
    openingStock: openingTotal,
    totalIn,
    totalOut,
    totalAdjustment,
    currentStock,
    status
  };
}

async function getMaterialStockSummaryRows(companyId) {
  const whereClause = companyId !== null ? "WHERE msi.company_id = ?" : "";
  const params = companyId !== null ? [companyId] : [];

  const [itemSummaryRows] = await pool.query(
    `
    SELECT
      COUNT(*) AS total_material_types,
      COALESCE(SUM(current_stock), 0) AS total_current_stock,
      SUM(CASE WHEN status = 'LOW_STOCK' THEN 1 ELSE 0 END) AS low_stock_items,
      SUM(CASE WHEN status = 'OUT_OF_STOCK' THEN 1 ELSE 0 END) AS out_of_stock_items
    FROM material_stock_items msi
    ${whereClause}
    `,
    params
  );

  const movementWhereClause = companyId !== null ? "WHERE company_id = ?" : "";
  const movementParams = companyId !== null ? [companyId] : [];

  const [movementRows] = await pool.query(
    `
    SELECT
      COALESCE(SUM(CASE WHEN movement_type = 'IN' AND DATE(movement_date) = CURDATE() THEN qty ELSE 0 END), 0) AS today_stock_in,
      COALESCE(SUM(CASE WHEN movement_type = 'OUT' AND DATE(movement_date) = CURDATE() THEN qty ELSE 0 END), 0) AS today_stock_out
    FROM material_stock_movements
    ${movementWhereClause}
    `,
    movementParams
  );

  return {
    totalMaterialTypes: Number(itemSummaryRows[0]?.total_material_types || 0),
    totalCurrentStock: Number(itemSummaryRows[0]?.total_current_stock || 0),
    lowStockItems: Number(itemSummaryRows[0]?.low_stock_items || 0),
    outOfStockItems: Number(itemSummaryRows[0]?.out_of_stock_items || 0),
    todayStockIn: Number(movementRows[0]?.today_stock_in || 0),
    todayStockOut: Number(movementRows[0]?.today_stock_out || 0)
  };
}

function normalizeReportDateInput(value) {
  const raw = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const parsed = new Date(raw || Date.now());
  if (Number.isNaN(parsed.getTime())) {
    const today = new Date();
    return `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
  }

  return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}-${String(parsed.getDate()).padStart(2, "0")}`;
}

function getNextDateString(dateValue) {
  const parsed = new Date(`${dateValue}T00:00:00`);
  parsed.setDate(parsed.getDate() + 1);
  return `${parsed.getFullYear()}-${String(parsed.getMonth() + 1).padStart(2, "0")}-${String(parsed.getDate()).padStart(2, "0")}`;
}

async function handleUserApprovalAction(req, res, { action = "approve", label = "User" } = {}) {
  const targetUserId = Number(req.params.id);
  const access = await resolveAccessContext(req, {
    requireActingUser: true,
    requireCompanyScope: false,
    allowSuperAdminAll: true
  });

  if (!access.ok) {
    return sendAccessError(res, access);
  }

  if (!targetUserId) {
    return res.json({
      success: false,
      message: "User id is required"
    });
  }

  if (!access.isSuperAdmin && !access.isApprovedAdmin) {
    return res.json({
      success: false,
      message: `You do not have access to ${action === "approve" ? "approve" : "reject"}`
    });
  }

  const targetUser = await findUserById(targetUserId);

  if (!targetUser) {
    return res.json({
      success: false,
      message: "Target user not found"
    });
  }

  if (
    !access.isSuperAdmin &&
    (targetUser.company_id === null ||
      Number(targetUser.company_id) !== Number(access.actingCompanyId))
  ) {
    return res.json({
      success: false,
      message: `You can only ${action === "approve" ? "approve" : "reject"} your own company's ${label.toLowerCase()}`
    });
  }

  if (action === "approve") {
    const role = normalizeApprovedRole(req.body.role);

    if (!role) {
      return res.json({
        success: false,
        message: "A valid role is required"
      });
    }

    await pool.query(
      `
      UPDATE users
      SET role = ?, status = 'approved'
      WHERE id = ?
      `,
      [role, targetUserId]
    );

    return res.json({
      success: true,
      message: `${label} approved successfully`
    });
  }

  await pool.query(
    `
    UPDATE users
    SET status = 'rejected'
    WHERE id = ?
    `,
    [targetUserId]
  );

  return res.json({
    success: true,
    message: `${label} rejected successfully`
  });
}

async function testDbConnection() {
  const conn = await pool.getConnection();
  await conn.ping();
  conn.release();
}

async function tableExists(tableName) {
  const [rows] = await pool.query(
    `
    SELECT COUNT(*) AS total
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = ?
    `,
    [tableName]
  );
  return Number(rows[0]?.total || 0) > 0;
}

async function columnExists(tableName, columnName) {
  const [rows] = await pool.query(
    `
    SELECT COUNT(*) AS total
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = ?
      AND column_name = ?
    `,
    [tableName, columnName]
  );
  return Number(rows[0]?.total || 0) > 0;
}

async function addColumnIfMissing(tableName, columnName, definitionSql) {
  const exists = await columnExists(tableName, columnName);
  if (!exists) {
    await pool.query(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definitionSql}`);
    console.log(`Column added: ${tableName}.${columnName}`);
  }
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS companies (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_name VARCHAR(255) NOT NULL,
      owner_name VARCHAR(255) DEFAULT '',
      owner_email VARCHAR(255) DEFAULT '',
      status VARCHAR(50) DEFAULT 'active',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS company_signup_requests (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_name VARCHAR(255) NOT NULL,
      owner_name VARCHAR(255) NOT NULL,
      mobile VARCHAR(20) DEFAULT '',
      owner_email VARCHAR(255) NOT NULL,
      password VARCHAR(255) NOT NULL,
      status VARCHAR(50) DEFAULT 'pending',
      company_id INT DEFAULT NULL,
      approved_at DATETIME DEFAULT NULL,
      rejected_at DATETIME DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      mobile VARCHAR(20) DEFAULT '',
      email VARCHAR(255) NOT NULL UNIQUE,
      password VARCHAR(255) NOT NULL,
      role VARCHAR(50) DEFAULT '',
      company_id INT DEFAULT NULL,
      status VARCHAR(50) DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS stock (
      id INT AUTO_INCREMENT PRIMARY KEY,
      serial VARCHAR(50) DEFAULT '',
      product_name VARCHAR(255) DEFAULT '',
      purity VARCHAR(50) DEFAULT '',
      sku VARCHAR(100) DEFAULT '',
      mm VARCHAR(50) DEFAULT '',
      size VARCHAR(100) DEFAULT '',
      weight DECIMAL(10,3) DEFAULT 0.000,
      qty INT DEFAULT 1,
      lot_number VARCHAR(100) DEFAULT '',
      barcode VARCHAR(255) DEFAULT '',
      metal_type VARCHAR(50) DEFAULT '',
      process_type VARCHAR(100) DEFAULT '',
      status VARCHAR(50) DEFAULT 'IN_STOCK',
      invoice_number VARCHAR(100) DEFAULT '',
      sold_at DATETIME DEFAULT NULL,
      company_id INT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      deleted_at DATETIME DEFAULT NULL
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sales_history (
      id INT AUTO_INCREMENT PRIMARY KEY,
      invoice_number VARCHAR(100) DEFAULT '',
      customer_name VARCHAR(255) DEFAULT '',
      mobile VARCHAR(20) DEFAULT '',
      gst_number VARCHAR(100) DEFAULT '',
      invoice_date VARCHAR(50) DEFAULT '',
      payment_mode VARCHAR(50) DEFAULT '',
      payment_status VARCHAR(50) DEFAULT '',
      paid_amount DECIMAL(12,2) DEFAULT 0.00,
      due_amount DECIMAL(12,2) DEFAULT 0.00,
      total_items INT DEFAULT 0,
      total_weight DECIMAL(12,3) DEFAULT 0.000,
      rate_per_gram DECIMAL(12,2) DEFAULT 0.00,
      mc_rate DECIMAL(12,2) DEFAULT 0.00,
      round_off DECIMAL(12,2) DEFAULT 0.00,
      subtotal DECIMAL(12,2) DEFAULT 0.00,
      total_amount DECIMAL(12,2) DEFAULT 0.00,
      status VARCHAR(50) DEFAULT 'ACTIVE',
      company_id INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sales_items (
      id INT AUTO_INCREMENT PRIMARY KEY,
      sale_id INT DEFAULT NULL,
      invoice_number VARCHAR(100) DEFAULT '',
      barcode VARCHAR(255) DEFAULT '',
      product_name VARCHAR(255) DEFAULT '',
      sku VARCHAR(100) DEFAULT '',
      purity VARCHAR(50) DEFAULT '',
      size VARCHAR(100) DEFAULT '',
      weight DECIMAL(10,3) DEFAULT 0.000,
      lot_number VARCHAR(100) DEFAULT '',
      customer_name VARCHAR(255) DEFAULT '',
      company_id INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS return_history (
      id INT AUTO_INCREMENT PRIMARY KEY,
      barcode VARCHAR(255) DEFAULT '',
      invoice_number VARCHAR(100) DEFAULT '',
      customer_name VARCHAR(255) DEFAULT '',
      product_name VARCHAR(255) DEFAULT '',
      sku VARCHAR(100) DEFAULT '',
      size VARCHAR(100) DEFAULT '',
      weight DECIMAL(10,3) DEFAULT 0.000,
      lot_number VARCHAR(100) DEFAULT '',
      return_type VARCHAR(50) DEFAULT '',
      return_reason VARCHAR(255) DEFAULT '',
      return_date DATETIME DEFAULT CURRENT_TIMESTAMP,
      company_id INT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS material_stock_items (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      category VARCHAR(120) DEFAULT '',
      material_name VARCHAR(255) DEFAULT '',
      variant VARCHAR(255) DEFAULT '',
      size VARCHAR(120) DEFAULT '',
      unit VARCHAR(50) DEFAULT '',
      opening_stock DECIMAL(12,3) DEFAULT 0.000,
      current_stock DECIMAL(12,3) DEFAULT 0.000,
      low_stock_level DECIMAL(12,3) DEFAULT 0.000,
      supplier_name VARCHAR(255) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      status VARCHAR(50) DEFAULT 'IN_STOCK',
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS material_stock_movements (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      material_id INT NOT NULL,
      movement_type VARCHAR(50) DEFAULT '',
      qty DECIMAL(12,3) DEFAULT 0.000,
      unit VARCHAR(50) DEFAULT '',
      movement_date DATETIME DEFAULT CURRENT_TIMESTAMP,
      supplier_name VARCHAR(255) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      reference_no VARCHAR(120) DEFAULT '',
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS party_master (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      party_code VARCHAR(80) DEFAULT '',
      party_name VARCHAR(255) NOT NULL,
      display_name VARCHAR(255) DEFAULT '',
      party_type VARCHAR(50) DEFAULT '',
      status VARCHAR(50) DEFAULT 'ACTIVE',
      mobile VARCHAR(20) DEFAULT '',
      alternate_mobile VARCHAR(20) DEFAULT '',
      gst_no VARCHAR(100) DEFAULT '',
      pan_no VARCHAR(50) DEFAULT '',
      address_line1 VARCHAR(255) DEFAULT '',
      address_line2 VARCHAR(255) DEFAULT '',
      city VARCHAR(120) DEFAULT '',
      state VARCHAR(120) DEFAULT '',
      pin_code VARCHAR(20) DEFAULT '',
      contact_person VARCHAR(255) DEFAULT '',
      default_metal_type VARCHAR(20) DEFAULT '',
      default_purity DECIMAL(8,3) DEFAULT 0.000,
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS party_opening_balance (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      party_id INT NOT NULL,
      opening_date DATE DEFAULT NULL,
      cash_receivable DECIMAL(14,2) DEFAULT 0.00,
      cash_payable DECIMAL(14,2) DEFAULT 0.00,
      gold_gross_receivable DECIMAL(14,3) DEFAULT 0.000,
      gold_gross_payable DECIMAL(14,3) DEFAULT 0.000,
      gold_fine_receivable DECIMAL(14,3) DEFAULT 0.000,
      gold_fine_payable DECIMAL(14,3) DEFAULT 0.000,
      silver_gross_receivable DECIMAL(14,3) DEFAULT 0.000,
      silver_gross_payable DECIMAL(14,3) DEFAULT 0.000,
      silver_fine_receivable DECIMAL(14,3) DEFAULT 0.000,
      silver_fine_payable DECIMAL(14,3) DEFAULT 0.000,
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS metal_master (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      metal_code VARCHAR(20) NOT NULL,
      metal_name VARCHAR(100) DEFAULT '',
      default_unit VARCHAR(20) DEFAULT 'GRAM',
      status VARCHAR(50) DEFAULT 'ACTIVE',
      remarks TEXT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS transaction_master (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      voucher_no VARCHAR(100) NOT NULL,
      voucher_date DATE DEFAULT NULL,
      voucher_time TIME DEFAULT NULL,
      transaction_type VARCHAR(60) DEFAULT '',
      party_id INT NOT NULL,
      party_type VARCHAR(50) DEFAULT '',
      status VARCHAR(50) DEFAULT 'POSTED',
      reference_no VARCHAR(120) DEFAULT '',
      invoice_no VARCHAR(120) DEFAULT '',
      purchase_no VARCHAR(120) DEFAULT '',
      lot_no VARCHAR(120) DEFAULT '',
      process_lot_no VARCHAR(120) DEFAULT '',
      karigar_id INT DEFAULT NULL,
      source_module VARCHAR(80) DEFAULT '',
      payment_mode VARCHAR(50) DEFAULT '',
      payment_status VARCHAR(50) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      note TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      approved_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS transaction_lines (
      id INT AUTO_INCREMENT PRIMARY KEY,
      transaction_id INT NOT NULL,
      line_no INT DEFAULT 1,
      item_name VARCHAR(255) DEFAULT '',
      item_id INT DEFAULT NULL,
      barcode VARCHAR(255) DEFAULT '',
      lot_no VARCHAR(120) DEFAULT '',
      metal_type VARCHAR(20) DEFAULT '',
      purity DECIMAL(8,3) DEFAULT 0.000,
      gross_weight DECIMAL(14,3) DEFAULT 0.000,
      net_weight DECIMAL(14,3) DEFAULT 0.000,
      fine_weight DECIMAL(14,3) DEFAULT 0.000,
      qty DECIMAL(14,3) DEFAULT 0.000,
      rate_per_gram DECIMAL(14,2) DEFAULT 0.00,
      metal_value DECIMAL(14,2) DEFAULT 0.00,
      making_charge DECIMAL(14,2) DEFAULT 0.00,
      hallmark_charge DECIMAL(14,2) DEFAULT 0.00,
      labour_charge DECIMAL(14,2) DEFAULT 0.00,
      other_charge DECIMAL(14,2) DEFAULT 0.00,
      discount_amount DECIMAL(14,2) DEFAULT 0.00,
      gst_amount DECIMAL(14,2) DEFAULT 0.00,
      line_amount DECIMAL(14,2) DEFAULT 0.00,
      remarks TEXT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS transaction_settlements (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      transaction_id INT NOT NULL,
      settlement_type VARCHAR(30) DEFAULT '',
      against_transaction_id INT DEFAULT NULL,
      against_invoice_no VARCHAR(120) DEFAULT '',
      against_voucher_no VARCHAR(120) DEFAULT '',
      cash_amount DECIMAL(14,2) DEFAULT 0.00,
      metal_type VARCHAR(20) DEFAULT '',
      gross_weight DECIMAL(14,3) DEFAULT 0.000,
      fine_weight DECIMAL(14,3) DEFAULT 0.000,
      purity DECIMAL(8,3) DEFAULT 0.000,
      rate_basis DECIMAL(14,2) DEFAULT 0.00,
      settlement_date DATE DEFAULT NULL,
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS cash_ledger (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      party_id INT NOT NULL,
      transaction_id INT NOT NULL,
      entry_date DATE DEFAULT NULL,
      entry_type VARCHAR(20) DEFAULT '',
      debit_amount DECIMAL(14,2) DEFAULT 0.00,
      credit_amount DECIMAL(14,2) DEFAULT 0.00,
      running_balance DECIMAL(14,2) DEFAULT 0.00,
      reference_type VARCHAR(60) DEFAULT '',
      reference_no VARCHAR(120) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS metal_ledger (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      party_id INT NOT NULL,
      transaction_id INT NOT NULL,
      entry_date DATE DEFAULT NULL,
      metal_type VARCHAR(20) DEFAULT '',
      entry_type VARCHAR(20) DEFAULT '',
      purity DECIMAL(8,3) DEFAULT 0.000,
      gross_in DECIMAL(14,3) DEFAULT 0.000,
      gross_out DECIMAL(14,3) DEFAULT 0.000,
      fine_in DECIMAL(14,3) DEFAULT 0.000,
      fine_out DECIMAL(14,3) DEFAULT 0.000,
      running_gross_balance DECIMAL(14,3) DEFAULT 0.000,
      running_fine_balance DECIMAL(14,3) DEFAULT 0.000,
      reference_type VARCHAR(60) DEFAULT '',
      reference_no VARCHAR(120) DEFAULT '',
      lot_no VARCHAR(120) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS party_balance_summary (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      party_id INT NOT NULL,
      cash_balance DECIMAL(14,2) DEFAULT 0.00,
      gold_gross_balance DECIMAL(14,3) DEFAULT 0.000,
      gold_fine_balance DECIMAL(14,3) DEFAULT 0.000,
      silver_gross_balance DECIMAL(14,3) DEFAULT 0.000,
      silver_fine_balance DECIMAL(14,3) DEFAULT 0.000,
      last_transaction_id INT DEFAULT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_transaction_link (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      invoice_no VARCHAR(120) DEFAULT '',
      transaction_id INT NOT NULL,
      link_type VARCHAR(60) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS purchase_transaction_link (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      purchase_no VARCHAR(120) DEFAULT '',
      transaction_id INT NOT NULL,
      link_type VARCHAR(60) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS lot_transaction_link (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      lot_no VARCHAR(120) DEFAULT '',
      process_lot_no VARCHAR(120) DEFAULT '',
      transaction_id INT NOT NULL,
      link_type VARCHAR(60) DEFAULT '',
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS karigar_transaction_link (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      karigar_id INT NOT NULL,
      transaction_id INT NOT NULL,
      lot_no VARCHAR(120) DEFAULT '',
      process_lot_no VARCHAR(120) DEFAULT '',
      issue_weight DECIMAL(14,3) DEFAULT 0.000,
      receive_weight DECIMAL(14,3) DEFAULT 0.000,
      loss_weight DECIMAL(14,3) DEFAULT 0.000,
      labour_amount DECIMAL(14,2) DEFAULT 0.00,
      remarks TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS process_lots (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      lot_no VARCHAR(120) NOT NULL,
      raw_weight DECIMAL(14,3) DEFAULT 0.000,
      loss_weight DECIMAL(14,3) DEFAULT 0.000,
      final_weight DECIMAL(14,3) DEFAULT 0.000,
      saved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS karigar_work (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      karigar_name VARCHAR(255) NOT NULL,
      lot_no VARCHAR(120) DEFAULT '',
      issue_weight DECIMAL(14,3) DEFAULT 0.000,
      receive_weight DECIMAL(14,3) DEFAULT 0.000,
      loss_weight DECIMAL(14,3) DEFAULT 0.000,
      labour_amount DECIMAL(14,2) DEFAULT 0.00,
      work_time DATETIME DEFAULT CURRENT_TIMESTAMP,
      created_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS expenses (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      person VARCHAR(255) DEFAULT '',
      expense_date DATE DEFAULT NULL,
      expense_time TIME DEFAULT NULL,
      amount DECIMAL(14,2) DEFAULT 0.00,
      category VARCHAR(120) DEFAULT '',
      reason VARCHAR(255) DEFAULT '',
      note TEXT DEFAULT NULL,
      created_by INT DEFAULT NULL,
      updated_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_drafts (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT DEFAULT NULL,
      customer_name VARCHAR(255) DEFAULT '',
      mobile VARCHAR(30) DEFAULT '',
      invoice_number VARCHAR(120) DEFAULT '',
      invoice_date DATE DEFAULT NULL,
      status VARCHAR(50) DEFAULT 'DRAFT',
      converted_invoice_no VARCHAR(120) DEFAULT '',
      created_by INT DEFAULT NULL,
      updated_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_draft_items (
      id INT AUTO_INCREMENT PRIMARY KEY,
      draft_id INT NOT NULL,
      company_id INT DEFAULT NULL,
      barcode VARCHAR(255) DEFAULT '',
      product_name VARCHAR(255) DEFAULT '',
      sku VARCHAR(120) DEFAULT '',
      purity VARCHAR(120) DEFAULT '',
      size VARCHAR(120) DEFAULT '',
      weight DECIMAL(14,3) DEFAULT 0.000,
      lot_number VARCHAR(120) DEFAULT '',
      item_stage VARCHAR(30) DEFAULT 'PENDING',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS company_settings (
      id INT AUTO_INCREMENT PRIMARY KEY,
      company_id INT NOT NULL,
      owner_email VARCHAR(255) DEFAULT '',
      top_title VARCHAR(255) DEFAULT '',
      company_name VARCHAR(255) DEFAULT '',
      gstin VARCHAR(120) DEFAULT '',
      account_no VARCHAR(120) DEFAULT '',
      ifsc VARCHAR(80) DEFAULT '',
      address TEXT DEFAULT NULL,
      declaration TEXT DEFAULT NULL,
      upi_id VARCHAR(255) DEFAULT '',
      upi_name VARCHAR(255) DEFAULT '',
      business_state VARCHAR(120) DEFAULT 'Odisha',
      default_bill_type VARCHAR(50) DEFAULT 'GST',
      default_tax_type VARCHAR(50) DEFAULT 'CGST_SGST',
      default_rate_per_gram DECIMAL(14,2) DEFAULT 0.00,
      default_mc_rate DECIMAL(14,2) DEFAULT 0.00,
      subscription_plan VARCHAR(80) DEFAULT 'basic',
      subscription_status VARCHAR(80) DEFAULT 'active',
      subscription_start_date DATE DEFAULT NULL,
      subscription_end_date DATE DEFAULT NULL,
      created_by INT DEFAULT NULL,
      updated_by INT DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await pool.query(
    `
    INSERT INTO metal_master (company_id, metal_code, metal_name, default_unit, status)
    SELECT NULL, 'GOLD', 'Gold', 'GRAM', 'ACTIVE'
    WHERE NOT EXISTS (
      SELECT 1 FROM metal_master WHERE company_id IS NULL AND metal_code = 'GOLD'
    )
    `
  );

  await pool.query(
    `
    INSERT INTO metal_master (company_id, metal_code, metal_name, default_unit, status)
    SELECT NULL, 'SILVER', 'Silver', 'GRAM', 'ACTIVE'
    WHERE NOT EXISTS (
      SELECT 1 FROM metal_master WHERE company_id IS NULL AND metal_code = 'SILVER'
    )
    `
  );

  if (await tableExists("users")) {
    await addColumnIfMissing("users", "mobile", "VARCHAR(20) DEFAULT ''");
    await addColumnIfMissing("users", "role", "VARCHAR(50) DEFAULT ''");
    await addColumnIfMissing("users", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("users", "status", "VARCHAR(50) DEFAULT 'pending'");
    await addColumnIfMissing("users", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
  }

  if (await tableExists("companies")) {
    await addColumnIfMissing("companies", "owner_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("companies", "owner_email", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("companies", "status", "VARCHAR(50) DEFAULT 'active'");
    await addColumnIfMissing("companies", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
  }

  if (await tableExists("company_signup_requests")) {
    await addColumnIfMissing("company_signup_requests", "mobile", "VARCHAR(20) DEFAULT ''");
    await addColumnIfMissing("company_signup_requests", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("company_signup_requests", "approved_at", "DATETIME DEFAULT NULL");
    await addColumnIfMissing("company_signup_requests", "rejected_at", "DATETIME DEFAULT NULL");
    await addColumnIfMissing("company_signup_requests", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
  }

  if (await tableExists("stock")) {
    await addColumnIfMissing("stock", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("stock", "qty", "INT DEFAULT 1");
    await addColumnIfMissing("stock", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("stock", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("stock", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
    await addColumnIfMissing("stock", "deleted_at", "DATETIME DEFAULT NULL");
    await addColumnIfMissing("stock", "invoice_number", "VARCHAR(100) DEFAULT ''");
    await addColumnIfMissing("stock", "sold_at", "DATETIME DEFAULT NULL");
  }

  if (await tableExists("sales_history")) {
    await addColumnIfMissing("sales_history", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("sales_history", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("sales_history", "total_items", "INT DEFAULT 0");
    await addColumnIfMissing("sales_history", "total_weight", "DECIMAL(12,3) DEFAULT 0.000");
    await addColumnIfMissing("sales_history", "status", "VARCHAR(50) DEFAULT 'ACTIVE'");
  }

  if (await tableExists("sales_items")) {
    await addColumnIfMissing("sales_items", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("sales_items", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("sales_items", "item_status", "VARCHAR(50) DEFAULT 'SOLD'");
    await addColumnIfMissing("sales_items", "return_type", "VARCHAR(50) DEFAULT ''");
    await addColumnIfMissing("sales_items", "returned_at", "DATETIME DEFAULT NULL");
    await addColumnIfMissing("sales_items", "return_id", "INT DEFAULT NULL");
    await addColumnIfMissing("sales_items", "return_transaction_id", "INT DEFAULT NULL");
  }

  if (await tableExists("return_history")) {
    await addColumnIfMissing("return_history", "barcode", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("return_history", "invoice_number", "VARCHAR(100) DEFAULT ''");
    await addColumnIfMissing("return_history", "customer_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("return_history", "product_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("return_history", "sku", "VARCHAR(100) DEFAULT ''");
    await addColumnIfMissing("return_history", "size", "VARCHAR(100) DEFAULT ''");
    await addColumnIfMissing("return_history", "weight", "DECIMAL(10,3) DEFAULT 0.000");
    await addColumnIfMissing("return_history", "lot_number", "VARCHAR(100) DEFAULT ''");
    await addColumnIfMissing("return_history", "return_type", "VARCHAR(50) DEFAULT ''");
    await addColumnIfMissing("return_history", "return_reason", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("return_history", "return_date", "DATETIME DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("return_history", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("return_history", "party_id", "INT DEFAULT NULL");
    await addColumnIfMissing("return_history", "estimated_amount", "DECIMAL(14,2) DEFAULT 0.00");
    await addColumnIfMissing("return_history", "transaction_id", "INT DEFAULT NULL");
    await addColumnIfMissing("return_history", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("return_history", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
  }

  if (await tableExists("material_stock_items")) {
    await addColumnIfMissing("material_stock_items", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("material_stock_items", "category", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("material_stock_items", "material_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("material_stock_items", "variant", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("material_stock_items", "size", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("material_stock_items", "unit", "VARCHAR(50) DEFAULT ''");
    await addColumnIfMissing("material_stock_items", "opening_stock", "DECIMAL(12,3) DEFAULT 0.000");
    await addColumnIfMissing("material_stock_items", "current_stock", "DECIMAL(12,3) DEFAULT 0.000");
    await addColumnIfMissing("material_stock_items", "low_stock_level", "DECIMAL(12,3) DEFAULT 0.000");
    await addColumnIfMissing("material_stock_items", "supplier_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("material_stock_items", "remarks", "TEXT DEFAULT NULL");
    await addColumnIfMissing("material_stock_items", "status", "VARCHAR(50) DEFAULT 'IN_STOCK'");
    await addColumnIfMissing("material_stock_items", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("material_stock_items", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("material_stock_items", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  if (await tableExists("material_stock_movements")) {
    await addColumnIfMissing("material_stock_movements", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("material_stock_movements", "material_id", "INT NOT NULL");
    await addColumnIfMissing("material_stock_movements", "movement_type", "VARCHAR(50) DEFAULT ''");
    await addColumnIfMissing("material_stock_movements", "qty", "DECIMAL(12,3) DEFAULT 0.000");
    await addColumnIfMissing("material_stock_movements", "unit", "VARCHAR(50) DEFAULT ''");
    await addColumnIfMissing("material_stock_movements", "movement_date", "DATETIME DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("material_stock_movements", "supplier_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("material_stock_movements", "remarks", "TEXT DEFAULT NULL");
    await addColumnIfMissing("material_stock_movements", "reference_no", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("material_stock_movements", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("material_stock_movements", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
  }

  if (await tableExists("process_lots")) {
    await addColumnIfMissing("process_lots", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("process_lots", "lot_no", "VARCHAR(120) NOT NULL");
    await addColumnIfMissing("process_lots", "raw_weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("process_lots", "loss_weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("process_lots", "final_weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("process_lots", "saved_at", "DATETIME DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("process_lots", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("process_lots", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("process_lots", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  if (await tableExists("karigar_work")) {
    await addColumnIfMissing("karigar_work", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("karigar_work", "karigar_name", "VARCHAR(255) NOT NULL");
    await addColumnIfMissing("karigar_work", "lot_no", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("karigar_work", "issue_weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("karigar_work", "receive_weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("karigar_work", "loss_weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("karigar_work", "labour_amount", "DECIMAL(14,2) DEFAULT 0.00");
    await addColumnIfMissing("karigar_work", "work_time", "DATETIME DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("karigar_work", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("karigar_work", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("karigar_work", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  if (await tableExists("expenses")) {
    await addColumnIfMissing("expenses", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("expenses", "person", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("expenses", "expense_date", "DATE DEFAULT NULL");
    await addColumnIfMissing("expenses", "expense_time", "TIME DEFAULT NULL");
    await addColumnIfMissing("expenses", "amount", "DECIMAL(14,2) DEFAULT 0.00");
    await addColumnIfMissing("expenses", "category", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("expenses", "reason", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("expenses", "note", "TEXT DEFAULT NULL");
    await addColumnIfMissing("expenses", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("expenses", "updated_by", "INT DEFAULT NULL");
    await addColumnIfMissing("expenses", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("expenses", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  if (await tableExists("invoice_drafts")) {
    await addColumnIfMissing("invoice_drafts", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("invoice_drafts", "customer_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("invoice_drafts", "mobile", "VARCHAR(30) DEFAULT ''");
    await addColumnIfMissing("invoice_drafts", "invoice_number", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("invoice_drafts", "invoice_date", "DATE DEFAULT NULL");
    await addColumnIfMissing("invoice_drafts", "status", "VARCHAR(50) DEFAULT 'DRAFT'");
    await addColumnIfMissing("invoice_drafts", "converted_invoice_no", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("invoice_drafts", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("invoice_drafts", "updated_by", "INT DEFAULT NULL");
    await addColumnIfMissing("invoice_drafts", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("invoice_drafts", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  if (await tableExists("invoice_draft_items")) {
    await addColumnIfMissing("invoice_draft_items", "draft_id", "INT NOT NULL");
    await addColumnIfMissing("invoice_draft_items", "company_id", "INT DEFAULT NULL");
    await addColumnIfMissing("invoice_draft_items", "barcode", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("invoice_draft_items", "product_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("invoice_draft_items", "sku", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("invoice_draft_items", "purity", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("invoice_draft_items", "size", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("invoice_draft_items", "weight", "DECIMAL(14,3) DEFAULT 0.000");
    await addColumnIfMissing("invoice_draft_items", "lot_number", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("invoice_draft_items", "item_stage", "VARCHAR(30) DEFAULT 'PENDING'");
    await addColumnIfMissing("invoice_draft_items", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("invoice_draft_items", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  if (await tableExists("company_settings")) {
    await addColumnIfMissing("company_settings", "company_id", "INT NOT NULL");
    await addColumnIfMissing("company_settings", "owner_email", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("company_settings", "top_title", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("company_settings", "company_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("company_settings", "gstin", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("company_settings", "account_no", "VARCHAR(120) DEFAULT ''");
    await addColumnIfMissing("company_settings", "ifsc", "VARCHAR(80) DEFAULT ''");
    await addColumnIfMissing("company_settings", "address", "TEXT DEFAULT NULL");
    await addColumnIfMissing("company_settings", "declaration", "TEXT DEFAULT NULL");
    await addColumnIfMissing("company_settings", "upi_id", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("company_settings", "upi_name", "VARCHAR(255) DEFAULT ''");
    await addColumnIfMissing("company_settings", "business_state", "VARCHAR(120) DEFAULT 'Odisha'");
    await addColumnIfMissing("company_settings", "default_bill_type", "VARCHAR(50) DEFAULT 'GST'");
    await addColumnIfMissing("company_settings", "default_tax_type", "VARCHAR(50) DEFAULT 'CGST_SGST'");
    await addColumnIfMissing("company_settings", "default_rate_per_gram", "DECIMAL(14,2) DEFAULT 0.00");
    await addColumnIfMissing("company_settings", "default_mc_rate", "DECIMAL(14,2) DEFAULT 0.00");
    await addColumnIfMissing("company_settings", "subscription_plan", "VARCHAR(80) DEFAULT 'basic'");
    await addColumnIfMissing("company_settings", "subscription_status", "VARCHAR(80) DEFAULT 'active'");
    await addColumnIfMissing("company_settings", "subscription_start_date", "DATE DEFAULT NULL");
    await addColumnIfMissing("company_settings", "subscription_end_date", "DATE DEFAULT NULL");
    await addColumnIfMissing("company_settings", "created_by", "INT DEFAULT NULL");
    await addColumnIfMissing("company_settings", "updated_by", "INT DEFAULT NULL");
    await addColumnIfMissing("company_settings", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP");
    await addColumnIfMissing("company_settings", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
  }

  console.log("Schema ensured ✅");
}

async function ensurePartyBalanceSummaryRow(connection, companyId, partyId) {
  await connection.query(
    `
    INSERT INTO party_balance_summary
    (
      company_id, party_id, cash_balance,
      gold_gross_balance, gold_fine_balance,
      silver_gross_balance, silver_fine_balance,
      updated_at
    )
    SELECT ?, ?, 0.00, 0.000, 0.000, 0.000, 0.000, NOW()
    WHERE NOT EXISTS (
      SELECT 1
      FROM party_balance_summary
      WHERE company_id = ? AND party_id = ?
    )
    `,
    [companyId, partyId, companyId, partyId]
  );
}

async function recalcPartyBalanceSummary(connection, companyId, partyId, lastTransactionId = null) {
  const [cashRows] = await connection.query(
    `
    SELECT COALESCE(SUM(debit_amount), 0) - COALESCE(SUM(credit_amount), 0) AS cash_balance
    FROM cash_ledger
    WHERE company_id = ? AND party_id = ?
    `,
    [companyId, partyId]
  );

  const [metalRows] = await connection.query(
    `
    SELECT
      metal_type,
      COALESCE(SUM(gross_in), 0) - COALESCE(SUM(gross_out), 0) AS gross_balance,
      COALESCE(SUM(fine_in), 0) - COALESCE(SUM(fine_out), 0) AS fine_balance
    FROM metal_ledger
    WHERE company_id = ? AND party_id = ?
    GROUP BY metal_type
    `,
    [companyId, partyId]
  );

  let goldGross = 0;
  let goldFine = 0;
  let silverGross = 0;
  let silverFine = 0;

  metalRows.forEach((row) => {
    const metalType = normalizeMetalType(row.metal_type);
    if (metalType === "GOLD") {
      goldGross = toNumber(row.gross_balance);
      goldFine = toNumber(row.fine_balance);
    }
    if (metalType === "SILVER") {
      silverGross = toNumber(row.gross_balance);
      silverFine = toNumber(row.fine_balance);
    }
  });

  await ensurePartyBalanceSummaryRow(connection, companyId, partyId);

  await connection.query(
    `
    UPDATE party_balance_summary
    SET cash_balance = ?,
        gold_gross_balance = ?,
        gold_fine_balance = ?,
        silver_gross_balance = ?,
        silver_fine_balance = ?,
        last_transaction_id = ?,
        updated_at = NOW()
    WHERE company_id = ? AND party_id = ?
    `,
    [
      toNumber(cashRows[0]?.cash_balance),
      goldGross,
      goldFine,
      silverGross,
      silverFine,
      lastTransactionId,
      companyId,
      partyId
    ]
  );
}

async function createCashLedgerEntry(connection, payload) {
  const companyId = Number(payload.companyId);
  const partyId = Number(payload.partyId);
  const transactionId = Number(payload.transactionId);
  const debitAmount = toNumber(payload.debitAmount);
  const creditAmount = toNumber(payload.creditAmount);

  const [balanceRows] = await connection.query(
    `
    SELECT running_balance
    FROM cash_ledger
    WHERE company_id = ? AND party_id = ?
    ORDER BY id DESC
    LIMIT 1
    `,
    [companyId, partyId]
  );

  const previousBalance = toNumber(balanceRows[0]?.running_balance);
  const runningBalance = previousBalance + debitAmount - creditAmount;

  await connection.query(
    `
    INSERT INTO cash_ledger
    (
      company_id, party_id, transaction_id, entry_date, entry_type,
      debit_amount, credit_amount, running_balance,
      reference_type, reference_no, remarks, created_by
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      companyId,
      partyId,
      transactionId,
      String(payload.entryDate || getTodayDateOnly()).trim(),
      normalizeCashEntryType(payload.entryType),
      debitAmount,
      creditAmount,
      runningBalance,
      String(payload.referenceType || "").trim(),
      String(payload.referenceNo || "").trim(),
      String(payload.remarks || "").trim(),
      payload.createdBy ?? null
    ]
  );
}

async function createMetalLedgerEntry(connection, payload) {
  const companyId = Number(payload.companyId);
  const partyId = Number(payload.partyId);
  const transactionId = Number(payload.transactionId);
  const metalType = normalizeMetalType(payload.metalType);
  const grossIn = toNumber(payload.grossIn);
  const grossOut = toNumber(payload.grossOut);
  const fineIn = toNumber(payload.fineIn);
  const fineOut = toNumber(payload.fineOut);

  const [balanceRows] = await connection.query(
    `
    SELECT running_gross_balance, running_fine_balance
    FROM metal_ledger
    WHERE company_id = ? AND party_id = ? AND metal_type = ?
    ORDER BY id DESC
    LIMIT 1
    `,
    [companyId, partyId, metalType]
  );

  const runningGrossBalance = toNumber(balanceRows[0]?.running_gross_balance) + grossIn - grossOut;
  const runningFineBalance = toNumber(balanceRows[0]?.running_fine_balance) + fineIn - fineOut;

  await connection.query(
    `
    INSERT INTO metal_ledger
    (
      company_id, party_id, transaction_id, entry_date, metal_type, entry_type,
      purity, gross_in, gross_out, fine_in, fine_out,
      running_gross_balance, running_fine_balance,
      reference_type, reference_no, lot_no, remarks, created_by
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      companyId,
      partyId,
      transactionId,
      String(payload.entryDate || getTodayDateOnly()).trim(),
      metalType,
      normalizeMetalEntryType(payload.entryType),
      toNumber(payload.purity),
      grossIn,
      grossOut,
      fineIn,
      fineOut,
      runningGrossBalance,
      runningFineBalance,
      String(payload.referenceType || "").trim(),
      String(payload.referenceNo || "").trim(),
      String(payload.lotNo || "").trim(),
      String(payload.remarks || "").trim(),
      payload.createdBy ?? null
    ]
  );
}

async function getPartyByIdForCompany(connection, companyId, partyId) {
  const [rows] = await connection.query(
    `
    SELECT *
    FROM party_master
    WHERE id = ? AND company_id = ?
    LIMIT 1
    `,
    [partyId, companyId]
  );

  return rows.length ? rows[0] : null;
}

async function findOrCreateBillingParty(connection, payload) {
  const companyId = Number(payload.companyId);
  const createdBy = payload.createdBy ?? null;
  const partyName = String(payload.partyName || "").trim();
  const mobile = String(payload.mobile || "").trim();
  const gstNo = String(payload.gstNo || "").trim();

  const [existingRows] = await connection.query(
    `
    SELECT *
    FROM party_master
    WHERE company_id = ?
      AND LOWER(TRIM(party_name)) = LOWER(TRIM(?))
      AND party_type IN ('CUSTOMER', 'CUSTOMER_SUPPLIER')
    ORDER BY CASE WHEN party_type = 'CUSTOMER' THEN 0 ELSE 1 END, id ASC
    LIMIT 1
    `,
    [companyId, partyName]
  );

  if (existingRows.length) {
    const party = existingRows[0];
    if ((!party.mobile && mobile) || (!party.gst_no && gstNo)) {
      await connection.query(
        `
        UPDATE party_master
        SET mobile = CASE WHEN TRIM(COALESCE(mobile, '')) = '' THEN ? ELSE mobile END,
            gst_no = CASE WHEN TRIM(COALESCE(gst_no, '')) = '' THEN ? ELSE gst_no END,
            updated_at = NOW()
        WHERE id = ?
        `,
        [mobile, gstNo, party.id]
      );
    }

    await ensurePartyBalanceSummaryRow(connection, companyId, party.id);
    return { ...party, mobile: party.mobile || mobile, gst_no: party.gst_no || gstNo };
  }

  const [insertResult] = await connection.query(
    `
    INSERT INTO party_master
    (
      company_id, party_code, party_name, display_name, party_type, status,
      mobile, gst_no, remarks, created_by
    )
    VALUES (?, ?, ?, ?, 'CUSTOMER', 'ACTIVE', ?, ?, ?, ?)
    `,
    [
      companyId,
      `CUST-${Date.now()}`,
      partyName,
      partyName,
      mobile,
      gstNo,
      "Auto-created from billing save",
      createdBy
    ]
  );

  await ensurePartyBalanceSummaryRow(connection, companyId, insertResult.insertId);

  return {
    id: insertResult.insertId,
    company_id: companyId,
    party_name: partyName,
    display_name: partyName,
    party_type: "CUSTOMER",
    mobile,
    gst_no: gstNo
  };
}

async function findOrCreateKarigarParty(connection, payload) {
  const companyId = Number(payload.companyId);
  const createdBy = payload.createdBy ?? null;
  const partyName = normalizeKarigarName(payload.partyName);

  if (!partyName) {
    throw new Error("Karigar party name missing");
  }

  const [existingRows] = await connection.query(
    `
    SELECT *
    FROM party_master
    WHERE company_id = ?
      AND LOWER(TRIM(party_name)) = LOWER(TRIM(?))
      AND party_type = 'KARIGAR'
    ORDER BY id ASC
    LIMIT 1
    `,
    [companyId, partyName]
  );

  if (existingRows.length) {
    const party = existingRows[0];
    await ensurePartyBalanceSummaryRow(connection, companyId, party.id);
    return party;
  }

  const [insertResult] = await connection.query(
    `
    INSERT INTO party_master
    (
      company_id, party_code, party_name, display_name, party_type, status,
      remarks, created_by
    )
    VALUES (?, ?, ?, ?, 'KARIGAR', 'ACTIVE', ?, ?)
    `,
    [
      companyId,
      `KAR-${Date.now()}`,
      partyName,
      partyName,
      "Auto-created from process karigar work",
      createdBy
    ]
  );

  await ensurePartyBalanceSummaryRow(connection, companyId, insertResult.insertId);

  return {
    id: insertResult.insertId,
    company_id: companyId,
    party_name: partyName,
    display_name: partyName,
    party_type: "KARIGAR",
    default_metal_type: "",
    default_purity: 0
  };
}

function getPurityRatio(value) {
  const purity = toNumber(value);
  if (purity <= 0) return 0;
  if (purity > 100) return purity / 1000;
  if (purity > 1) return purity / 100;
  return purity;
}

function calculateFineWeight(grossWeight, purity) {
  return toNumber(grossWeight) * getPurityRatio(purity);
}

async function getLotMetalContext(connection, payload) {
  const companyId = Number(payload.companyId);
  const lotNo = normalizeProcessLotNo(payload.lotNo);
  const party = payload.party || null;

  const [stockRows] = await connection.query(
    `
    SELECT metal_type, purity
    FROM stock
    WHERE company_id = ?
      AND lot_number = ?
      AND UPPER(COALESCE(status, 'IN_STOCK')) <> 'DELETED'
    ORDER BY id ASC
    `,
    [companyId, lotNo]
  );

  for (const row of stockRows) {
    const metalType = normalizeMetalType(row.metal_type);
    if (!metalType) continue;

    return {
      metalType,
      purity: toNumber(row.purity)
    };
  }

  const fallbackMetalType = normalizeMetalType(party?.default_metal_type);
  if (fallbackMetalType) {
    return {
      metalType: fallbackMetalType,
      purity: toNumber(party?.default_purity)
    };
  }

  throw new Error(`Metal context was not found for lot ${lotNo}`);
}

async function createProcessKarigarTransaction(connection, payload) {
  const companyId = Number(payload.companyId);
  const createdBy = payload.createdBy ?? null;
  const partyId = Number(payload.partyId);
  const karigarId = Number(payload.karigarId || payload.partyId);
  const transactionType = normalizeTransactionType(payload.transactionType);
  const voucherDate = String(payload.voucherDate || getTodayDateOnly()).trim() || getTodayDateOnly();
  const voucherNo = String(payload.voucherNo || buildVoucherNo(transactionType)).trim();
  const lotNo = normalizeProcessLotNo(payload.lotNo);
  const processLotNo = normalizeProcessLotNo(payload.processLotNo || lotNo);
  const metalType = normalizeMetalType(payload.metalType);
  const purity = toNumber(payload.purity);
  const grossWeight = toNumber(payload.grossWeight);
  const fineWeight = calculateFineWeight(grossWeight, purity);
  const remarks = String(payload.remarks || "").trim();

  if (!partyId || !transactionType || !metalType || grossWeight <= 0) {
    return null;
  }

  const party = await getPartyByIdForCompany(connection, companyId, partyId);
  if (!party) {
    throw new Error("Karigar party not found");
  }

  const finalPartyType = normalizePartyType(party.party_type) || "KARIGAR";
  const metalEntryType =
    transactionType === "KARIGAR_ISSUE"
      ? "IN"
      : transactionType === "KARIGAR_RECEIVE" || transactionType === "KARIGAR_LOSS_ADJUSTMENT"
        ? "OUT"
        : "";

  if (!metalEntryType) {
    return null;
  }

  const [insertResult] = await connection.query(
    `
    INSERT INTO transaction_master
    (
      company_id, voucher_no, voucher_date, transaction_type, party_id, party_type,
      status, lot_no, process_lot_no, karigar_id, source_module,
      remarks, note, created_by
    )
    VALUES (?, ?, ?, ?, ?, ?, 'POSTED', ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      companyId,
      voucherNo,
      voucherDate,
      transactionType,
      partyId,
      finalPartyType,
      lotNo,
      processLotNo,
      karigarId,
      "process_module",
      remarks,
      "Auto-posted from process karigar work",
      createdBy
    ]
  );

  const transactionId = Number(insertResult.insertId);

  await connection.query(
    `
    INSERT INTO transaction_lines
    (
      transaction_id, line_no, item_name, lot_no, metal_type, purity,
      gross_weight, fine_weight, qty, line_amount, remarks
    )
    VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, 0, ?)
    `,
    [
      transactionId,
      transactionType,
      lotNo,
      metalType,
      purity,
      grossWeight,
      fineWeight,
      grossWeight,
      remarks
    ]
  );

  await createMetalLedgerEntry(connection, {
    companyId,
    partyId,
    transactionId,
    entryDate: voucherDate,
    metalType,
    entryType: metalEntryType,
    purity,
    grossIn: metalEntryType === "IN" ? grossWeight : 0,
    grossOut: metalEntryType === "OUT" ? grossWeight : 0,
    fineIn: metalEntryType === "IN" ? fineWeight : 0,
    fineOut: metalEntryType === "OUT" ? fineWeight : 0,
    referenceType: transactionType,
    referenceNo: voucherNo,
    lotNo,
    remarks,
    createdBy
  });

  await connection.query(
    `
    INSERT INTO lot_transaction_link
    (company_id, lot_no, process_lot_no, transaction_id, link_type, remarks, created_by)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
    [companyId, lotNo, processLotNo, transactionId, transactionType, remarks, createdBy]
  );

  await connection.query(
    `
    INSERT INTO karigar_transaction_link
    (
      company_id, karigar_id, transaction_id, lot_no, process_lot_no,
      issue_weight, receive_weight, loss_weight, labour_amount, remarks, created_by
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
    `,
    [
      companyId,
      karigarId,
      transactionId,
      lotNo,
      processLotNo,
      transactionType === "KARIGAR_ISSUE" ? grossWeight : 0,
      transactionType === "KARIGAR_RECEIVE" ? grossWeight : 0,
      transactionType === "KARIGAR_LOSS_ADJUSTMENT" ? grossWeight : 0,
      remarks,
      createdBy
    ]
  );

  await recalcPartyBalanceSummary(connection, companyId, partyId, transactionId);

  return {
    transactionId,
    voucherNo,
    transactionType,
    grossWeight,
    fineWeight,
    metalType,
    purity
  };
}

async function postKarigarWorkTransactions(connection, payload) {
  const companyId = Number(payload.companyId);
  const createdBy = payload.createdBy ?? null;
  const karigarName = normalizeKarigarName(payload.karigarName);
  const lotNo = normalizeProcessLotNo(payload.lotNo);
  const processLotNo = normalizeProcessLotNo(payload.processLotNo || lotNo);
  const issueWeight = toNumber(payload.issueWeight);
  const receiveWeight = toNumber(payload.receiveWeight);
  const lossWeight = toNumber(payload.lossWeight);
  const workId = Number(payload.workId || 0);

  if (!karigarName || !lotNo) {
    return {
      party: null,
      transactions: []
    };
  }

  const party = await findOrCreateKarigarParty(connection, {
    companyId,
    partyName: karigarName,
    createdBy
  });

  const lotContext = await getLotMetalContext(connection, {
    companyId,
    lotNo,
    party
  });

  const voucherDate = String(payload.voucherDate || getTodayDateOnly()).trim() || getTodayDateOnly();
  const remarksBase = `Auto-posted from karigar work #${workId || "new"} (${karigarName} / Lot ${lotNo})`;
  const transactions = [];

  if (issueWeight > 0) {
    const txn = await createProcessKarigarTransaction(connection, {
      companyId,
      createdBy,
      partyId: party.id,
      karigarId: party.id,
      transactionType: "KARIGAR_ISSUE",
      voucherDate,
      voucherNo: `KISS-${Date.now()}-${workId || 0}`,
      lotNo,
      processLotNo,
      metalType: lotContext.metalType,
      purity: lotContext.purity,
      grossWeight: issueWeight,
      remarks: `${remarksBase} | Issue`
    });
    if (txn) transactions.push(txn);
  }

  if (receiveWeight > 0) {
    const txn = await createProcessKarigarTransaction(connection, {
      companyId,
      createdBy,
      partyId: party.id,
      karigarId: party.id,
      transactionType: "KARIGAR_RECEIVE",
      voucherDate,
      voucherNo: `KREC-${Date.now()}-${workId || 0}`,
      lotNo,
      processLotNo,
      metalType: lotContext.metalType,
      purity: lotContext.purity,
      grossWeight: receiveWeight,
      remarks: `${remarksBase} | Receive`
    });
    if (txn) transactions.push(txn);
  }

  if (lossWeight > 0) {
    const txn = await createProcessKarigarTransaction(connection, {
      companyId,
      createdBy,
      partyId: party.id,
      karigarId: party.id,
      transactionType: "KARIGAR_LOSS_ADJUSTMENT",
      voucherDate,
      voucherNo: `KLOS-${Date.now()}-${workId || 0}`,
      lotNo,
      processLotNo,
      metalType: lotContext.metalType,
      purity: lotContext.purity,
      grossWeight: lossWeight,
      remarks: `${remarksBase} | Loss`
    });
    if (txn) transactions.push(txn);
  }

  return {
    party,
    transactions
  };
}

function getBillingItemMetalType(items) {
  if (!Array.isArray(items)) return "";

  for (const item of items) {
    const metalType = normalizeMetalType(item?.metalType || item?.metal_type);
    if (metalType) return metalType;
  }

  return "";
}

async function findExistingSaleInvoiceTransaction(connection, companyId, invoiceNo) {
  const [rows] = await connection.query(
    `
    SELECT tm.id, tm.voucher_no
    FROM invoice_transaction_link itl
    INNER JOIN transaction_master tm ON tm.id = itl.transaction_id
    WHERE itl.company_id = ?
      AND itl.invoice_no = ?
      AND tm.transaction_type = 'SALE_INVOICE'
    ORDER BY tm.id DESC
    LIMIT 1
    `,
    [companyId, invoiceNo]
  );

  return rows.length ? rows[0] : null;
}

async function postBillingToTransactionFoundation(connection, payload) {
  const companyId = Number(payload.companyId);
  const createdBy = payload.createdBy ?? null;
  const invoiceNumber = String(payload.invoiceNumber || "").trim();
  const customerName = String(payload.customerName || "").trim();
  const mobile = String(payload.mobile || "").trim();
  const gstNo = String(payload.gstNo || "").trim();
  const billDate = String(payload.billDate || getTodayDateOnly()).trim();
  const paymentMode = String(payload.paymentMode || "").trim();
  const paymentStatus = String(payload.paymentStatus || "").trim();
  const paidAmount = toNumber(payload.paidAmount);
  const dueAmount = toNumber(payload.dueAmount);
  const totalAmount = toNumber(payload.totalAmount);
  const totalWeight = toNumber(payload.totalWeight);
  const ratePerGram = toNumber(payload.ratePerGram);
  const mcRate = toNumber(payload.mcRate);
  const roundOff = toNumber(payload.roundOff);
  const subtotal = toNumber(payload.subtotal);
  const items = Array.isArray(payload.items) ? payload.items : [];
  const metalPercent = toNumber(payload.metalPercent);
  const metalPayable = toNumber(payload.metalPayable);
  const metalNote = String(payload.metalNote || "").trim();
  const metalType = normalizeMetalType(payload.metalType || getBillingItemMetalType(items));

  const existingSaleTxn = await findExistingSaleInvoiceTransaction(connection, companyId, invoiceNumber);
  if (existingSaleTxn) {
    throw new Error(`Billing transaction posting already exists for invoice ${invoiceNumber}`);
  }

  const party = await findOrCreateBillingParty(connection, {
    companyId,
    createdBy,
    partyName: customerName,
    mobile,
    gstNo
  });

  const saleVoucherNo = invoiceNumber;
  const [saleTxnInsert] = await connection.query(
    `
    INSERT INTO transaction_master
    (
      company_id, voucher_no, voucher_date, transaction_type, party_id, party_type,
      status, reference_no, invoice_no, source_module, payment_mode, payment_status,
      remarks, note, created_by
    )
    VALUES (?, ?, ?, 'SALE_INVOICE', ?, ?, 'POSTED', ?, ?, 'billing', ?, ?, ?, ?, ?)
    `,
    [
      companyId,
      saleVoucherNo,
      billDate || null,
      party.id,
      party.party_type || "CUSTOMER",
      invoiceNumber,
      invoiceNumber,
      paymentMode,
      paymentStatus,
      "Auto-posted from billing",
      `Bill total ${totalAmount.toFixed(2)} | Weight ${totalWeight.toFixed(3)}g`,
      createdBy
    ]
  );

  const saleTransactionId = saleTxnInsert.insertId;

  for (let index = 0; index < items.length; index += 1) {
    const item = items[index] || {};
    await connection.query(
      `
      INSERT INTO transaction_lines
      (
        transaction_id, line_no, item_name, barcode, lot_no, metal_type,
        purity, gross_weight, fine_weight, qty, rate_per_gram, metal_value,
        making_charge, hallmark_charge, line_amount, remarks
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        saleTransactionId,
        index + 1,
        String(item.itemName || item.productName || item.product_name || "").trim(),
        String(item.barcode || "").trim(),
        String(item.lot || item.lot_number || "").trim(),
        normalizeMetalType(item.metalType || item.metal_type || metalType),
        toNumber(item.purity),
        toNumber(item.weight),
        toNumber(item.fineWeight ?? item.fine_weight ?? item.weight),
        toNumber(item.qty || 1),
        ratePerGram,
        toNumber(item.metalValue ?? item.metal_value ?? 0),
        mcRate,
        toNumber(item.hallmarkCharge ?? item.hallmark_charge ?? 0),
        toNumber(item.totalPrice ?? item.total_price ?? 0),
        "Billing item line"
      ]
    );
  }

  await connection.query(
    `
    INSERT INTO invoice_transaction_link
    (company_id, invoice_no, transaction_id, link_type, remarks, created_by)
    VALUES (?, ?, ?, 'SALE_INVOICE', ?, ?)
    `,
    [companyId, invoiceNumber, saleTransactionId, "Billing sale posting", createdBy]
  );

  await createCashLedgerEntry(connection, {
    companyId,
    partyId: party.id,
    transactionId: saleTransactionId,
    entryDate: billDate,
    entryType: "DEBIT",
    debitAmount: totalAmount,
    creditAmount: 0,
    referenceType: "SALE_INVOICE",
    referenceNo: saleVoucherNo,
    remarks: "Billing total receivable posted",
    createdBy
  });

  let lastTransactionId = saleTransactionId;

  if (paidAmount > 0) {
    const paymentVoucherNo = `PAY-${invoiceNumber}`;
    const [paymentTxnInsert] = await connection.query(
      `
      INSERT INTO transaction_master
      (
        company_id, voucher_no, voucher_date, transaction_type, party_id, party_type,
        status, reference_no, invoice_no, source_module, payment_mode, payment_status,
        remarks, note, created_by
      )
      VALUES (?, ?, ?, 'PAYMENT_RECEIVED', ?, ?, 'POSTED', ?, ?, 'billing', ?, ?, ?, ?, ?)
      `,
      [
        companyId,
        paymentVoucherNo,
        billDate || null,
        party.id,
        party.party_type || "CUSTOMER",
        invoiceNumber,
        invoiceNumber,
        paymentMode,
        paymentStatus || "Paid",
        "Auto-posted payment from billing",
        `Cash/online receipt ${paidAmount.toFixed(2)}`,
        createdBy
      ]
    );

    const paymentTransactionId = paymentTxnInsert.insertId;

    await connection.query(
      `
      INSERT INTO transaction_settlements
      (
        company_id, transaction_id, settlement_type, against_transaction_id,
        against_invoice_no, against_voucher_no, cash_amount, settlement_date,
        remarks, created_by
      )
      VALUES (?, ?, 'CASH', ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        companyId,
        paymentTransactionId,
        saleTransactionId,
        invoiceNumber,
        saleVoucherNo,
        paidAmount,
        billDate || null,
        "Billing payment settlement",
        createdBy
      ]
    );

    await connection.query(
      `
      INSERT INTO invoice_transaction_link
      (company_id, invoice_no, transaction_id, link_type, remarks, created_by)
      VALUES (?, ?, ?, 'PAYMENT_RECEIVED', ?, ?)
      `,
      [companyId, invoiceNumber, paymentTransactionId, "Billing payment posting", createdBy]
    );

    await createCashLedgerEntry(connection, {
      companyId,
      partyId: party.id,
      transactionId: paymentTransactionId,
      entryDate: billDate,
      entryType: "CREDIT",
      debitAmount: 0,
      creditAmount: paidAmount,
      referenceType: "PAYMENT_RECEIVED",
      referenceNo: paymentVoucherNo,
      remarks: "Billing payment received posted",
      createdBy
    });

    lastTransactionId = paymentTransactionId;
  }

  const metalSettlementAmount = Math.max(totalAmount - paidAmount - dueAmount, 0);
  const shouldPostMetalSettlement =
    metalPayable > 0 &&
    (paymentMode.toUpperCase() === "METAL" || paymentStatus.toUpperCase() === "METAL SETTLED" || metalSettlementAmount > 0);

  if (shouldPostMetalSettlement) {
    const metalVoucherNo = `MET-${invoiceNumber}`;
    const effectiveRateBasis =
      metalSettlementAmount > 0
        ? metalSettlementAmount / Math.max(metalPayable, 1)
        : ratePerGram;

    const [metalTxnInsert] = await connection.query(
      `
      INSERT INTO transaction_master
      (
        company_id, voucher_no, voucher_date, transaction_type, party_id, party_type,
        status, reference_no, invoice_no, source_module, payment_mode, payment_status,
        remarks, note, created_by
      )
      VALUES (?, ?, ?, 'METAL_SETTLEMENT_RECEIVED', ?, ?, 'POSTED', ?, ?, 'billing', 'METAL', ?, ?, ?, ?)
      `,
      [
        companyId,
        metalVoucherNo,
        billDate || null,
        party.id,
        party.party_type || "CUSTOMER",
        invoiceNumber,
        invoiceNumber,
        paymentStatus || "Metal Settled",
        "Auto-posted metal settlement from billing",
        metalNote || `Metal settlement ${metalPayable.toFixed(3)}g`,
        createdBy
      ]
    );

    const metalTransactionId = metalTxnInsert.insertId;

    await connection.query(
      `
      INSERT INTO transaction_settlements
      (
        company_id, transaction_id, settlement_type, against_transaction_id,
        against_invoice_no, against_voucher_no, cash_amount, metal_type,
        gross_weight, fine_weight, purity, rate_basis, settlement_date,
        remarks, created_by
      )
      VALUES (?, ?, 'METAL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        companyId,
        metalTransactionId,
        saleTransactionId,
        invoiceNumber,
        saleVoucherNo,
        metalSettlementAmount,
        metalType,
        metalPayable,
        metalPayable,
        metalPercent,
        effectiveRateBasis,
        billDate || null,
        metalNote || "Billing metal settlement",
        createdBy
      ]
    );

    await connection.query(
      `
      INSERT INTO invoice_transaction_link
      (company_id, invoice_no, transaction_id, link_type, remarks, created_by)
      VALUES (?, ?, ?, 'METAL_SETTLEMENT_RECEIVED', ?, ?)
      `,
      [companyId, invoiceNumber, metalTransactionId, "Billing metal settlement posting", createdBy]
    );

    await createMetalLedgerEntry(connection, {
      companyId,
      partyId: party.id,
      transactionId: metalTransactionId,
      entryDate: billDate,
      metalType: metalType || "SILVER",
      entryType: "IN",
      purity: metalPercent,
      grossIn: metalPayable,
      grossOut: 0,
      fineIn: metalPayable,
      fineOut: 0,
      referenceType: "METAL_SETTLEMENT_RECEIVED",
      referenceNo: metalVoucherNo,
      remarks: metalNote || "Billing metal settlement received",
      createdBy
    });

    if (metalSettlementAmount > 0) {
      await createCashLedgerEntry(connection, {
        companyId,
        partyId: party.id,
        transactionId: metalTransactionId,
        entryDate: billDate,
        entryType: "CREDIT",
        debitAmount: 0,
        creditAmount: metalSettlementAmount,
        referenceType: "METAL_SETTLEMENT_RECEIVED",
        referenceNo: metalVoucherNo,
        remarks: "Billing metal settlement adjusted against receivable",
        createdBy
      });
    }

    lastTransactionId = metalTransactionId;
  }

  await recalcPartyBalanceSummary(connection, companyId, party.id, lastTransactionId);

  return {
    partyId: party.id,
    saleTransactionId,
    lastTransactionId
  };
}

async function findUserByEmailAndPassword(email, password) {
  const [rows] = await pool.query(
    `
    SELECT 
      u.*,
      c.company_name,
      c.owner_name AS company_owner_name,
      c.owner_email AS company_owner_email,
      c.status AS company_status
    FROM users u
    LEFT JOIN companies c ON c.id = u.company_id
    WHERE LOWER(u.email) = LOWER(?) AND u.password = ?
    LIMIT 1
    `,
    [email, password]
  );

  return rows.length ? rows[0] : null;
}

async function findUserById(userId) {
  const [rows] = await pool.query(
    `
    SELECT 
      u.*,
      c.company_name,
      c.owner_name AS company_owner_name,
      c.owner_email AS company_owner_email,
      c.status AS company_status
    FROM users u
    LEFT JOIN companies c ON c.id = u.company_id
    WHERE u.id = ?
    LIMIT 1
    `,
    [userId]
  );

  return rows.length ? rows[0] : null;
}

async function ensureSuperAdminExists() {
  try {
    const superAdminEmail = "grudrapratap0@gmail.com";
    const superAdminPassword = process.env.SUPERADMIN_PASSWORD || "@ownerofshagoon";

    const [rows] = await pool.query(
      `SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1`,
      [superAdminEmail]
    );

    if (rows.length > 0) {
      await pool.query(
        `
        UPDATE users
        SET password = ?, role = 'SuperAdmin', status = 'approved', company_id = NULL
        WHERE LOWER(email) = LOWER(?)
        `,
        [superAdminPassword, superAdminEmail]
      );
      console.log("SuperAdmin synced ✅");
      return;
    }

    await pool.query(
      `
      INSERT INTO users
      (name, mobile, email, password, role, status, company_id)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      [
        "Super Admin",
        "",
        superAdminEmail,
        superAdminPassword,
        "SuperAdmin",
        "approved",
        null
      ]
    );

    console.log("Default SuperAdmin created ✅");
  } catch (error) {
    console.error("SuperAdmin create error:", error);
  }
}

/* =========================
   BASIC ROUTES
========================= */
app.use("/css", express.static(FRONTEND_CSS_DIR));
app.use("/js/backend", (req, res) => {
  return res.status(404).json({
    success: false,
    message: "Route not found"
  });
});
app.use("/js", express.static(FRONTEND_JS_DIR));

app.get("/", (req, res) => {
  return res.sendFile(FRONTEND_INDEX_FILE);
});

app.get("/:page", (req, res, next) => {
  if (!req.params.page.endsWith(".html")) {
    return next();
  }

  const requestedFile = path.join(FRONTEND_ROOT, req.params.page);
  return res.sendFile(requestedFile, (error) => {
    if (error) {
      return next();
    }
  });
});

app.get("/api/test", (req, res) => {
  return res.status(200).send("API TEST OK");
});

app.get("/health", async (req, res) => {
  try {
    await testDbConnection();
    return res.status(200).json({
      success: true,
      app: "ok",
      db: "ok"
    });
  } catch (error) {
    console.error("Health check error:", error);
    return res.status(500).json({
      success: false,
      app: "ok",
      db: "failed",
      error: error.message
    });
  }
});

/* =========================
   DASHBOARD
========================= */
app.get("/api/dashboard", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const stockWhere = companyId !== null ? "WHERE company_id = ?" : "";
    const stockParams = companyId !== null ? [companyId] : [];

    const salesWhere = companyId !== null ? "WHERE company_id = ?" : "";
    const salesParams = companyId !== null ? [companyId] : [];

    const [stockSummary] = await pool.query(
      `
      SELECT COUNT(*) AS total_items, COALESCE(SUM(weight), 0) AS total_weight
      FROM stock
      ${stockWhere}
      `,
      stockParams
    );

    const [soldSummary] = await pool.query(
      `
      SELECT COUNT(*) AS sold_items
      FROM stock
      ${companyId !== null ? "WHERE company_id = ? AND status = 'SOLD'" : "WHERE status = 'SOLD'"}
      `,
      companyId !== null ? [companyId] : []
    );

    const [inStockSummary] = await pool.query(
      `
      SELECT COUNT(*) AS in_stock_items
      FROM stock
      ${companyId !== null ? "WHERE company_id = ? AND status = 'IN_STOCK'" : "WHERE status = 'IN_STOCK'"}
      `,
      companyId !== null ? [companyId] : []
    );

    const [salesSummary] = await pool.query(
      `
      SELECT COUNT(*) AS total_sales, COALESCE(SUM(total_amount), 0) AS total_sales_amount
      FROM sales_history
      ${salesWhere}
      `,
      salesParams
    );

    const [recentInvoices] = await pool.query(
      `
      SELECT invoice_number, customer_name, total_amount, invoice_date, created_at, company_id
      FROM sales_history
      ${salesWhere}
      ORDER BY id DESC
      LIMIT 8
      `,
      salesParams
    );

    const returnSummary = await getReturnSummaryRows(companyId);

    return res.json({
      success: true,
      totalStock: Number(stockSummary[0]?.total_items || 0),
      totalWeight: Number(stockSummary[0]?.total_weight || 0),
      soldItems: Number(soldSummary[0]?.sold_items || 0),
      availableItems: Number(inStockSummary[0]?.in_stock_items || 0),
      normalReturns: returnSummary.returnToStockCount,
      damagedReturns: returnSummary.damagedReturnCount,
      totalSales: Number(salesSummary[0]?.total_sales || 0),
      totalSalesAmount: Number(salesSummary[0]?.total_sales_amount || 0),
      recentInvoices,
      recentReturns: returnSummary.recentReturns
    });
  } catch (error) {
    console.error("Dashboard error:", error);
    return res.status(500).json({ success: false, message: "Dashboard fetch failed" });
  }
});

/* =========================
   COMPANY SETTINGS
========================= */
app.get("/settings/company", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    if (access.companyScope === null) {
      return res.json({
        success: true,
        settings: normalizeCompanySettingsRow(null)
      });
    }

    const row = await getCompanySettingsForCompany(pool, access.companyScope);

    return res.json({
      success: true,
      settings: normalizeCompanySettingsRow(row)
    });
  } catch (error) {
    console.error("Company settings fetch error:", error);
    return res.status(500).json({
      success: false,
      message: "Company settings fetch failed",
      error: error.message
    });
  }
});

app.post("/settings/company", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    const companyId = access.companyScope;
    if (companyId === null) {
      return res.status(400).json({
        success: false,
        message: "Company scope missing for settings save"
      });
    }
    const createdBy = access.actingUserId ?? getRequestedUserId(req);
    const payload = normalizeCompanySettingsRow({
      owner_email: req.body.ownerEmail,
      top_title: req.body.top_title,
      company_name: req.body.company_name,
      gstin: req.body.gstin,
      account_no: req.body.account_no,
      ifsc: req.body.ifsc,
      address: req.body.address,
      declaration: req.body.declaration,
      upi_id: req.body.upi_id,
      upi_name: req.body.upi_name,
      business_state: req.body.business_state,
      default_bill_type: req.body.default_bill_type,
      default_tax_type: req.body.default_tax_type,
      default_rate_per_gram: req.body.default_rate_per_gram,
      default_mc_rate: req.body.default_mc_rate,
      subscription_plan: req.body.subscription_plan,
      subscription_status: req.body.subscription_status,
      subscription_start_date: req.body.subscription_start_date,
      subscription_end_date: req.body.subscription_end_date
    });

    const existingRow = await getCompanySettingsForCompany(connection, companyId);

    if (existingRow) {
      await connection.query(
        `
        UPDATE company_settings
        SET owner_email = ?,
            top_title = ?,
            company_name = ?,
            gstin = ?,
            account_no = ?,
            ifsc = ?,
            address = ?,
            declaration = ?,
            upi_id = ?,
            upi_name = ?,
            business_state = ?,
            default_bill_type = ?,
            default_tax_type = ?,
            default_rate_per_gram = ?,
            default_mc_rate = ?,
            subscription_plan = ?,
            subscription_status = ?,
            subscription_start_date = ?,
            subscription_end_date = ?,
            updated_by = ?,
            updated_at = NOW()
        WHERE id = ?
        `,
        [
          payload.ownerEmail,
          payload.top_title,
          payload.company_name,
          payload.gstin,
          payload.account_no,
          payload.ifsc,
          payload.address,
          payload.declaration,
          payload.upi_id,
          payload.upi_name,
          payload.business_state,
          payload.default_bill_type,
          payload.default_tax_type,
          payload.default_rate_per_gram,
          payload.default_mc_rate,
          payload.subscription_plan,
          payload.subscription_status,
          payload.subscription_start_date || null,
          payload.subscription_end_date || null,
          createdBy,
          existingRow.id
        ]
      );
    } else {
      await connection.query(
        `
        INSERT INTO company_settings
        (
          company_id, owner_email, top_title, company_name, gstin, account_no, ifsc,
          address, declaration, upi_id, upi_name, business_state, default_bill_type,
          default_tax_type, default_rate_per_gram, default_mc_rate, subscription_plan,
          subscription_status, subscription_start_date, subscription_end_date,
          created_by, updated_by, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        `,
        [
          companyId,
          payload.ownerEmail,
          payload.top_title,
          payload.company_name,
          payload.gstin,
          payload.account_no,
          payload.ifsc,
          payload.address,
          payload.declaration,
          payload.upi_id,
          payload.upi_name,
          payload.business_state,
          payload.default_bill_type,
          payload.default_tax_type,
          payload.default_rate_per_gram,
          payload.default_mc_rate,
          payload.subscription_plan,
          payload.subscription_status,
          payload.subscription_start_date || null,
          payload.subscription_end_date || null,
          createdBy,
          createdBy
        ]
      );
    }

    const savedRow = await getCompanySettingsForCompany(connection, companyId);
    return res.json({
      success: true,
      message: "Settings saved successfully",
      settings: normalizeCompanySettingsRow(savedRow)
    });
  } catch (error) {
    console.error("Company settings save error:", error);
    return res.status(500).json({
      success: false,
      message: "Company settings save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

/* =========================
   EXPENSE MANAGER
========================= */
app.get("/expenses", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const params = [];
    let whereClause = "";

    if (companyId !== null) {
      whereClause = "WHERE e.company_id = ?";
      params.push(companyId);
    }

    const [rows] = await pool.query(
      `
      SELECT
        e.*,
        DATE_FORMAT(e.expense_date, '%Y-%m-%d') AS date,
        TIME_FORMAT(e.expense_time, '%H:%i') AS time,
        c.company_name,
        u.name AS created_by_name
      FROM expenses e
      LEFT JOIN companies c ON c.id = e.company_id
      LEFT JOIN users u ON u.id = e.created_by
      ${whereClause}
      ORDER BY e.expense_date DESC, e.expense_time DESC, e.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      expenses: rows.map((row) => normalizeExpenseRow(row))
    });
  } catch (error) {
    console.error("Expense fetch error:", error);
    return res.status(500).json({
      success: false,
      message: "Expense fetch failed",
      error: error.message
    });
  }
});

app.post("/expenses", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const person = String(req.body.person || "").trim();
    const expenseDate = String(req.body.date || req.body.expenseDate || "").trim();
    const expenseTime = String(req.body.time || req.body.expenseTime || "").trim();
    const amount = Number(req.body.amount || 0);
    const category = String(req.body.category || "").trim();
    const reason = String(req.body.reason || "").trim();
    const note = String(req.body.note || "").trim();

    if (!person || !expenseDate || amount <= 0 || !category) {
      return res.status(400).json({
        success: false,
        message: "Name, date, amount, and category are required"
      });
    }

    connection = await pool.getConnection();
    const [insertResult] = await connection.query(
      `
      INSERT INTO expenses
      (
        company_id,
        person,
        expense_date,
        expense_time,
        amount,
        category,
        reason,
        note,
        created_by,
        updated_by,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
      `,
      [
        access.companyScope,
        person,
        expenseDate,
        expenseTime || null,
        amount,
        category,
        reason,
        note,
        access.actingUserId ?? getRequestedUserId(req),
        access.actingUserId ?? getRequestedUserId(req)
      ]
    );

    const [rows] = await connection.query(
      `
      SELECT
        e.*,
        DATE_FORMAT(e.expense_date, '%Y-%m-%d') AS date,
        TIME_FORMAT(e.expense_time, '%H:%i') AS time
      FROM expenses e
      WHERE e.id = ?
      LIMIT 1
      `,
      [insertResult.insertId]
    );

    return res.json({
      success: true,
      message: "Expense saved successfully",
      expense: normalizeExpenseRow(rows[0] || {})
    });
  } catch (error) {
    console.error("Expense save error:", error);
    return res.status(500).json({
      success: false,
      message: "Expense save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.put("/expenses/:id", async (req, res) => {
  let connection;

  try {
    const expenseId = Number(req.params.id);
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    if (!expenseId) {
      return res.status(400).json({
        success: false,
        message: "Expense id missing"
      });
    }

    const person = String(req.body.person || "").trim();
    const expenseDate = String(req.body.date || req.body.expenseDate || "").trim();
    const expenseTime = String(req.body.time || req.body.expenseTime || "").trim();
    const amount = Number(req.body.amount || 0);
    const category = String(req.body.category || "").trim();
    const reason = String(req.body.reason || "").trim();
    const note = String(req.body.note || "").trim();

    if (!person || !expenseDate || amount <= 0 || !category) {
      return res.status(400).json({
        success: false,
        message: "Name, date, amount, and category are required"
      });
    }

    connection = await pool.getConnection();

    const [existingRows] = await connection.query(
      `
      SELECT id
      FROM expenses
      WHERE id = ? AND company_id = ?
      LIMIT 1
      `,
      [expenseId, access.companyScope]
    );

    if (!existingRows.length) {
      return res.status(404).json({
        success: false,
        message: "Expense not found"
      });
    }

    await connection.query(
      `
      UPDATE expenses
      SET person = ?,
          expense_date = ?,
          expense_time = ?,
          amount = ?,
          category = ?,
          reason = ?,
          note = ?,
          updated_by = ?,
          updated_at = NOW()
      WHERE id = ? AND company_id = ?
      `,
      [
        person,
        expenseDate,
        expenseTime || null,
        amount,
        category,
        reason,
        note,
        access.actingUserId ?? getRequestedUserId(req),
        expenseId,
        access.companyScope
      ]
    );

    const [rows] = await connection.query(
      `
      SELECT
        e.*,
        DATE_FORMAT(e.expense_date, '%Y-%m-%d') AS date,
        TIME_FORMAT(e.expense_time, '%H:%i') AS time
      FROM expenses e
      WHERE e.id = ?
      LIMIT 1
      `,
      [expenseId]
    );

    return res.json({
      success: true,
      message: "Expense updated successfully",
      expense: normalizeExpenseRow(rows[0] || {})
    });
  } catch (error) {
    console.error("Expense update error:", error);
    return res.status(500).json({
      success: false,
      message: "Expense update failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.delete("/expenses/:id", async (req, res) => {
  try {
    const expenseId = Number(req.params.id);
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    if (!expenseId) {
      return res.status(400).json({
        success: false,
        message: "Expense id missing"
      });
    }

    const [deleteResult] = await pool.query(
      `
      DELETE FROM expenses
      WHERE id = ? AND company_id = ?
      `,
      [expenseId, access.companyScope]
    );

    if (Number(deleteResult.affectedRows || 0) === 0) {
      return res.status(404).json({
        success: false,
        message: "Expense not found"
      });
    }

    return res.json({
      success: true,
      message: "Expense deleted successfully"
    });
  } catch (error) {
    console.error("Expense delete error:", error);
    return res.status(500).json({
      success: false,
      message: "Expense delete failed",
      error: error.message
    });
  }
});

/* =========================
   DAILY REPORT
========================= */
app.get("/getDailyReport", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const reportDate = normalizeReportDateInput(req.query.date);
    const nextDate = getNextDateString(reportDate);
    const companyParams = companyId !== null ? [companyId] : [];

    const [processRows] = await pool.query(
      `
      SELECT
        pl.id,
        pl.lot_no AS lotNo,
        pl.raw_weight AS rawWeight,
        pl.loss_weight AS lossWeight,
        pl.final_weight AS finalWeight,
        COALESCE((
          SELECT SUM(kw.issue_weight)
          FROM karigar_work kw
          WHERE kw.company_id = pl.company_id
            AND kw.lot_no = pl.lot_no
        ), 0) AS usedWeight,
        (pl.raw_weight - COALESCE((
          SELECT SUM(kw.issue_weight)
          FROM karigar_work kw
          WHERE kw.company_id = pl.company_id
            AND kw.lot_no = pl.lot_no
        ), 0)) AS balanceWeight,
        pl.saved_at AS savedAt,
        pl.company_id,
        c.company_name
      FROM process_lots pl
      LEFT JOIN companies c ON c.id = pl.company_id
      WHERE pl.saved_at >= ?
        AND pl.saved_at < ?
        ${companyId !== null ? "AND pl.company_id = ?" : ""}
      ORDER BY pl.saved_at DESC, pl.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [stockCreatedRows] = await pool.query(
      `
      SELECT
        s.id,
        s.serial,
        s.product_name,
        s.barcode,
        s.lot_number,
        s.size,
        s.weight,
        s.qty,
        s.created_at,
        s.company_id,
        c.company_name
      FROM stock s
      LEFT JOIN companies c ON c.id = s.company_id
      WHERE s.created_at >= ?
        AND s.created_at < ?
        ${companyId !== null ? "AND s.company_id = ?" : ""}
      ORDER BY s.created_at DESC, s.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [stockSoldRows] = await pool.query(
      `
      SELECT
        s.id,
        s.serial,
        s.product_name,
        s.barcode,
        s.lot_number,
        s.size,
        s.weight,
        s.qty,
        s.invoice_number,
        s.sold_at,
        s.company_id,
        c.company_name
      FROM stock s
      LEFT JOIN companies c ON c.id = s.company_id
      WHERE s.sold_at IS NOT NULL
        AND s.sold_at >= ?
        AND s.sold_at < ?
        ${companyId !== null ? "AND s.company_id = ?" : ""}
      ORDER BY s.sold_at DESC, s.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [invoiceRows] = await pool.query(
      `
      SELECT
        sh.id,
        sh.invoice_number,
        sh.customer_name,
        sh.mobile,
        sh.invoice_date,
        sh.payment_mode,
        sh.payment_status,
        sh.total_items,
        sh.total_weight,
        sh.total_amount,
        sh.paid_amount,
        sh.due_amount,
        sh.status,
        sh.created_at,
        sh.company_id,
        c.company_name
      FROM sales_history sh
      LEFT JOIN companies c ON c.id = sh.company_id
      WHERE sh.created_at >= ?
        AND sh.created_at < ?
        ${companyId !== null ? "AND sh.company_id = ?" : ""}
      ORDER BY sh.created_at DESC, sh.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [returnRows] = await pool.query(
      `
      SELECT
        rh.id,
        rh.barcode,
        rh.invoice_number,
        rh.customer_name,
        rh.product_name,
        rh.size,
        rh.weight,
        rh.return_type,
        rh.return_reason,
        rh.return_date,
        rh.company_id,
        c.company_name
      FROM return_history rh
      LEFT JOIN companies c ON c.id = rh.company_id
      LEFT JOIN users u ON u.id = rh.created_by
      WHERE rh.return_date >= ?
        AND rh.return_date < ?
        ${companyId !== null ? "AND rh.company_id = ?" : ""}
      ORDER BY rh.return_date DESC, rh.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [materialRows] = await pool.query(
      `
      SELECT
        msm.id,
        msm.movement_type,
        msm.qty,
        msm.unit,
        msm.movement_date,
        msm.supplier_name,
        msm.reference_no,
        msm.remarks,
        msi.category,
        msi.material_name,
        msi.variant,
        msi.size,
        msi.low_stock_level,
        msi.current_stock,
        msi.status,
        msm.company_id,
        c.company_name
      FROM material_stock_movements msm
      LEFT JOIN material_stock_items msi ON msi.id = msm.material_id
      LEFT JOIN companies c ON c.id = msm.company_id
      WHERE msm.movement_date >= ?
        AND msm.movement_date < ?
        ${companyId !== null ? "AND msm.company_id = ?" : ""}
      ORDER BY msm.movement_date DESC, msm.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [expenseRows] = await pool.query(
      `
      SELECT
        e.id,
        DATE_FORMAT(e.expense_date, '%Y-%m-%d') AS date,
        TIME_FORMAT(e.expense_time, '%H:%i') AS time,
        e.person,
        e.category,
        e.reason,
        e.note,
        e.amount,
        e.company_id,
        c.company_name
      FROM expenses e
      LEFT JOIN companies c ON c.id = e.company_id
      WHERE e.expense_date >= ?
        AND e.expense_date < ?
        ${companyId !== null ? "AND e.company_id = ?" : ""}
      ORDER BY e.expense_date DESC, e.expense_time DESC, e.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const [transactionRows] = await pool.query(
      `
      SELECT
        tm.id,
        COALESCE(DATE_FORMAT(tm.voucher_date, '%Y-%m-%d'), DATE_FORMAT(tm.created_at, '%Y-%m-%d')) AS date,
        COALESCE(TIME_FORMAT(tm.voucher_time, '%H:%i'), TIME_FORMAT(tm.created_at, '%H:%i')) AS time,
        COALESCE(pm.party_name, '') AS customer,
        tm.transaction_type AS type,
        COALESCE((
          SELECT tl.item_name
          FROM transaction_lines tl
          WHERE tl.transaction_id = tm.id
          ORDER BY tl.line_no ASC, tl.id ASC
          LIMIT 1
        ), '') AS itemName,
        COALESCE((
          SELECT tl.qty
          FROM transaction_lines tl
          WHERE tl.transaction_id = tm.id
          ORDER BY tl.line_no ASC, tl.id ASC
          LIMIT 1
        ), 0) AS qty,
        COALESCE((
          SELECT tl.gross_weight
          FROM transaction_lines tl
          WHERE tl.transaction_id = tm.id
          ORDER BY tl.line_no ASC, tl.id ASC
          LIMIT 1
        ), 0) AS grossWeight,
        tm.payment_mode AS mode,
        COALESCE((
          SELECT tl.line_amount
          FROM transaction_lines tl
          WHERE tl.transaction_id = tm.id
          ORDER BY tl.line_no ASC, tl.id ASC
          LIMIT 1
        ), 0) AS amount,
        COALESCE(tm.note, tm.remarks, '') AS note,
        tm.company_id,
        c.company_name
      FROM transaction_master tm
      LEFT JOIN party_master pm ON pm.id = tm.party_id
      LEFT JOIN companies c ON c.id = tm.company_id
      WHERE tm.created_at >= ?
        AND tm.created_at < ?
        AND UPPER(COALESCE(tm.status, 'POSTED')) <> 'CANCELLED'
        ${companyId !== null ? "AND tm.company_id = ?" : ""}
      ORDER BY tm.created_at DESC, tm.id DESC
      `,
      [reportDate, nextDate, ...companyParams]
    );

    const materialSummary = await getMaterialStockSummaryRows(companyId);
    const totalBillingAmount = invoiceRows.reduce((sum, row) => sum + Number(row.total_amount || 0), 0);
    const totalBills = invoiceRows.length;
    const totalReturns = returnRows.length;
    const normalReturns = returnRows.filter((row) => normalizeReturnType(row.return_type) === "RETURN_TO_STOCK").length;
    const damagedReturns = returnRows.filter((row) => normalizeReturnType(row.return_type) === "DAMAGED_RETURN").length;
    const materialIn = materialRows
      .filter((row) => normalizeMaterialMovementType(row.movement_type) === "IN")
      .reduce((sum, row) => sum + Number(row.qty || 0), 0);
    const materialOut = materialRows
      .filter((row) => normalizeMaterialMovementType(row.movement_type) === "OUT")
      .reduce((sum, row) => sum + Number(row.qty || 0), 0);
    const totalExpenses = expenseRows.reduce((sum, row) => sum + Number(row.amount || 0), 0);

    return res.json({
      success: true,
      date: reportDate,
      companyScope: companyId,
      summary: {
        totalProcessCount: processRows.length,
        totalStickersCreated: stockCreatedRows.length,
        stockAdded: stockCreatedRows.length,
        stockSold: stockSoldRows.length,
        totalReturns,
        normalReturns,
        damagedReturns,
        totalBillingAmount,
        totalBills,
        totalExpenses,
        totalTransactions: transactionRows.length,
        materialIn,
        materialOut,
        lowStockCount: materialSummary.lowStockItems
      },
      sections: {
        process: {
          rows: processRows.map((row) => ({
            ...row,
            rawWeight: toNumber(row.rawWeight),
            lossWeight: toNumber(row.lossWeight),
            finalWeight: toNumber(row.finalWeight),
            usedWeight: toNumber(row.usedWeight),
            balanceWeight: toNumber(row.balanceWeight)
          })),
          totalCount: processRows.length
        },
        sticker: {
          rows: stockCreatedRows,
          totalCreated: stockCreatedRows.length
        },
        stock: {
          addedRows: stockCreatedRows,
          soldRows: stockSoldRows,
          addedCount: stockCreatedRows.length,
          soldCount: stockSoldRows.length
        },
        material: {
          rows: materialRows,
          totalIn: materialIn,
          totalOut: materialOut,
          lowStockCount: materialSummary.lowStockItems
        },
        invoiceBilling: {
          rows: invoiceRows,
          totalBills,
          totalBillingAmount
        },
        returns: {
          rows: returnRows,
          totalReturns,
          normalReturns,
          damagedReturns
        },
        salesHistory: {
          rows: invoiceRows,
          totalSalesCount: totalBills,
          totalSalesAmount: totalBillingAmount
        },
        expenses: {
          rows: expenseRows.map((row) => normalizeExpenseRow(row)),
          totalExpenses
        },
        transactions: {
          rows: transactionRows.map((row) => ({
            ...row,
            qty: toNumber(row.qty),
            grossWeight: toNumber(row.grossWeight),
            amount: toNumber(row.amount)
          })),
          totalTransactions: transactionRows.length
        }
      }
    });
  } catch (error) {
    console.error("Get daily report error:", error);
    return res.status(500).json({
      success: false,
      message: "Daily report fetch failed",
      error: error.message
    });
  }
});

/* =========================
   GET ALL STOCK
========================= */
app.get("/getStock", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const whereClause = companyId !== null ? "WHERE company_id = ?" : "";
    const params = companyId !== null ? [companyId] : [];

    const [rows] = await pool.query(
      `
      SELECT *
      FROM stock
      ${whereClause}
      ORDER BY
        CAST(COALESCE(lot_number, '0') AS UNSIGNED) ASC,
        CAST(COALESCE(serial, '0') AS UNSIGNED) ASC,
        id ASC
      `,
      params
    );

    return res.json(rows);
  } catch (error) {
    console.error("Get stock error:", error);
    return res.status(500).json({
      success: false,
      message: "Stock fetch failed",
      error: error.message
    });
  }
});

app.get("/process/data", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const lotWhereClause = companyId !== null ? "WHERE company_id = ?" : "";
    const lotParams = companyId !== null ? [companyId] : [];
    const workWhereClause = companyId !== null ? "WHERE company_id = ?" : "";
    const workParams = companyId !== null ? [companyId] : [];

    const [lotRows] = await pool.query(
      `
      SELECT *
      FROM process_lots
      ${lotWhereClause}
      ORDER BY
        CAST(COALESCE(lot_no, '0') AS UNSIGNED) ASC,
        lot_no ASC,
        id ASC
      `,
      lotParams
    );

    const [workRows] = await pool.query(
      `
      SELECT *
      FROM karigar_work
      ${workWhereClause}
      ORDER BY id ASC
      `,
      workParams
    );

    return res.json({
      success: true,
      lots: lotRows.map(normalizeProcessLotRow),
      karigarWork: workRows.map(normalizeKarigarWorkRow)
    });
  } catch (error) {
    console.error("Get process data error:", error);
    return res.status(500).json({
      success: false,
      message: "Process data fetch failed",
      error: error.message
    });
  }
});

app.post("/process/lots", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const userId = access.actingUserId;
    const lotNo = normalizeProcessLotNo(req.body.lotNo || req.body.lot_no);
    const rawWeight = toNumber(req.body.rawWeight ?? req.body.raw_weight);
    const lossWeight = toNumber(req.body.lossWeight ?? req.body.loss_weight);
    const finalWeight = toNumber(req.body.finalWeight ?? req.body.final_weight, rawWeight - lossWeight);

    if (!lotNo) {
      return res.status(400).json({
        success: false,
        message: "lotNo is required"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [existingRows] = await connection.query(
      `
      SELECT id
      FROM process_lots
      WHERE company_id = ? AND lot_no = ?
      LIMIT 1
      `,
      [companyId, lotNo]
    );

    let processLotId = null;

    if (existingRows.length) {
      processLotId = Number(existingRows[0].id);
      await connection.query(
        `
        UPDATE process_lots
        SET raw_weight = ?,
            loss_weight = ?,
            final_weight = ?,
            saved_at = NOW(),
            created_by = ?
        WHERE id = ?
        `,
        [rawWeight, lossWeight, finalWeight, userId, processLotId]
      );
    } else {
      const [insertResult] = await connection.query(
        `
        INSERT INTO process_lots
        (company_id, lot_no, raw_weight, loss_weight, final_weight, saved_at, created_by)
        VALUES (?, ?, ?, ?, ?, NOW(), ?)
        `,
        [companyId, lotNo, rawWeight, lossWeight, finalWeight, userId]
      );
      processLotId = Number(insertResult.insertId);
    }

    const [savedRows] = await connection.query(
      `
      SELECT *
      FROM process_lots
      WHERE id = ?
      LIMIT 1
      `,
      [processLotId]
    );

    await connection.commit();

    return res.json({
      success: true,
      message: "Process lot saved successfully",
      lot: savedRows.length ? normalizeProcessLotRow(savedRows[0]) : null
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Save process lot error:", error);
    return res.status(500).json({
      success: false,
      message: "Process lot save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.post("/process/karigar-work", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const userId = access.actingUserId;
    const karigarName = normalizeKarigarName(req.body.karigarName || req.body.karigar_name || req.body.name);
    const lotNo = normalizeProcessLotNo(req.body.lotNo || req.body.lot_no || req.body.lot);
    const processLotNo = normalizeProcessLotNo(req.body.processLotNo || req.body.process_lot_no || lotNo);
    const issueWeight = toNumber(req.body.issueWeight ?? req.body.issue_weight ?? req.body.given);
    const receiveWeight = toNumber(req.body.receiveWeight ?? req.body.receive_weight ?? req.body.returned);
    const requestedLossWeight = req.body.lossWeight ?? req.body.loss_weight ?? req.body.loss;
    const lossWeight =
      requestedLossWeight === undefined || requestedLossWeight === null || requestedLossWeight === ""
        ? issueWeight - receiveWeight
        : toNumber(requestedLossWeight);
    const labourAmount = toNumber(req.body.labourAmount ?? req.body.labour_amount ?? req.body.labour);

    if (!karigarName || !lotNo) {
      return res.status(400).json({
        success: false,
        message: "Karigar name and lot number are required"
      });
    }

    if (issueWeight <= 0) {
      return res.status(400).json({
        success: false,
        message: "Issue weight must be greater than 0"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [insertResult] = await connection.query(
      `
      INSERT INTO karigar_work
      (
        company_id, karigar_name, lot_no,
        issue_weight, receive_weight, loss_weight, labour_amount,
        work_time, created_by
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), ?)
      `,
      [companyId, karigarName, lotNo, issueWeight, receiveWeight, lossWeight, labourAmount, userId]
    );

    const [savedRows] = await connection.query(
      `
      SELECT *
      FROM karigar_work
      WHERE id = ?
      LIMIT 1
      `,
      [insertResult.insertId]
    );

    const savedWork = savedRows.length ? normalizeKarigarWorkRow(savedRows[0]) : null;
    const workDate =
      String(savedWork?.work_time || "").trim().slice(0, 10) ||
      getTodayDateOnly();

    const postingResult = await postKarigarWorkTransactions(connection, {
      companyId,
      createdBy: userId,
      workId: insertResult.insertId,
      karigarName,
      lotNo,
      processLotNo,
      issueWeight,
      receiveWeight,
      lossWeight,
      voucherDate: workDate
    });

    await connection.commit();

    return res.json({
      success: true,
      message: "Karigar work saved successfully",
      work: savedWork,
      transactionPosting: {
        partyId: postingResult.party?.id ?? null,
        transactionCount: postingResult.transactions.length,
        transactions: postingResult.transactions
      }
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Save karigar work error:", error);
    return res.status(500).json({
      success: false,
      message: "Karigar work save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

/* =========================
   GET ITEM BY BARCODE
========================= */
app.get("/getSticker/:barcode", async (req, res) => {
  try {
    const barcode = String(req.params.barcode || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!barcode) {
      return res.status(400).json({
        success: false,
        message: "Barcode is required"
      });
    }

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [rows] = await pool.query(
      `
      SELECT *
      FROM stock
      WHERE barcode = ?
      AND company_id = ?
      LIMIT 1
      `,
      [barcode, companyId]
    );

    if (!rows.length) {
      return res.status(404).json({
        success: false,
        message: "Item not found"
      });
    }

    return res.json({
      success: true,
      item: rows[0]
    });
  } catch (error) {
    console.error("Get sticker error:", error);
    return res.status(500).json({
      success: false,
      message: "Fetch failed",
      error: error.message
    });
  }
});

app.get("/getReturnItem/:barcode", async (req, res) => {
  try {
    const barcode = String(req.params.barcode || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: true
    });

    if (!barcode) {
      return res.status(400).json({
        success: false,
        message: "Barcode is required"
      });
    }

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const [rows] = await pool.query(
      `
      SELECT
        s.*,
        si.invoice_number AS sale_invoice_number,
        si.customer_name AS sale_customer_name,
        si.product_name AS sale_product_name,
        si.sku AS sale_sku,
        si.size AS sale_size,
        si.weight AS sale_weight,
        si.lot_number AS sale_lot_number
      FROM stock s
      LEFT JOIN (
        SELECT si1.*
        FROM sales_items si1
        INNER JOIN (
          SELECT barcode, company_id, MAX(id) AS max_id
          FROM sales_items
          WHERE barcode = ? AND company_id = ?
          GROUP BY barcode, company_id
        ) latest
          ON latest.max_id = si1.id
      ) si
        ON si.barcode = s.barcode
       AND si.company_id = s.company_id
      WHERE s.barcode = ? AND s.company_id = ?
      LIMIT 1
      `,
      [barcode, companyId, barcode, companyId]
    );

    if (!rows.length) {
      return res.status(404).json({
        success: false,
        message: "Item not found"
      });
    }

    const item = rows[0];
    return res.json({
      success: true,
      item: {
        ...item,
        invoice_number: item.sale_invoice_number || item.invoice_number || "",
        customer_name: item.sale_customer_name || "",
        product_name: item.product_name || item.sale_product_name || "",
        sku: item.sku || item.sale_sku || "",
        size: item.size || item.sale_size || "",
        weight: item.weight || item.sale_weight || 0,
        lot_number: item.lot_number || item.sale_lot_number || ""
      }
    });
  } catch (error) {
    console.error("Get return item error:", error);
    return res.status(500).json({
      success: false,
      message: "Return item fetch failed",
      error: error.message
    });
  }
});

/* =========================
   RETURN MANAGEMENT
========================= */
app.post("/saveReturn", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const barcode = String(req.body.barcode || "").trim();
    const returnType = normalizeReturnType(req.body.return_type || req.body.returnType);
    const returnReason = String(req.body.return_reason || req.body.returnReason || "").trim();
    const finalCompanyId = access.companyScope;

    if (!barcode) {
      return res.status(400).json({
        success: false,
        message: "Barcode is required"
      });
    }

    if (!returnType) {
      return res.status(400).json({
        success: false,
        message: "A valid return_type is required"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [stockRows] = await connection.query(
      `
      SELECT *
      FROM stock
      WHERE barcode = ? AND company_id = ?
      LIMIT 1
      `,
      [barcode, finalCompanyId]
    );

    if (!stockRows.length) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        message: "Barcode was not found in this company's stock"
      });
    }

    const stockItem = stockRows[0];
    const currentStatus = String(stockItem.status || "").trim().toUpperCase();

    if (currentStatus === "DELETED") {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "You cannot save a return for a deleted stock item"
      });
    }

    if (returnType === "RETURN_TO_STOCK" && currentStatus === "IN_STOCK") {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "This item is already marked as IN_STOCK"
      });
    }

    if (returnType === "DAMAGED_RETURN" && currentStatus === "DAMAGED_RETURN") {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "This item is already marked as DAMAGED_RETURN"
      });
    }

    const saleItem = await getLatestSaleItemByBarcode(connection, barcode, finalCompanyId);
    const invoiceNumber = String(
      req.body.invoice_number ||
        req.body.invoiceNumber ||
        stockItem.invoice_number ||
        saleItem?.invoice_number ||
        ""
    ).trim();
    const customerName = String(
      req.body.customer_name ||
        req.body.customerName ||
        saleItem?.customer_name ||
        ""
    ).trim();
    const productName = String(
      req.body.product_name ||
        req.body.productName ||
        stockItem.product_name ||
        saleItem?.product_name ||
        ""
    ).trim();
    const sku = String(req.body.sku || stockItem.sku || saleItem?.sku || "").trim();
    const size = String(req.body.size || stockItem.size || saleItem?.size || "").trim();
    const weight = Number(req.body.weight || stockItem.weight || saleItem?.weight || 0);
    const lotNumber = String(
      req.body.lot_number || req.body.lotNumber || stockItem.lot_number || saleItem?.lot_number || ""
    ).trim();
    const saleStatus = String(saleItem?.sale_status || "").trim().toUpperCase();
    const saleItemStatus = String(saleItem?.item_status || "").trim().toUpperCase();

    if (!saleItem || !invoiceNumber) {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "No matching sold invoice item was found for this return"
      });
    }

    if (saleStatus === "DELETED") {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "You cannot save a return for a deleted sale item"
      });
    }

    if (saleItemStatus === "RETURN_TO_STOCK" || saleItemStatus === "DAMAGED_RETURN") {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "This sale item has already been returned"
      });
    }

    const nextStockStatus =
      returnType === "RETURN_TO_STOCK" ? "IN_STOCK" : "DAMAGED_RETURN";

    if (returnType === "RETURN_TO_STOCK") {
      await connection.query(
        `
        UPDATE stock
        SET status = 'IN_STOCK',
            invoice_number = '',
            sold_at = NULL
        WHERE barcode = ? AND company_id = ?
        `,
        [barcode, finalCompanyId]
      );
    } else {
      await connection.query(
        `
        UPDATE stock
        SET status = 'DAMAGED_RETURN'
        WHERE barcode = ? AND company_id = ?
        `,
        [barcode, finalCompanyId]
      );
    }

    const transactionPosting = await postReturnToTransactionFoundation(connection, {
      companyId: finalCompanyId,
      createdBy: access.actingUserId,
      saleItem,
      saleRow: saleItem,
      invoiceNumber,
      customerName,
      mobile: saleItem?.sale_mobile || "",
      gstNo: saleItem?.sale_gst_number || "",
      returnType,
      returnReason,
      returnDate: getTodayDateOnly(),
      productName,
      barcode,
      lotNumber,
      weight
    });

    const [returnInsert] = await connection.query(
      `
      INSERT INTO return_history
      (
        barcode,
        invoice_number,
        customer_name,
        product_name,
        sku,
        size,
        weight,
        lot_number,
        return_type,
        return_reason,
        return_date,
        company_id,
        party_id,
        estimated_amount,
        transaction_id,
        created_by,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?, ?, NOW())
      `,
      [
        barcode,
        invoiceNumber,
        customerName,
        productName,
        sku,
        size,
        Number.isNaN(weight) ? 0 : weight,
        lotNumber,
        returnType,
        returnReason,
        finalCompanyId,
        transactionPosting.partyId,
        transactionPosting.estimatedAmount,
        transactionPosting.transactionId,
        access.actingUserId
      ]
    );

    await connection.query(
      `
      UPDATE sales_items
      SET item_status = ?,
          return_type = ?,
          returned_at = NOW(),
          return_id = ?,
          return_transaction_id = ?
      WHERE id = ?
      `,
      [
        returnType,
        returnType,
        returnInsert.insertId,
        transactionPosting.transactionId,
        saleItem.id
      ]
    );

    await connection.commit();

    return res.json({
      success: true,
      message: "Return saved successfully",
      item: {
        barcode,
        invoice_number: invoiceNumber,
        customer_name: customerName,
        product_name: productName,
        sku,
        size,
        weight: Number.isNaN(weight) ? 0 : weight,
        lot_number: lotNumber,
        return_type: returnType,
        return_reason: returnReason,
        company_id: finalCompanyId,
        stock_status: nextStockStatus,
        transaction_id: transactionPosting.transactionId,
        estimated_amount: transactionPosting.estimatedAmount
      }
    });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_) {}
    }

    console.error("Save return error:", error);
    return res.status(500).json({
      success: false,
      message: "Return save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.get("/getReturns", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const whereClause = companyId !== null ? "WHERE rh.company_id = ?" : "";
    const params = companyId !== null ? [companyId] : [];

    const [rows] = await pool.query(
      `
      SELECT
        rh.*,
        c.company_name,
        u.name AS created_by_name
      FROM return_history rh
      LEFT JOIN companies c ON c.id = rh.company_id
      LEFT JOIN users u ON u.id = rh.created_by
      ${whereClause}
      ORDER BY rh.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      returns: rows
    });
  } catch (error) {
    console.error("Get returns error:", error);
    return res.status(500).json({
      success: false,
      message: "Returns fetch failed",
      error: error.message
    });
  }
});

app.get("/getReturnSummary", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const summary = await getReturnSummaryRows(companyId);

    return res.json({
      success: true,
      ...summary
    });
  } catch (error) {
    console.error("Get return summary error:", error);
    return res.status(500).json({
      success: false,
      message: "Return summary fetch failed",
      error: error.message
    });
  }
});

/* =========================
   MATERIAL STOCK
========================= */
app.post("/materialStock/items", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const category = String(req.body.category || "").trim();
    const materialName = String(req.body.material_name || req.body.materialName || "").trim();
    const variant = String(req.body.variant || "").trim();
    const size = String(req.body.size || "").trim();
    const unit = String(req.body.unit || "").trim();
    const openingStock = Number(req.body.opening_stock ?? req.body.openingStock ?? 0);
    const lowStockLevel = Number(req.body.low_stock_level ?? req.body.lowStockLevel ?? 0);
    const supplierName = String(req.body.supplier_name || req.body.supplierName || "").trim();
    const remarks = String(req.body.remarks || "").trim();
    const finalCompanyId = access.companyScope;
    const finalUserId = access.actingUserId ?? getRequestedUserId(req);

    if (!category || !materialName || !unit) {
      return res.status(400).json({
        success: false,
        message: "Category, material name, and unit are required"
      });
    }

    if (Number.isNaN(openingStock) || Number.isNaN(lowStockLevel)) {
      return res.status(400).json({
        success: false,
        message: "Opening stock and low stock level must be valid numbers"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [duplicateRows] = await connection.query(
      `
      SELECT id
      FROM material_stock_items
      WHERE company_id = ?
        AND LOWER(TRIM(category)) = LOWER(TRIM(?))
        AND LOWER(TRIM(material_name)) = LOWER(TRIM(?))
        AND LOWER(TRIM(COALESCE(variant, ''))) = LOWER(TRIM(?))
        AND LOWER(TRIM(COALESCE(size, ''))) = LOWER(TRIM(?))
        AND LOWER(TRIM(unit)) = LOWER(TRIM(?))
      LIMIT 1
      `,
      [finalCompanyId, category, materialName, variant, size, unit]
    );

    if (duplicateRows.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "This material variant/size/unit already exists in the same company"
      });
    }

    const status = getMaterialStockStatus(openingStock, lowStockLevel);

    const [insertResult] = await connection.query(
      `
      INSERT INTO material_stock_items (
        company_id, category, material_name, variant, size, unit,
        opening_stock, current_stock, low_stock_level, supplier_name, remarks, status, created_by
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        finalCompanyId,
        category,
        materialName,
        variant,
        size,
        unit,
        openingStock,
        openingStock,
        lowStockLevel,
        supplierName,
        remarks,
        status,
        finalUserId
      ]
    );

    await connection.query(
      `
      INSERT INTO material_stock_movements (
        company_id, material_id, movement_type, qty, unit, movement_date,
        supplier_name, remarks, reference_no, created_by
      ) VALUES (?, ?, 'OPENING', ?, ?, NOW(), ?, ?, 'OPENING', ?)
      `,
      [finalCompanyId, insertResult.insertId, openingStock, unit, supplierName, remarks, finalUserId]
    );

    await syncMaterialStockBalance(connection, insertResult.insertId, finalCompanyId);
    await connection.commit();

    return res.json({
      success: true,
      message: "Material item saved successfully",
      materialId: insertResult.insertId
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Create material item error:", error);
    return res.status(500).json({
      success: false,
      message: "Material item save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.put("/materialStock/items/:id", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const materialId = Number(req.params.id);
    const category = String(req.body.category || "").trim();
    const materialName = String(req.body.material_name || req.body.materialName || "").trim();
    const variant = String(req.body.variant || "").trim();
    const size = String(req.body.size || "").trim();
    const unit = String(req.body.unit || "").trim();
    const openingStock = Number(req.body.opening_stock ?? req.body.openingStock ?? 0);
    const lowStockLevel = Number(req.body.low_stock_level ?? req.body.lowStockLevel ?? 0);
    const supplierName = String(req.body.supplier_name || req.body.supplierName || "").trim();
    const remarks = String(req.body.remarks || "").trim();
    const finalCompanyId = access.companyScope;

    if (!materialId) {
      return res.status(400).json({
        success: false,
        message: "Material id is required"
      });
    }

    if (!category || !materialName || !unit) {
      return res.status(400).json({
        success: false,
        message: "Category, material name, and unit are required"
      });
    }

    if (Number.isNaN(openingStock) || Number.isNaN(lowStockLevel)) {
      return res.status(400).json({
        success: false,
        message: "Opening stock and low stock level must be valid numbers"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [existingRows] = await connection.query(
      `
      SELECT id
      FROM material_stock_items
      WHERE id = ? AND company_id = ?
      LIMIT 1
      `,
      [materialId, finalCompanyId]
    );

    if (!existingRows.length) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        message: "Material item not found"
      });
    }

    const [duplicateRows] = await connection.query(
      `
      SELECT id
      FROM material_stock_items
      WHERE company_id = ?
        AND id <> ?
        AND LOWER(TRIM(category)) = LOWER(TRIM(?))
        AND LOWER(TRIM(material_name)) = LOWER(TRIM(?))
        AND LOWER(TRIM(COALESCE(variant, ''))) = LOWER(TRIM(?))
        AND LOWER(TRIM(COALESCE(size, ''))) = LOWER(TRIM(?))
        AND LOWER(TRIM(unit)) = LOWER(TRIM(?))
      LIMIT 1
      `,
      [finalCompanyId, materialId, category, materialName, variant, size, unit]
    );

    if (duplicateRows.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "This material identity already exists in the same company"
      });
    }

    await connection.query(
      `
      UPDATE material_stock_items
      SET category = ?,
          material_name = ?,
          variant = ?,
          size = ?,
          unit = ?,
          low_stock_level = ?,
          supplier_name = ?,
          remarks = ?,
          updated_at = NOW()
      WHERE id = ? AND company_id = ?
      `,
      [category, materialName, variant, size, unit, lowStockLevel, supplierName, remarks, materialId, finalCompanyId]
    );

    const [openingRows] = await connection.query(
      `
      SELECT id
      FROM material_stock_movements
      WHERE material_id = ? AND company_id = ? AND movement_type = 'OPENING'
      ORDER BY id ASC
      LIMIT 1
      `,
      [materialId, finalCompanyId]
    );

    if (openingRows.length > 0) {
      await connection.query(
        `
        UPDATE material_stock_movements
        SET qty = ?,
            unit = ?,
            supplier_name = ?,
            remarks = ?
        WHERE id = ?
        `,
        [openingStock, unit, supplierName, remarks, openingRows[0].id]
      );
    } else {
      await connection.query(
        `
        INSERT INTO material_stock_movements (
          company_id, material_id, movement_type, qty, unit, movement_date,
          supplier_name, remarks, reference_no, created_by
        ) VALUES (?, ?, 'OPENING', ?, ?, NOW(), ?, ?, 'OPENING', ?)
        `,
        [finalCompanyId, materialId, openingStock, unit, supplierName, remarks, access.actingUserId]
      );
    }

    await syncMaterialStockBalance(connection, materialId, finalCompanyId);
    await connection.commit();

    return res.json({
      success: true,
      message: "Material item updated successfully"
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Update material item error:", error);
    return res.status(500).json({
      success: false,
      message: "Material item update failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.get("/materialStock/items", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const whereClause = companyId !== null ? "WHERE msi.company_id = ?" : "";
    const params = companyId !== null ? [companyId] : [];

    const [rows] = await pool.query(
      `
      SELECT
        msi.*,
        COALESCE(SUM(CASE WHEN msm.movement_type = 'OPENING' THEN msm.qty ELSE 0 END), 0) AS opening_total,
        COALESCE(SUM(CASE WHEN msm.movement_type = 'IN' THEN msm.qty ELSE 0 END), 0) AS total_in,
        COALESCE(SUM(CASE WHEN msm.movement_type = 'OUT' THEN msm.qty ELSE 0 END), 0) AS total_out,
        COALESCE(SUM(CASE WHEN msm.movement_type = 'ADJUSTMENT' THEN msm.qty ELSE 0 END), 0) AS total_adjustment,
        MAX(msm.movement_date) AS last_movement_date,
        c.company_name,
        u.name AS created_by_name
      FROM material_stock_items msi
      LEFT JOIN material_stock_movements msm ON msm.material_id = msi.id
      LEFT JOIN companies c ON c.id = msi.company_id
      LEFT JOIN users u ON u.id = msi.created_by
      ${whereClause}
      GROUP BY msi.id
      ORDER BY msi.category ASC, msi.material_name ASC, msi.variant ASC, msi.size ASC, msi.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      items: rows.map((row) => ({
        ...row,
        opening_total: Number(row.opening_total || 0),
        total_in: Number(row.total_in || 0),
        total_out: Number(row.total_out || 0),
        total_adjustment: Number(row.total_adjustment || 0),
        opening_stock: Number(row.opening_stock || 0),
        current_stock: Number(row.current_stock || 0),
        low_stock_level: Number(row.low_stock_level || 0)
      }))
    });
  } catch (error) {
    console.error("Get material items error:", error);
    return res.status(500).json({
      success: false,
      message: "Material items fetch failed",
      error: error.message
    });
  }
});

app.post("/materialStock/movements", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const materialId = Number(req.body.material_id ?? req.body.materialId ?? 0);
    const movementType = normalizeMaterialMovementType(req.body.movement_type || req.body.movementType);
    const qty = Number(req.body.qty ?? req.body.quantity ?? 0);
    const movementDateRaw = String(req.body.movement_date || req.body.movementDate || "").trim();
    const supplierName = String(req.body.supplier_name || req.body.supplierName || "").trim();
    const referenceNo = String(req.body.reference_no || req.body.referenceNo || "").trim();
    const remarks = String(req.body.remarks || "").trim();
    const finalCompanyId = access.companyScope;
    const finalUserId = access.actingUserId ?? getRequestedUserId(req);

    if (!materialId) {
      return res.status(400).json({
        success: false,
        message: "Please select a material"
      });
    }

    if (!movementType || movementType === "OPENING") {
      return res.status(400).json({
        success: false,
        message: "A valid movement type is required"
      });
    }

    if (Number.isNaN(qty) || qty === 0) {
      return res.status(400).json({
        success: false,
        message: "Quantity must be valid"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [materialRows] = await connection.query(
      `
      SELECT *
      FROM material_stock_items
      WHERE id = ? AND company_id = ?
      LIMIT 1
      `,
      [materialId, finalCompanyId]
    );

    if (!materialRows.length) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        message: "Material item not found"
      });
    }

    const material = materialRows[0];
    const safeQty = movementType === "ADJUSTMENT" ? qty : Math.abs(qty);
    const projectedStock =
      Number(material.current_stock || 0) +
      (movementType === "IN" ? safeQty : 0) +
      (movementType === "OUT" ? -safeQty : 0) +
      (movementType === "ADJUSTMENT" ? safeQty : 0);

    if (projectedStock < 0) {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "Current stock cannot go below zero"
      });
    }

    const movementDate = movementDateRaw ? movementDateRaw : new Date().toISOString().slice(0, 10);

    await connection.query(
      `
      INSERT INTO material_stock_movements (
        company_id, material_id, movement_type, qty, unit, movement_date,
        supplier_name, remarks, reference_no, created_by
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        finalCompanyId,
        materialId,
        movementType,
        safeQty,
        String(material.unit || "").trim(),
        movementDate,
        supplierName || String(material.supplier_name || "").trim(),
        remarks,
        referenceNo,
        finalUserId
      ]
    );

    await syncMaterialStockBalance(connection, materialId, finalCompanyId);
    await connection.commit();

    return res.json({
      success: true,
      message: "Material movement saved successfully"
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Create material movement error:", error);
    return res.status(500).json({
      success: false,
      message: "Material movement save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.get("/materialStock/movements", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const whereClause = companyId !== null ? "WHERE msm.company_id = ?" : "";
    const params = companyId !== null ? [companyId] : [];

    const [rows] = await pool.query(
      `
      SELECT
        msm.*,
        msi.category,
        msi.material_name,
        msi.variant,
        msi.size,
        msi.low_stock_level,
        c.company_name,
        u.name AS created_by_name
      FROM material_stock_movements msm
      LEFT JOIN material_stock_items msi ON msi.id = msm.material_id
      LEFT JOIN companies c ON c.id = msm.company_id
      LEFT JOIN users u ON u.id = msm.created_by
      ${whereClause}
      ORDER BY msm.movement_date DESC, msm.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      movements: rows.map((row) => ({
        ...row,
        qty: Number(row.qty || 0),
        low_stock_level: Number(row.low_stock_level || 0)
      }))
    });
  } catch (error) {
    console.error("Get material movements error:", error);
    return res.status(500).json({
      success: false,
      message: "Material movements fetch failed",
      error: error.message
    });
  }
});

app.get("/materialStock/summary", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const summary = await getMaterialStockSummaryRows(companyId);

    return res.json({
      success: true,
      ...summary
    });
  } catch (error) {
    console.error("Get material stock summary error:", error);
    return res.status(500).json({
      success: false,
      message: "Material stock summary fetch failed",
      error: error.message
    });
  }
});

/* =========================
   ADD STICKER
========================= */
app.post("/addSticker", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const {
      serial,
      productName,
      purity,
      sku,
      mm,
      size,
      weight,
      lot,
      metalType,
      processType,
      barcode,
      companyId
    } = req.body;

    const finalCompanyId = access.companyScope;
    const finalUserId = access.actingUserId ?? getRequestedUserId(req);

    if (!serial || !productName || !purity || !sku || !size || !weight || !lot || !barcode) {
      return res.json({
        success: false,
        message: "Serial, product, purity, SKU, size, weight, lot, and barcode are required"
      });
    }

    const cleanLot = String(lot).trim();
    const cleanSerial = String(serial).trim();
    const cleanBarcode = String(barcode).trim();

    const [dupLotSerial] = await pool.query(
      `
      SELECT id FROM stock
      WHERE lot_number = ?
        AND serial = ?
        AND company_id = ?
        AND UPPER(COALESCE(status, 'IN_STOCK')) = 'IN_STOCK'
      LIMIT 1
      `,
      [cleanLot, cleanSerial, finalCompanyId]
    );

    if (dupLotSerial.length > 0) {
      return res.json({
        success: false,
        message: `Serial ${cleanSerial} already exists in lot ${cleanLot}`
      });
    }

    const [dupBarcode] = await pool.query(
      `
      SELECT id FROM stock
      WHERE barcode = ?
        AND company_id = ?
        AND UPPER(COALESCE(status, 'IN_STOCK')) = 'IN_STOCK'
      LIMIT 1
      `,
      [cleanBarcode, finalCompanyId]
    );

    if (dupBarcode.length > 0) {
      return res.json({
        success: false,
        message: `Barcode ${cleanBarcode} already exists`
      });
    }

    await pool.query(
      `
      INSERT INTO stock (
        serial,
        product_name,
        purity,
        sku,
        mm,
        size,
        weight,
        lot_number,
        barcode,
        metal_type,
        process_type,
        status,
        company_id,
        created_by,
        deleted_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        cleanSerial,
        String(productName).trim(),
        String(purity).trim(),
        String(sku).trim(),
        String(mm || "").trim(),
        String(size).trim(),
        Number(format3(weight)),
        cleanLot,
        cleanBarcode,
        String(metalType || "").trim(),
        String(processType || "").trim(),
        "IN_STOCK",
        finalCompanyId,
        finalUserId,
        null
      ]
    );

    return res.json({
      success: true,
      message: "Sticker added successfully"
    });
  } catch (err) {
    console.error("Add sticker error:", err);
    return res.status(500).json({
      success: false,
      message: "Add sticker failed",
      error: err.message
    });
  }
});

/* =========================
   UPDATE STICKER
========================= */
app.put("/updateSticker/:barcode", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const oldBarcode = String(req.params.barcode || "").trim();

    const {
      serial = "",
      productName = "",
      purity = "",
      sku = "",
      mm = "",
      size = "",
      weight = 0,
      lot = "",
      barcode = oldBarcode,
      metalType = "",
      processType = "",
      qty = 1,
      status = "IN_STOCK",
      companyId = null,
      invoiceNumber = ""
    } = req.body;

    const finalCompanyId = access.companyScope;

    if (!oldBarcode) {
      return res.json({ success: false, message: "Old barcode is missing" });
    }

    if (String(status).toUpperCase() === "SOLD") {
      const soldParams = ["SOLD", String(invoiceNumber || "").trim(), oldBarcode, finalCompanyId];
      const soldSql = `
        UPDATE stock
        SET status = ?, invoice_number = ?, sold_at = NOW(), deleted_at = NULL
        WHERE barcode = ?
          AND company_id = ?
      `;

      const [soldResult] = await pool.query(soldSql, soldParams);

      if (Number(soldResult.affectedRows || 0) === 0) {
        return res.json({ success: false, message: "Sticker item not found" });
      }

      return res.json({
        success: true,
        message: "Sticker updated successfully"
      });
    }

    if (!serial || !productName || !purity || !sku || !size || !weight || !lot) {
      return res.json({
        success: false,
        message: "Serial, product, purity, SKU, size, weight, and lot are required"
      });
    }

    const cleanLot = String(lot).trim();
    const cleanSerial = String(serial).trim();
    const newBarcode = String(barcode || oldBarcode).trim();

    const [currentRows] = await pool.query(
      `
      SELECT id
      FROM stock
      WHERE barcode = ? AND company_id = ?
      LIMIT 1
      `,
      [oldBarcode, finalCompanyId]
    );

    if (currentRows.length === 0) {
      return res.json({ success: false, message: "Sticker item not found" });
    }

    const currentId = currentRows[0].id;

    const [dupLotSerial] = await pool.query(
      `
      SELECT id FROM stock
      WHERE lot_number = ?
        AND serial = ?
        AND company_id = ?
        AND id <> ?
        AND UPPER(COALESCE(status, 'IN_STOCK')) = 'IN_STOCK'
      LIMIT 1
      `,
      [cleanLot, cleanSerial, finalCompanyId, currentId]
    );

    if (dupLotSerial.length > 0) {
      return res.json({
        success: false,
        message: `Serial ${cleanSerial} already exists in lot ${cleanLot}`
      });
    }

    const [dupBarcode] = await pool.query(
      `
      SELECT id FROM stock
      WHERE barcode = ?
        AND company_id = ?
        AND id <> ?
        AND UPPER(COALESCE(status, 'IN_STOCK')) = 'IN_STOCK'
      LIMIT 1
      `,
      [newBarcode, finalCompanyId, currentId]
    );

    if (dupBarcode.length > 0) {
      return res.json({
        success: false,
        message: `Barcode ${newBarcode} already exists`
      });
    }

    await pool.query(
      `
      UPDATE stock
      SET
        serial = ?,
        product_name = ?,
        purity = ?,
        sku = ?,
        mm = ?,
        size = ?,
        weight = ?,
        qty = ?,
        lot_number = ?,
        barcode = ?,
        metal_type = ?,
        process_type = ?,
        status = ?,
        deleted_at = CASE WHEN ? = 'DELETED' THEN NOW() ELSE NULL END
      WHERE id = ?
      `,
      [
        cleanSerial,
        String(productName).trim(),
        String(purity).trim(),
        String(sku).trim(),
        String(mm || "").trim(),
        String(size).trim(),
        Number(format3(weight)),
        Number(qty || 1),
        cleanLot,
        newBarcode,
        String(metalType || "").trim(),
        String(processType || "").trim(),
        String(status || "IN_STOCK").trim(),
        String(status || "IN_STOCK").trim(),
        currentId
      ]
    );

    return res.json({
      success: true,
      message: "Sticker updated successfully"
    });
  } catch (error) {
    console.error("Update sticker error:", error);
    return res.status(500).json({
      success: false,
      message: "Server error",
      error: error.message
    });
  }
});
/* =========================
   DELETE STICKER (SOFT DELETE)
========================= */
app.delete("/deleteSticker/:barcode", async (req, res) => {
  try {
    const barcode = String(req.params.barcode || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!barcode) {
      return res.json({
        success: false,
        message: "Barcode is required"
      });
    }

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const query = `
      UPDATE stock
      SET status = 'DELETED', deleted_at = NOW()
      WHERE barcode = ?
        AND company_id = ?
    `;
    const params = [barcode, companyId];

    const [result] = await pool.query(query, params);

    if (Number(result.affectedRows || 0) === 0) {
      return res.json({
        success: false,
        message: "Sticker item not found"
      });
    }

    return res.json({
      success: true,
      message: "Sticker deleted successfully"
    });
  } catch (error) {
    console.error("Delete sticker error:", error);
    return res.status(500).json({
      success: false,
      message: "Server error",
      error: error.message
    });
  }
});

/* =========================
   RESTORE STICKER
========================= */
app.put("/restoreSticker/:barcode", async (req, res) => {
  try {
    const barcode = String(req.params.barcode || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!barcode) {
      return res.json({
        success: false,
        message: "Barcode is required"
      });
    }

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const query = `
      UPDATE stock
      SET status = 'IN_STOCK', deleted_at = NULL
      WHERE barcode = ?
        AND company_id = ?
    `;
    const params = [barcode, companyId];

    const [result] = await pool.query(query, params);

    if (Number(result.affectedRows || 0) === 0) {
      return res.json({
        success: false,
        message: "No item was found to restore"
      });
    }

    return res.json({
      success: true,
      message: "Sticker restored successfully"
    });
  } catch (err) {
    console.error("Restore error:", err);
    return res.status(500).json({
      success: false,
      message: "Restore failed",
      error: err.message
    });
  }
});

/* =========================
   INVOICE DRAFTS
========================= */
app.get("/invoice-drafts/current", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    const draftRow = await getCurrentInvoiceDraft(
      connection,
      access.companyScope,
      access.actingUserId ?? getRequestedUserId(req)
    );

    const payload = draftRow
      ? await getInvoiceDraftPayload(connection, draftRow.id)
      : mapInvoiceDraftPayload(null, []);

    return res.json({
      success: true,
      ...payload
    });
  } catch (error) {
    console.error("Current invoice draft fetch error:", error);
    return res.status(500).json({
      success: false,
      message: "Current invoice draft fetch failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.post("/invoice-drafts/current/header", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    const actingUserId = access.actingUserId ?? getRequestedUserId(req);
    const draftRow = await getOrCreateCurrentInvoiceDraft(connection, access.companyScope, actingUserId);

    await connection.query(
      `
      UPDATE invoice_drafts
      SET customer_name = ?,
          mobile = ?,
          invoice_number = ?,
          invoice_date = ?,
          updated_by = ?,
          updated_at = NOW()
      WHERE id = ? AND company_id = ?
      `,
      [
        String(req.body.customerName || "").trim(),
        String(req.body.mobile || "").trim(),
        String(req.body.invoiceNumber || "").trim(),
        String(req.body.invoiceDate || "").trim() || null,
        actingUserId,
        draftRow.id,
        access.companyScope
      ]
    );

    const payload = await getInvoiceDraftPayload(connection, draftRow.id);
    return res.json({
      success: true,
      message: "Invoice draft header saved",
      ...payload
    });
  } catch (error) {
    console.error("Invoice draft header save error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice draft header save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.post("/invoice-drafts/current/items", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const barcode = String(req.body.barcode || "").trim();
    if (!barcode) {
      return res.status(400).json({
        success: false,
        message: "Barcode missing"
      });
    }

    connection = await pool.getConnection();
    const actingUserId = access.actingUserId ?? getRequestedUserId(req);
    const draftRow = await getOrCreateCurrentInvoiceDraft(connection, access.companyScope, actingUserId);

    const [duplicateRows] = await connection.query(
      `
      SELECT id
      FROM invoice_draft_items
      WHERE draft_id = ? AND barcode = ?
      LIMIT 1
      `,
      [draftRow.id, barcode]
    );

    if (duplicateRows.length > 0) {
      const payload = await getInvoiceDraftPayload(connection, draftRow.id);
      return res.json({
        success: true,
        message: "Barcode already present in draft",
        ...payload
      });
    }

    const [stockRows] = await connection.query(
      `
      SELECT *
      FROM stock
      WHERE barcode = ? AND company_id = ?
      LIMIT 1
      `,
      [barcode, access.companyScope]
    );

    if (!stockRows.length) {
      return res.status(404).json({
        success: false,
        message: "The barcode was not found in stock"
      });
    }

    const stockRow = stockRows[0];
    const stockStatus = String(stockRow.status || "IN_STOCK").trim().toUpperCase();
    if (stockStatus !== "IN_STOCK") {
      return res.status(400).json({
        success: false,
        message: "This barcode is not in sellable stock"
      });
    }

    await connection.query(
      `
      INSERT INTO invoice_draft_items
      (
        draft_id,
        company_id,
        barcode,
        product_name,
        sku,
        purity,
        size,
        weight,
        lot_number,
        item_stage,
        created_at,
        updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', NOW(), NOW())
      `,
      [
        draftRow.id,
        access.companyScope,
        barcode,
        String(stockRow.product_name || "").trim(),
        String(stockRow.sku || "").trim(),
        String(stockRow.purity || "").trim(),
        String(stockRow.size || "").trim(),
        Number(stockRow.weight || 0),
        String(stockRow.lot_number || "").trim()
      ]
    );

    const payload = await getInvoiceDraftPayload(connection, draftRow.id);
    return res.json({
      success: true,
      message: "Invoice draft item added",
      ...payload
    });
  } catch (error) {
    console.error("Invoice draft item add error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice draft item add failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.delete("/invoice-drafts/current/items/:barcode", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const barcode = String(req.params.barcode || "").trim();
    connection = await pool.getConnection();
    const draftRow = await getCurrentInvoiceDraft(
      connection,
      access.companyScope,
      access.actingUserId ?? getRequestedUserId(req)
    );

    if (!draftRow) {
      return res.json({
        success: true,
        message: "Invoice draft already empty",
        ...mapInvoiceDraftPayload(null, [])
      });
    }

    await connection.query(
      `
      DELETE FROM invoice_draft_items
      WHERE draft_id = ? AND barcode = ?
      `,
      [draftRow.id, barcode]
    );

    const payload = await getInvoiceDraftPayload(connection, draftRow.id);
    return res.json({
      success: true,
      message: "Invoice draft item removed",
      ...payload
    });
  } catch (error) {
    console.error("Invoice draft item delete error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice draft item delete failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.post("/invoice-drafts/current/apply-details", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const customerName = String(req.body.customerName || "").trim();
    const mobile = String(req.body.mobile || "").trim();
    const invoiceNumber = String(req.body.invoiceNumber || "").trim();
    const invoiceDate = String(req.body.invoiceDate || "").trim();

    if (!customerName) {
      return res.status(400).json({
        success: false,
        message: "Please enter the customer name"
      });
    }

    if (!invoiceNumber) {
      return res.status(400).json({
        success: false,
        message: "Please enter the invoice number"
      });
    }

    connection = await pool.getConnection();
    const actingUserId = access.actingUserId ?? getRequestedUserId(req);
    const draftRow = await getCurrentInvoiceDraft(connection, access.companyScope, actingUserId);

    if (!draftRow) {
      return res.status(400).json({
        success: false,
        message: "Please scan a barcode first"
      });
    }

    const [pendingRows] = await connection.query(
      `
      SELECT id
      FROM invoice_draft_items
      WHERE draft_id = ?
        AND UPPER(COALESCE(item_stage, 'PENDING')) = 'PENDING'
      LIMIT 1
      `,
      [draftRow.id]
    );

    if (!pendingRows.length) {
      return res.status(400).json({
        success: false,
        message: "Please scan a barcode first"
      });
    }

    await connection.query(
      `
      UPDATE invoice_drafts
      SET customer_name = ?,
          mobile = ?,
          invoice_number = ?,
          invoice_date = ?,
          updated_by = ?,
          updated_at = NOW()
      WHERE id = ? AND company_id = ?
      `,
      [
        customerName,
        mobile,
        invoiceNumber,
        invoiceDate || null,
        actingUserId,
        draftRow.id,
        access.companyScope
      ]
    );

    await connection.query(
      `
      UPDATE invoice_draft_items
      SET item_stage = 'READY',
          updated_at = NOW()
      WHERE draft_id = ?
        AND UPPER(COALESCE(item_stage, 'PENDING')) = 'PENDING'
      `,
      [draftRow.id]
    );

    const payload = await getInvoiceDraftPayload(connection, draftRow.id);
    return res.json({
      success: true,
      message: "The barcode has been updated with customer details",
      ...payload
    });
  } catch (error) {
    console.error("Invoice draft apply details error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice draft update failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.delete("/invoice-drafts/current", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    const actingUserId = access.actingUserId ?? getRequestedUserId(req);
    const draftRow = await getCurrentInvoiceDraft(connection, access.companyScope, actingUserId);

    if (!draftRow) {
      return res.json({
        success: true,
        message: "Invoice draft cleared",
        ...mapInvoiceDraftPayload(null, [])
      });
    }

    await connection.query(
      `
      DELETE FROM invoice_draft_items
      WHERE draft_id = ?
      `,
      [draftRow.id]
    );

    await connection.query(
      `
      UPDATE invoice_drafts
      SET customer_name = '',
          mobile = '',
          invoice_number = '',
          invoice_date = NULL,
          updated_by = ?,
          updated_at = NOW()
      WHERE id = ? AND company_id = ?
      `,
      [actingUserId, draftRow.id, access.companyScope]
    );

    const payload = await getInvoiceDraftPayload(connection, draftRow.id);
    return res.json({
      success: true,
      message: "Invoice draft cleared",
      ...payload
    });
  } catch (error) {
    console.error("Invoice draft clear error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice draft clear failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.get("/invoice-drafts/:id/billing", async (req, res) => {
  let connection;

  try {
    const draftId = Number(req.params.id);
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    if (!draftId) {
      return res.status(400).json({
        success: false,
        message: "Draft id missing"
      });
    }

    connection = await pool.getConnection();
    const [draftRows] = await connection.query(
      `
      SELECT *
      FROM invoice_drafts
      WHERE id = ? AND company_id = ?
      LIMIT 1
      `,
      [draftId, access.companyScope]
    );

    const draftRow = draftRows[0] || null;
    if (!draftRow) {
      return res.status(404).json({
        success: false,
        message: "Invoice draft not found"
      });
    }

    const [itemRows] = await connection.query(
      `
      SELECT *
      FROM invoice_draft_items
      WHERE draft_id = ?
        AND UPPER(COALESCE(item_stage, 'PENDING')) = 'READY'
      ORDER BY id ASC
      `,
      [draftId]
    );

    return res.json({
      success: true,
      draft: {
        id: draftRow.id,
        customerName: draftRow.customer_name || "",
        mobile: draftRow.mobile || "",
        invoiceNumber: draftRow.invoice_number || "",
        invoiceDate: draftRow.invoice_date || "",
        status: draftRow.status || "DRAFT"
      },
      items: itemRows.map((item) => ({
        id: item.id,
        barcode: item.barcode || "",
        productName: item.product_name || "",
        product_name: item.product_name || "",
        itemName: item.product_name || "",
        sku: item.sku || "",
        purity: item.purity || "",
        size: item.size || "",
        weight: toNumber(item.weight),
        lot: item.lot_number || "",
        lot_number: item.lot_number || "",
        customerName: draftRow.customer_name || "",
        mobile: draftRow.mobile || "",
        invoiceDate: draftRow.invoice_date || "",
        invoiceNumber: draftRow.invoice_number || "",
        company_id: item.company_id
      }))
    });
  } catch (error) {
    console.error("Invoice draft billing fetch error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice draft billing fetch failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

/* =========================
   SAVE INVOICE
========================= */
app.post("/saveInvoice", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const {
      invoiceNumber = "",
      customerName = "",
      mobile = "",
      gstNumber = "",
      invoiceDate = "",
      paymentMode = "",
      paymentStatus = "",
      paidAmount = 0,
      dueAmount = 0,
      ratePerGram = 0,
      mcRate = 0,
      roundOff = 0,
      subtotal = 0,
      grandTotal = 0,
      items = [],
      companyId = null
    } = req.body;

    const finalCompanyId = access.companyScope;
    const validation = await validateInvoiceSaveRequest(
      connection,
      invoiceNumber,
      items,
      finalCompanyId
    );

    if (!validation.ok) {
      await connection.rollback();
      return res.status(validation.status || 400).json({
        success: false,
        message: validation.message
      });
    }

    const cleanInvoiceNumber = validation.invoiceNumber;

    const totalWeight = items.reduce((sum, item) => sum + Number(item.weight || 0), 0);

    const [saleInsert] = await connection.query(
      `
      INSERT INTO sales_history
      (
        invoice_number, customer_name, mobile, gst_number, invoice_date,
        payment_mode, payment_status, paid_amount, due_amount,
        total_items, total_weight,
        rate_per_gram, mc_rate, round_off, subtotal, total_amount, created_at, company_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?)
      `,
      [
        cleanInvoiceNumber,
        customerName,
        mobile,
        gstNumber,
        invoiceDate,
        paymentMode,
        paymentStatus,
        Number(paidAmount || 0),
        Number(dueAmount || 0),
        Number(items.length || 0),
        Number(totalWeight || 0),
        Number(ratePerGram || 0),
        Number(mcRate || 0),
        Number(roundOff || 0),
        Number(subtotal || 0),
        Number(grandTotal || 0),
        finalCompanyId
      ]
    );

    const saleId = saleInsert.insertId;

    for (const item of items) {
      const barcode = String(item.barcode || "").trim();

      await connection.query(
        `
        INSERT INTO sales_items
        (
          sale_id, invoice_number, barcode, product_name, sku, purity, size, weight, lot_number, customer_name, created_at, company_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?)
        `,
        [
          saleId,
          cleanInvoiceNumber,
          barcode,
          item.productName || item.product_name || "",
          item.sku || "",
          item.purity || "",
          item.size || "",
          Number(item.weight || 0),
          item.lot || item.lot_number || "",
          customerName,
          finalCompanyId
        ]
      );

      if (barcode) {
        const [stockUpdateResult] = await connection.query(
          `
          UPDATE stock
          SET status = 'SOLD', invoice_number = ?, sold_at = NOW()
          WHERE barcode = ? AND company_id = ? AND UPPER(COALESCE(status, 'IN_STOCK')) = 'IN_STOCK'
          `,
          [cleanInvoiceNumber, barcode, finalCompanyId]
        );

        if (Number(stockUpdateResult.affectedRows || 0) === 0) {
          await connection.rollback();
          return res.status(400).json({
            success: false,
            message: `Barcode ${barcode} could not be marked as SOLD in this company's stock`
          });
        }
      }
    }

    await connection.commit();

    return res.json({
      success: true,
      message: "Invoice saved successfully"
    });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_) {}
    }
    console.error("Save invoice error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice save failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

/* =========================
   SAVE BILLING
========================= */
app.post("/saveBilling", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const {
      invoiceNumber = "",
      customerName = "",
      mobile = "",
      gstNo = "",
      billDate = "",
      paymentMode = "",
      paymentStatus = "",
      paidAmount = 0,
      dueAmount = 0,
      totalAmount = 0,
      totalItems = 0,
      totalCount = 0,
      totalWeight = 0,
      ratePerGram = 0,
      mcRate = 0,
      roundOff = 0,
      subtotal = 0,
      metalPercent = 0,
      metalPayable = 0,
      metalNote = "",
      items = [],
      invoiceDraftId = null,
      company_id = null,
      companyId = null
    } = req.body;

    const finalCompanyId = access.companyScope;
    const validation = await validateInvoiceSaveRequest(
      connection,
      invoiceNumber,
      items,
      finalCompanyId
    );

    if (!validation.ok) {
      await connection.rollback();
      return res.status(validation.status || 400).json({
        success: false,
        message: validation.message
      });
    }

    const cleanInvoiceNumber = validation.invoiceNumber;

    const finalTotalItems = Number(totalItems || totalCount || items.length || 0);
    const finalTotalWeight = Number(
      totalWeight || items.reduce((sum, item) => sum + Number(item.weight || 0), 0)
    );

    const [saleInsert] = await connection.query(
      `
      INSERT INTO sales_history
      (
        invoice_number,
        customer_name,
        mobile,
        gst_number,
        invoice_date,
        payment_mode,
        payment_status,
        paid_amount,
        due_amount,
        total_items,
        total_weight,
        rate_per_gram,
        mc_rate,
        round_off,
        subtotal,
        total_amount,
        status,
        company_id,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, NOW())
      `,
      [
        cleanInvoiceNumber,
        String(customerName || "").trim(),
        String(mobile || "").trim(),
        String(gstNo || "").trim(),
        String(billDate || "").trim(),
        String(paymentMode || "").trim(),
        String(paymentStatus || "").trim(),
        Number(paidAmount || 0),
        Number(dueAmount || 0),
        finalTotalItems,
        Number(finalTotalWeight || 0),
        Number(ratePerGram || 0),
        Number(mcRate || 0),
        Number(roundOff || 0),
        Number(subtotal || 0),
        Number(totalAmount || 0),
        finalCompanyId
      ]
    );

    const saleId = saleInsert.insertId;

    for (const item of items) {
      const barcode = String(item.barcode || "").trim();

      await connection.query(
        `
        INSERT INTO sales_items
        (
          sale_id,
          invoice_number,
          barcode,
          product_name,
          sku,
          purity,
          size,
          weight,
          lot_number,
          customer_name,
          company_id,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
        `,
        [
          saleId,
          cleanInvoiceNumber,
          barcode,
          String(item.itemName || item.productName || item.product_name || "").trim(),
          String(item.sku || "").trim(),
          String(item.purity || "").trim(),
          String(item.size || "").trim(),
          Number(item.weight || 0),
          String(item.lot || item.lot_number || "").trim(),
          String(customerName || "").trim(),
          finalCompanyId
        ]
      );

      if (barcode) {
        const [stockUpdateResult] = await connection.query(
          `
          UPDATE stock
          SET status = 'SOLD',
              invoice_number = ?,
              sold_at = NOW(),
              deleted_at = NULL
          WHERE barcode = ? AND company_id = ? AND UPPER(COALESCE(status, 'IN_STOCK')) = 'IN_STOCK'
          `,
          [cleanInvoiceNumber, barcode, finalCompanyId]
        );

        if (Number(stockUpdateResult.affectedRows || 0) === 0) {
          await connection.rollback();
          return res.status(400).json({
            success: false,
            message: `Barcode ${barcode} could not be marked as SOLD in this company's stock`
          });
        }
      }
    }

    await postBillingToTransactionFoundation(connection, {
      companyId: finalCompanyId,
      createdBy: access.actingUserId ?? getRequestedUserId(req),
      invoiceNumber: cleanInvoiceNumber,
      customerName: String(customerName || "").trim(),
      mobile: String(mobile || "").trim(),
      gstNo: String(gstNo || "").trim(),
      billDate: String(billDate || "").trim(),
      paymentMode: String(paymentMode || "").trim(),
      paymentStatus: String(paymentStatus || "").trim(),
      paidAmount: Number(paidAmount || 0),
      dueAmount: Number(dueAmount || 0),
      totalAmount: Number(totalAmount || 0),
      totalWeight: Number(finalTotalWeight || 0),
      ratePerGram: Number(ratePerGram || 0),
      mcRate: Number(mcRate || 0),
      roundOff: Number(roundOff || 0),
      subtotal: Number(subtotal || 0),
      metalPercent: Number(metalPercent || 0),
      metalPayable: Number(metalPayable || 0),
      metalNote: String(metalNote || "").trim(),
      items
    });

    const cleanDraftId = Number(invoiceDraftId || 0);
    if (cleanDraftId > 0) {
      await connection.query(
        `
        UPDATE invoice_drafts
        SET status = 'CONVERTED',
            converted_invoice_no = ?,
            updated_by = ?,
            updated_at = NOW()
        WHERE id = ?
          AND company_id = ?
        `,
        [
          cleanInvoiceNumber,
          access.actingUserId ?? getRequestedUserId(req),
          cleanDraftId,
          finalCompanyId
        ]
      );

      await connection.query(
        `
        UPDATE invoice_draft_items
        SET item_stage = 'CONVERTED',
            updated_at = NOW()
        WHERE draft_id = ?
        `,
        [cleanDraftId]
      );
    }

    await connection.commit();

    return res.json({
      success: true,
      message: "Billing saved successfully"
    });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_) {}
    }

    console.error("Save billing error:", error);
    return res.status(500).json({
      success: false,
      message: "Server error",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

/* =========================
   SALES HISTORY
========================= */
app.get("/getSalesHistory", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [sales] = await pool.query(
      `
      SELECT 
        sh.*,
        (SELECT COUNT(*) FROM sales_items si WHERE si.sale_id = sh.id) AS total_items
      FROM sales_history sh
      ${companyId !== null ? "WHERE sh.company_id = ?" : ""}
      ORDER BY sh.id DESC
      `,
      companyId !== null ? [companyId] : []
    );

    return res.json({
      success: true,
      sales
    });
  } catch (error) {
    console.error("Sales history error:", error);
    return res.status(500).json({
      success: false,
      message: "Sales history fetch failed"
    });
  }
});

app.get("/sales-history", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [sales] = await pool.query(
      `
      SELECT 
        sh.*,
        (SELECT COUNT(*) FROM sales_items si WHERE si.sale_id = sh.id) AS total_items
      FROM sales_history sh
      ${companyId !== null ? "WHERE sh.company_id = ?" : ""}
      ORDER BY sh.id DESC
      `,
      companyId !== null ? [companyId] : []
    );

    return res.json(sales);
  } catch (error) {
    console.error("Sales history error:", error);
    return res.status(500).json([]);
  }
});

/* =========================
   INVOICE ITEMS
========================= */
app.get("/getInvoiceItems/:invoiceNumber", async (req, res) => {
  try {
    const invoiceNumber = String(req.params.invoiceNumber || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [items] = await pool.query(
      `
      SELECT *
      FROM sales_items
      WHERE invoice_number = ?
      ${companyId !== null ? "AND company_id = ?" : ""}
      ORDER BY id DESC
      `,
      companyId !== null ? [invoiceNumber, companyId] : [invoiceNumber]
    );

    return res.json({
      success: true,
      items
    });
  } catch (error) {
    console.error("Invoice items error:", error);
    return res.status(500).json({
      success: false,
      message: "Invoice items fetch failed"
    });
  }
});

/* =========================
   DELETE / RESTORE SALE
========================= */
app.put("/deleteSale/:invoiceNumber", async (req, res) => {
  let connection;

  try {
    const invoiceNumber = String(req.params.invoiceNumber || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const result = await setSaleStatusAndSyncStock(
      connection,
      invoiceNumber,
      access.companyScope,
      "DELETED"
    );

    if (!result.ok) {
      await connection.rollback();
      return res.status(result.status || 400).json({
        success: false,
        message: result.message
      });
    }

    await connection.commit();

    return res.json({ success: true });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_) {}
    }
    console.error("Delete sale error:", error);
    return res.status(500).json({ success: false });
  } finally {
    if (connection) connection.release();
  }
});

app.put("/restoreSale/:invoiceNumber", async (req, res) => {
  let connection;

  try {
    const invoiceNumber = String(req.params.invoiceNumber || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const result = await setSaleStatusAndSyncStock(
      connection,
      invoiceNumber,
      access.companyScope,
      "ACTIVE"
    );

    if (!result.ok) {
      await connection.rollback();
      return res.status(result.status || 400).json({
        success: false,
        message: result.message
      });
    }

    await connection.commit();

    return res.json({ success: true });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_) {}
    }
    console.error("Restore sale error:", error);
    return res.status(500).json({ success: false });
  } finally {
    if (connection) connection.release();
  }
});

/* =========================
   RETURN ITEM
========================= */
app.put("/returnItem/:barcode", async (req, res) => {
  try {
    const barcode = String(req.params.barcode || "").trim();
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [result] = await pool.query(
      `
      UPDATE stock
      SET status = 'IN_STOCK',
          invoice_number = '',
          sold_at = NULL
      WHERE barcode = ? AND company_id = ?
      `,
      [barcode, companyId]
    );

    if (Number(result.affectedRows || 0) === 0) {
      return res.json({
        success: false,
        message: "Item not found"
      });
    }

    return res.json({
      success: true,
      message: "Item returned successfully"
    });
  } catch (error) {
    console.error("Return item error:", error);
    return res.status(500).json({
      success: false,
      message: "Return failed",
      error: error.message
    });
  }
});

/* =========================
   COMPANY SIGNUP REQUEST
========================= */
app.post("/requestCompanySignup", async (req, res) => {
  try {
    const {
      companyName = "",
      ownerName = "",
      mobile = "",
      email = "",
      password = ""
    } = req.body;

    const cleanCompanyName = String(companyName).trim();
    const cleanOwnerName = String(ownerName).trim();
    const cleanMobile = String(mobile).trim();
    const cleanEmail = normalizeEmail(email);
    const cleanPassword = String(password).trim();

    if (!cleanCompanyName || !cleanOwnerName || !cleanEmail || !cleanPassword) {
      return res.json({
        success: false,
        message: "Company name, owner name, email, and password are required"
      });
    }

    const [existingRequest] = await pool.query(
      `
      SELECT id
      FROM company_signup_requests
      WHERE LOWER(owner_email) = LOWER(?)
        AND LOWER(COALESCE(status, '')) = 'pending'
      LIMIT 1
      `,
      [cleanEmail]
    );

    if (existingRequest.length > 0) {
      return res.json({
        success: false,
        message: "This signup request is already pending"
      });
    }

    const [existingUser] = await pool.query(
      `SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1`,
      [cleanEmail]
    );

    if (existingUser.length > 0) {
      return res.json({
        success: false,
        message: "This email already exists in the system"
      });
    }

    await pool.query(
      `
      INSERT INTO company_signup_requests
      (company_name, owner_name, mobile, owner_email, password, status)
      VALUES (?, ?, ?, ?, ?, ?)
      `,
      [cleanCompanyName, cleanOwnerName, cleanMobile, cleanEmail, cleanPassword, "pending"]
    );

    return res.json({
      success: true,
      message: "The signup request has been submitted for admin approval"
    });
  } catch (error) {
    console.error("Company signup request error:", error);
    return res.status(500).json({
      success: false,
      message: "Company signup request failed",
      error: error.message
    });
  }
});

app.get("/pendingCompanyRequests", async (req, res) => {
  try {
    const access = await requireSuperAdminAccess(req, res);
    if (!access) return;

    const [rows] = await pool.query(`
      SELECT *
      FROM company_signup_requests
      WHERE LOWER(COALESCE(status, '')) = 'pending'
      ORDER BY id DESC
    `);

    return res.json({
      success: true,
      requests: rows
    });
  } catch (error) {
    console.error("Pending company requests error:", error);
    return res.status(500).json({
      success: false,
      message: "Pending company requests fetch failed",
      error: error.message
    });
  }
});

app.put("/approveCompanyRequest/:id", async (req, res) => {
  let connection;

  try {
    const access = await requireSuperAdminAccess(req, res);
    if (!access) return;

    const requestId = Number(req.params.id);

    if (!requestId) {
      return res.json({
        success: false,
        message: "Request id is required"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [requestRows] = await connection.query(
      `
      SELECT *
      FROM company_signup_requests
      WHERE id = ?
        AND LOWER(COALESCE(status, '')) = 'pending'
      LIMIT 1
      `,
      [requestId]
    );

    if (!requestRows.length) {
      await connection.rollback();
      return res.json({
        success: false,
        message: "Pending request not found"
      });
    }

    const requestData = requestRows[0];

    const [existingCompany] = await connection.query(
      `SELECT id FROM companies WHERE LOWER(owner_email) = LOWER(?) LIMIT 1`,
      [requestData.owner_email]
    );

    if (existingCompany.length > 0) {
      await connection.rollback();
      return res.json({
        success: false,
        message: "A company already exists for this email"
      });
    }

    const [existingUser] = await connection.query(
      `SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1`,
      [requestData.owner_email]
    );

    if (existingUser.length > 0) {
      await connection.rollback();
      return res.json({
        success: false,
        message: "A user already exists for this email"
      });
    }

    const [companyInsert] = await connection.query(
      `
      INSERT INTO companies (company_name, owner_name, owner_email, status, created_at)
      VALUES (?, ?, ?, ?, NOW())
      `,
      [
        requestData.company_name,
        requestData.owner_name,
        requestData.owner_email,
        "active"
      ]
    );

    const companyId = companyInsert.insertId;

    await connection.query(
      `
      INSERT INTO users (name, mobile, email, password, role, status, company_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
      `,
      [
        requestData.owner_name,
        requestData.mobile || "",
        requestData.owner_email,
        requestData.password,
        "Admin",
        "approved",
        companyId
      ]
    );

    await connection.query(
      `
      UPDATE company_signup_requests
      SET status = 'approved', approved_at = NOW(), company_id = ?
      WHERE id = ?
      `,
      [companyId, requestId]
    );

    await connection.commit();

    return res.json({
      success: true,
      message: "The company and admin user have been created successfully",
      companyId
    });
  } catch (error) {
    if (connection) {
      try {
        await connection.rollback();
      } catch (_) {}
    }

    console.error("Approve company request error:", error);
    return res.status(500).json({
      success: false,
      message: "Approve company request failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.put("/rejectCompanyRequest/:id", async (req, res) => {
  try {
    const access = await requireSuperAdminAccess(req, res);
    if (!access) return;

    const requestId = Number(req.params.id);

    if (!requestId) {
      return res.json({
        success: false,
        message: "Request id is required"
      });
    }

    const [requestRows] = await pool.query(
      `
      SELECT id
      FROM company_signup_requests
      WHERE id = ?
        AND LOWER(COALESCE(status, '')) = 'pending'
      LIMIT 1
      `,
      [requestId]
    );

    if (!requestRows.length) {
      return res.json({
        success: false,
        message: "Pending request not found"
      });
    }

    await pool.query(
      `
      UPDATE company_signup_requests
      SET status = 'rejected', rejected_at = NOW()
      WHERE id = ?
      `,
      [requestId]
    );

    return res.json({
      success: true,
      message: "Company request rejected successfully"
    });
  } catch (error) {
    console.error("Reject company request error:", error);
    return res.status(500).json({
      success: false,
      message: "Reject company request failed",
      error: error.message
    });
  }
});

app.get("/approvedCompanies", async (req, res) => {
  try {
    const access = await requireSuperAdminAccess(req, res);
    if (!access) return;

    const [rows] = await pool.query(`
      SELECT *
      FROM companies
      ORDER BY id DESC
    `);

    return res.json({
      success: true,
      companies: rows
    });
  } catch (error) {
    console.error("Approved companies error:", error);
    return res.status(500).json({
      success: false,
      message: "Approved companies fetch failed",
      error: error.message
    });
  }
});

/* =========================
   USERS / STAFF
========================= */
app.get("/companyUsers", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const whereClause = companyId !== null ? "WHERE company_id = ?" : "";
    const params = companyId !== null ? [companyId] : [];

    const [rows] = await pool.query(
      `
      SELECT 
        id,
        name,
        mobile,
        email,
        role,
        status,
        company_id,
        created_at
      FROM users
      ${whereClause}
      ORDER BY id DESC
      `,
      params
    );

    return res.json({
      success: true,
      users: rows
    });
  } catch (error) {
    console.error("Company users error:", error);
    return res.status(500).json({
      success: false,
      message: "Company users fetch failed",
      error: error.message
    });
  }
});

app.post("/registerUser", async (req, res) => {
  try {
    const {
      name = "",
      mobile = "",
      email = "",
      password = "",
      companyId = null
    } = req.body;

    const cleanName = String(name).trim();
    const cleanMobile = String(mobile).trim();
    const cleanEmail = normalizeEmail(email);
    const cleanPassword = String(password).trim();

    const finalCompanyId =
      companyId === null || companyId === undefined || companyId === ""
        ? null
        : Number(companyId);

    if (!cleanName || !cleanEmail || !cleanPassword) {
      return res.json({
        success: false,
        message: "Name, email, and password are required"
      });
    }

    if (finalCompanyId === null || Number.isNaN(finalCompanyId)) {
      return res.json({
        success: false,
        message: "companyId is required"
      });
    }

    const [existingUsers] = await pool.query(
      `SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1`,
      [cleanEmail]
    );

    if (existingUsers.length > 0) {
      return res.json({
        success: false,
        message: "This email is already registered"
      });
    }

    await pool.query(
      `
      INSERT INTO users (name, mobile, email, password, role, status, company_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
      `,
      [cleanName, cleanMobile, cleanEmail, cleanPassword, "", "pending", finalCompanyId]
    );

    return res.json({
      success: true,
      message: "The request has been submitted for admin approval"
    });
  } catch (error) {
    console.error("Register user error:", error);
    return res.status(500).json({
      success: false,
      message: "Register failed",
      error: error.message
    });
  }
});

app.post("/requestStaffJoin", async (req, res) => {
  try {
    const {
      companyName = "",
      adminEmail = "",
      requestedRole = "",
      name = "",
      mobile = "",
      email = "",
      password = ""
    } = req.body;

    const cleanCompanyName = String(companyName).trim();
    const cleanAdminEmail = normalizeEmail(adminEmail);
    const cleanRequestedRole = String(requestedRole).trim();
    const cleanName = String(name).trim();
    const cleanMobile = String(mobile).trim();
    const cleanEmail = normalizeEmail(email);
    const cleanPassword = String(password).trim();

    if (
      !cleanCompanyName ||
      !cleanAdminEmail ||
      !cleanRequestedRole ||
      !cleanName ||
      !cleanEmail ||
      !cleanPassword
    ) {
      return res.json({
        success: false,
        message: "Please fill in all required fields"
      });
    }

    const [companyRows] = await pool.query(
      `
      SELECT c.id, c.company_name
      FROM companies c
      WHERE LOWER(c.company_name) = LOWER(?)
        AND LOWER(c.owner_email) = LOWER(?)
        AND LOWER(COALESCE(c.status,'')) = 'active'
      LIMIT 1
      `,
      [cleanCompanyName, cleanAdminEmail]
    );

    if (!companyRows.length) {
      return res.json({
        success: false,
        message: "The company or admin email did not match"
      });
    }

    const companyId = Number(companyRows[0].id);

    const [existingUsers] = await pool.query(
      `SELECT id FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1`,
      [cleanEmail]
    );

    if (existingUsers.length > 0) {
      return res.json({
        success: false,
        message: "This email is already registered"
      });
    }

    await pool.query(
      `
      INSERT INTO users (name, mobile, email, password, role, status, company_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
      `,
      [cleanName, cleanMobile, cleanEmail, cleanPassword, cleanRequestedRole, "pending", companyId]
    );

    return res.json({
      success: true,
      message: "The staff request has been submitted for admin approval"
    });
  } catch (error) {
    console.error("Request staff join error:", error);
    return res.status(500).json({
      success: false,
      message: "Staff request failed",
      error: error.message
    });
  }
});

app.get("/pendingUsers", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [rows] = await pool.query(
      `
      SELECT 
        u.id,
        u.name,
        u.mobile,
        u.email,
        u.role,
        u.status,
        u.created_at,
        u.company_id,
        c.company_name
      FROM users u
      LEFT JOIN companies c ON c.id = u.company_id
      WHERE LOWER(COALESCE(u.status, '')) = 'pending'
      ${companyId !== null ? "AND u.company_id = ?" : ""}
      ORDER BY u.id DESC
      `,
      companyId !== null ? [companyId] : []
    );

    return res.json({
      success: true,
      users: rows
    });
  } catch (error) {
    console.error("Pending users error:", error);
    return res.status(500).json({
      success: false,
      message: "Pending users fetch failed",
      error: error.message
    });
  }
});

app.get("/pendingStaffRequests", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [rows] = await pool.query(
      `
      SELECT 
        u.id,
        u.name,
        u.mobile,
        u.email,
        u.role,
        u.status,
        u.created_at,
        u.company_id,
        c.company_name
      FROM users u
      LEFT JOIN companies c ON c.id = u.company_id
      WHERE LOWER(COALESCE(u.status, '')) = 'pending'
      ${companyId !== null ? "AND u.company_id = ?" : ""}
      ORDER BY u.id DESC
      `,
      companyId !== null ? [companyId] : []
    );

    return res.json({
      success: true,
      requests: rows
    });
  } catch (error) {
    console.error("Pending staff error:", error);
    return res.status(500).json({
      success: false,
      message: "Pending staff fetch failed",
      error: error.message
    });
  }
});

app.get("/approvedUsers", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;

    const [rows] = await pool.query(
      `
      SELECT 
        u.id,
        u.name,
        u.mobile,
        u.email,
        u.role,
        u.status,
        u.created_at,
        u.company_id,
        c.company_name
      FROM users u
      LEFT JOIN companies c ON c.id = u.company_id
      WHERE LOWER(COALESCE(u.status, '')) = 'approved'
      ${companyId !== null ? "AND u.company_id = ?" : ""}
      ORDER BY u.id DESC
      `,
      companyId !== null ? [companyId] : []
    );

    return res.json({
      success: true,
      users: rows
    });
  } catch (error) {
    console.error("Approved users error:", error);
    return res.status(500).json({
      success: false,
      message: "Approved users fetch failed",
      error: error.message
    });
  }
});

app.put("/approveUser/:id", async (req, res) => {
  try {
    return await handleUserApprovalAction(req, res, {
      action: "approve",
      label: "User"
    });
  } catch (error) {
    console.error("Approve user error:", error);
    return res.status(500).json({
      success: false,
      message: "Approve failed",
      error: error.message
    });
  }
});

app.put("/approveStaffRequest/:id", async (req, res) => {
  try {
    return await handleUserApprovalAction(req, res, {
      action: "approve",
      label: "Staff"
    });
  } catch (error) {
    console.error("Approve staff error:", error);
    return res.status(500).json({
      success: false,
      message: "Approve failed",
      error: error.message
    });
  }
});

app.put("/rejectUser/:id", async (req, res) => {
  try {
    return await handleUserApprovalAction(req, res, {
      action: "reject",
      label: "User"
    });
  } catch (error) {
    console.error("Reject user error:", error);
    return res.status(500).json({
      success: false,
      message: "Reject failed",
      error: error.message
    });
  }
});

app.put("/rejectStaffRequest/:id", async (req, res) => {
  try {
    return await handleUserApprovalAction(req, res, {
      action: "reject",
      label: "Staff"
    });
  } catch (error) {
    console.error("Reject staff error:", error);
    return res.status(500).json({
      success: false,
      message: "Reject failed",
      error: error.message
    });
  }
});

/* =========================
   LOGIN
========================= */
app.post("/login", async (req, res) => {
  try {
    const email = normalizeEmail(req.body.email);
    const password = String(req.body.password || "").trim();

    if (!email || !password) {
      return res.json({
        success: false,
        message: "Email and password are required"
      });
    }

    const user = await findUserByEmailAndPassword(email, password);

    if (!user) {
      return res.json({ success: false, message: "Invalid login" });
    }

    if (isSuperAdminUser(user)) {
      user.role = "SuperAdmin";
      user.status = "approved";
      user.company_id = null;
      user.company_name = "";
      user.company_status = "";
    }

    if (String(user.status || "").toLowerCase() !== "approved") {
      return res.json({ success: false, message: "Pending approval" });
    }

    if (!String(user.role || "").trim()) {
      return res.json({ success: false, message: "Role not assigned yet" });
    }

    return res.json({
      success: true,
      user: {
        id: user.id,
        name: user.name,
        mobile: user.mobile,
        email: user.email,
        role: user.role,
        status: user.status,
        company_id: user.company_id,
        companyId: user.company_id,
        company_name: user.company_name || "",
        companyName: user.company_name || "",
        company_status: user.company_status || ""
      }
    });
  } catch (error) {
    console.error("Login error:", error);
    return res.status(500).json({
      success: false,
      message: "Server error",
      error: error.message
    });
  }
});

app.get("/userByEmail", async (req, res) => {
  try {
    const email = normalizeEmail(req.query.email);

    if (!email) {
      return res.json({ success: false, message: "Email is required" });
    }

    const [rows] = await pool.query(
      `
      SELECT 
        u.id, u.name, u.mobile, u.email, u.role, u.status, u.company_id, u.created_at,
        c.company_name
      FROM users u
      LEFT JOIN companies c ON c.id = u.company_id
      WHERE LOWER(u.email) = LOWER(?)
      LIMIT 1
      `,
      [email]
    );

    if (!rows.length) {
      return res.json({ success: false, message: "User not found" });
    }

    return res.json({ success: true, user: rows[0] });
  } catch (error) {
    console.error("User by email error:", error);
    return res.status(500).json({
      success: false,
      message: "Fetch failed",
      error: error.message
    });
  }
});

/* =========================
   TRANSACTION FOUNDATION
========================= */
app.post("/transaction/parties", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const partyName = String(req.body.party_name || req.body.partyName || "").trim();
    const displayName = String(req.body.display_name || req.body.displayName || "").trim();
    const partyType = normalizePartyType(req.body.party_type || req.body.partyType);
    const partyCode = String(req.body.party_code || req.body.partyCode || `PTY-${Date.now()}`).trim();
    const mobile = String(req.body.mobile || "").trim();
    const alternateMobile = String(req.body.alternate_mobile || req.body.alternateMobile || "").trim();
    const gstNo = String(req.body.gst_no || req.body.gstNo || "").trim();
    const panNo = String(req.body.pan_no || req.body.panNo || "").trim();
    const addressLine1 = String(req.body.address_line1 || req.body.addressLine1 || "").trim();
    const addressLine2 = String(req.body.address_line2 || req.body.addressLine2 || "").trim();
    const city = String(req.body.city || "").trim();
    const state = String(req.body.state || "").trim();
    const pinCode = String(req.body.pin_code || req.body.pinCode || "").trim();
    const contactPerson = String(req.body.contact_person || req.body.contactPerson || "").trim();
    const defaultMetalType = normalizeMetalType(req.body.default_metal_type || req.body.defaultMetalType);
    const defaultPurity = toNumber(req.body.default_purity ?? req.body.defaultPurity ?? 0);
    const remarks = String(req.body.remarks || "").trim();
    const finalCompanyId = access.companyScope;
    const finalUserId = access.actingUserId ?? getRequestedUserId(req);

    if (!partyName || !partyType) {
      return res.status(400).json({
        success: false,
        message: "party_name and party_type are required"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const [duplicateRows] = await connection.query(
      `
      SELECT id
      FROM party_master
      WHERE company_id = ?
        AND LOWER(TRIM(party_name)) = LOWER(TRIM(?))
        AND party_type = ?
      LIMIT 1
      `,
      [finalCompanyId, partyName, partyType]
    );

    if (duplicateRows.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        success: false,
        message: "The same party name and type already exist"
      });
    }

    const [insertResult] = await connection.query(
      `
      INSERT INTO party_master
      (
        company_id, party_code, party_name, display_name, party_type, status,
        mobile, alternate_mobile, gst_no, pan_no,
        address_line1, address_line2, city, state, pin_code,
        contact_person, default_metal_type, default_purity, remarks, created_by
      )
      VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        finalCompanyId,
        partyCode,
        partyName,
        displayName,
        partyType,
        mobile,
        alternateMobile,
        gstNo,
        panNo,
        addressLine1,
        addressLine2,
        city,
        state,
        pinCode,
        contactPerson,
        defaultMetalType,
        defaultPurity,
        remarks,
        finalUserId
      ]
    );

    await ensurePartyBalanceSummaryRow(connection, finalCompanyId, insertResult.insertId);
    await connection.commit();

    return res.json({
      success: true,
      message: "Party created successfully",
      partyId: insertResult.insertId
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Create party error:", error);
    return res.status(500).json({
      success: false,
      message: "Party create failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.get("/transaction/parties", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const search = String(req.query.search || "").trim().toLowerCase();
    const filterPartyType = normalizePartyType(req.query.partyType || req.query.party_type);
    const params = [];
    const whereParts = [];

    if (companyId !== null) {
      whereParts.push("pm.company_id = ?");
      params.push(companyId);
    }

    if (filterPartyType) {
      whereParts.push("pm.party_type = ?");
      params.push(filterPartyType);
    }

    if (search) {
      whereParts.push("(LOWER(pm.party_name) LIKE ? OR LOWER(pm.party_code) LIKE ? OR LOWER(pm.mobile) LIKE ?)");
      params.push(`%${search}%`, `%${search}%`, `%${search}%`);
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(" AND ")}` : "";
    const [rows] = await pool.query(
      `
      SELECT
        pm.*,
        pbs.cash_balance,
        pbs.gold_gross_balance,
        pbs.gold_fine_balance,
        pbs.silver_gross_balance,
        pbs.silver_fine_balance
      FROM party_master pm
      LEFT JOIN party_balance_summary pbs
        ON pbs.party_id = pm.id AND pbs.company_id = pm.company_id
      ${whereClause}
      ORDER BY pm.party_name ASC, pm.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      parties: rows
    });
  } catch (error) {
    console.error("Get parties error:", error);
    return res.status(500).json({
      success: false,
      message: "Party fetch failed",
      error: error.message
    });
  }
});

app.post("/transaction/transactions", async (req, res) => {
  let connection;

  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: true,
      allowSuperAdminAll: false
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const finalCompanyId = access.companyScope;
    const finalUserId = access.actingUserId ?? getRequestedUserId(req);
    const partyId = Number(req.body.party_id ?? req.body.partyId ?? 0);
    const transactionType = normalizeTransactionType(req.body.transaction_type || req.body.transactionType);
    const voucherNo = String(req.body.voucher_no || req.body.voucherNo || buildVoucherNo(transactionType)).trim();
    const voucherDate = String(req.body.voucher_date || req.body.voucherDate || getTodayDateOnly()).trim();
    const voucherTime = String(req.body.voucher_time || req.body.voucherTime || "").trim();
    const status = normalizeTransactionStatus(req.body.status);
    const referenceNo = String(req.body.reference_no || req.body.referenceNo || "").trim();
    const invoiceNo = String(req.body.invoice_no || req.body.invoiceNo || "").trim();
    const purchaseNo = String(req.body.purchase_no || req.body.purchaseNo || "").trim();
    const lotNo = String(req.body.lot_no || req.body.lotNo || "").trim();
    const processLotNo = String(req.body.process_lot_no || req.body.processLotNo || "").trim();
    const karigarId = req.body.karigar_id ?? req.body.karigarId ?? null;
    const sourceModule = String(req.body.source_module || req.body.sourceModule || "transaction_phase1").trim();
    const paymentMode = String(req.body.payment_mode || req.body.paymentMode || "").trim();
    const paymentStatus = String(req.body.payment_status || req.body.paymentStatus || "").trim();
    const remarks = String(req.body.remarks || "").trim();
    const note = String(req.body.note || "").trim();

    const cashAmount = toNumber(req.body.cash_amount ?? req.body.cashAmount ?? 0);
    const cashEntryType =
      normalizeCashEntryType(req.body.cash_entry_type || req.body.cashEntryType) ||
      getDefaultCashEntryType(transactionType);

    const metalType = normalizeMetalType(req.body.metal_type || req.body.metalType);
    const metalEntryType =
      normalizeMetalEntryType(req.body.metal_entry_type || req.body.metalEntryType) ||
      getDefaultMetalEntryType(transactionType);
    const purity = toNumber(req.body.purity ?? 0);
    const grossWeight = toNumber(req.body.gross_weight ?? req.body.grossWeight ?? 0);
    const fineWeight = toNumber(req.body.fine_weight ?? req.body.fineWeight ?? 0);

    const lines = Array.isArray(req.body.lines) ? req.body.lines : [];
    const settlements = Array.isArray(req.body.settlements) ? req.body.settlements : [];

    if (!partyId || !transactionType) {
      return res.status(400).json({
        success: false,
        message: "party_id and transaction_type are required"
      });
    }

    connection = await pool.getConnection();
    await connection.beginTransaction();

    const party = await getPartyByIdForCompany(connection, finalCompanyId, partyId);

    if (!party) {
      await connection.rollback();
      return res.status(404).json({
        success: false,
        message: "Party not found"
      });
    }

    const finalPartyType = normalizePartyType(req.body.party_type || req.body.partyType || party.party_type);

    const [insertResult] = await connection.query(
      `
      INSERT INTO transaction_master
      (
        company_id, voucher_no, voucher_date, voucher_time, transaction_type,
        party_id, party_type, status, reference_no, invoice_no,
        purchase_no, lot_no, process_lot_no, karigar_id, source_module,
        payment_mode, payment_status, remarks, note, created_by
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        finalCompanyId,
        voucherNo,
        voucherDate || null,
        voucherTime || null,
        transactionType,
        partyId,
        finalPartyType,
        status,
        referenceNo,
        invoiceNo,
        purchaseNo,
        lotNo,
        processLotNo,
        karigarId || null,
        sourceModule,
        paymentMode,
        paymentStatus,
        remarks,
        note,
        finalUserId
      ]
    );

    const transactionId = insertResult.insertId;
    const finalLines = lines.length
      ? lines
      : [{
          line_no: 1,
          item_name: String(req.body.item_name || req.body.itemName || "").trim(),
          barcode: String(req.body.barcode || "").trim(),
          lot_no: lotNo,
          metal_type: metalType,
          purity,
          gross_weight: grossWeight,
          fine_weight: fineWeight,
          qty: toNumber(req.body.qty ?? 0),
          rate_per_gram: toNumber(req.body.rate_per_gram ?? req.body.ratePerGram ?? 0),
          metal_value: toNumber(req.body.metal_value ?? req.body.metalValue ?? 0),
          making_charge: toNumber(req.body.making_charge ?? req.body.makingCharge ?? 0),
          hallmark_charge: toNumber(req.body.hallmark_charge ?? req.body.hallmarkCharge ?? 0),
          labour_charge: toNumber(req.body.labour_charge ?? req.body.labourCharge ?? 0),
          other_charge: toNumber(req.body.other_charge ?? req.body.otherCharge ?? 0),
          discount_amount: toNumber(req.body.discount_amount ?? req.body.discountAmount ?? 0),
          gst_amount: toNumber(req.body.gst_amount ?? req.body.gstAmount ?? 0),
          line_amount: toNumber(req.body.line_amount ?? req.body.lineAmount ?? cashAmount ?? 0),
          remarks
        }];

    for (let index = 0; index < finalLines.length; index += 1) {
      const line = finalLines[index] || {};
      await connection.query(
        `
        INSERT INTO transaction_lines
        (
          transaction_id, line_no, item_name, item_id, barcode, lot_no,
          metal_type, purity, gross_weight, net_weight, fine_weight, qty,
          rate_per_gram, metal_value, making_charge, hallmark_charge,
          labour_charge, other_charge, discount_amount, gst_amount,
          line_amount, remarks
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          transactionId,
          Number(line.line_no || index + 1),
          String(line.item_name || line.itemName || "").trim(),
          line.item_id ?? line.itemId ?? null,
          String(line.barcode || "").trim(),
          String(line.lot_no || line.lotNo || lotNo || "").trim(),
          normalizeMetalType(line.metal_type || line.metalType || metalType),
          toNumber(line.purity),
          toNumber(line.gross_weight ?? line.grossWeight),
          toNumber(line.net_weight ?? line.netWeight),
          toNumber(line.fine_weight ?? line.fineWeight),
          toNumber(line.qty),
          toNumber(line.rate_per_gram ?? line.ratePerGram),
          toNumber(line.metal_value ?? line.metalValue),
          toNumber(line.making_charge ?? line.makingCharge),
          toNumber(line.hallmark_charge ?? line.hallmarkCharge),
          toNumber(line.labour_charge ?? line.labourCharge),
          toNumber(line.other_charge ?? line.otherCharge),
          toNumber(line.discount_amount ?? line.discountAmount),
          toNumber(line.gst_amount ?? line.gstAmount),
          toNumber(line.line_amount ?? line.lineAmount),
          String(line.remarks || "").trim()
        ]
      );
    }

    for (const settlement of settlements) {
      const settlementType = normalizeSettlementType(settlement.settlement_type || settlement.settlementType);
      if (!settlementType) continue;

      await connection.query(
        `
        INSERT INTO transaction_settlements
        (
          company_id, transaction_id, settlement_type, against_transaction_id,
          against_invoice_no, against_voucher_no, cash_amount, metal_type,
          gross_weight, fine_weight, purity, rate_basis, settlement_date,
          remarks, created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          finalCompanyId,
          transactionId,
          settlementType,
          settlement.against_transaction_id ?? settlement.againstTransactionId ?? null,
          String(settlement.against_invoice_no || settlement.againstInvoiceNo || "").trim(),
          String(settlement.against_voucher_no || settlement.againstVoucherNo || "").trim(),
          toNumber(settlement.cash_amount ?? settlement.cashAmount),
          normalizeMetalType(settlement.metal_type || settlement.metalType),
          toNumber(settlement.gross_weight ?? settlement.grossWeight),
          toNumber(settlement.fine_weight ?? settlement.fineWeight),
          toNumber(settlement.purity),
          toNumber(settlement.rate_basis ?? settlement.rateBasis),
          String(settlement.settlement_date || settlement.settlementDate || voucherDate || "").trim() || null,
          String(settlement.remarks || "").trim(),
          finalUserId
        ]
      );
    }

    if (cashAmount > 0 && cashEntryType) {
      await createCashLedgerEntry(connection, {
        companyId: finalCompanyId,
        partyId,
        transactionId,
        entryDate: voucherDate,
        entryType: cashEntryType,
        debitAmount: cashEntryType === "DEBIT" ? cashAmount : 0,
        creditAmount: cashEntryType === "CREDIT" ? cashAmount : 0,
        referenceType: transactionType,
        referenceNo: voucherNo,
        remarks,
        createdBy: finalUserId
      });
    }

    if (metalType && metalEntryType && (grossWeight > 0 || fineWeight > 0)) {
      await createMetalLedgerEntry(connection, {
        companyId: finalCompanyId,
        partyId,
        transactionId,
        entryDate: voucherDate,
        metalType,
        entryType: metalEntryType,
        purity,
        grossIn: metalEntryType === "IN" ? grossWeight : 0,
        grossOut: metalEntryType === "OUT" ? grossWeight : 0,
        fineIn: metalEntryType === "IN" ? fineWeight : 0,
        fineOut: metalEntryType === "OUT" ? fineWeight : 0,
        referenceType: transactionType,
        referenceNo: voucherNo,
        lotNo,
        remarks,
        createdBy: finalUserId
      });
    }

    if (invoiceNo) {
      await connection.query(
        `
        INSERT INTO invoice_transaction_link
        (company_id, invoice_no, transaction_id, link_type, remarks, created_by)
        VALUES (?, ?, ?, ?, ?, ?)
        `,
        [finalCompanyId, invoiceNo, transactionId, transactionType, remarks, finalUserId]
      );
    }

    if (purchaseNo) {
      await connection.query(
        `
        INSERT INTO purchase_transaction_link
        (company_id, purchase_no, transaction_id, link_type, remarks, created_by)
        VALUES (?, ?, ?, ?, ?, ?)
        `,
        [finalCompanyId, purchaseNo, transactionId, transactionType, remarks, finalUserId]
      );
    }

    if (lotNo || processLotNo) {
      await connection.query(
        `
        INSERT INTO lot_transaction_link
        (company_id, lot_no, process_lot_no, transaction_id, link_type, remarks, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        [finalCompanyId, lotNo, processLotNo, transactionId, transactionType, remarks, finalUserId]
      );
    }

    if (karigarId) {
      await connection.query(
        `
        INSERT INTO karigar_transaction_link
        (
          company_id, karigar_id, transaction_id, lot_no, process_lot_no,
          issue_weight, receive_weight, loss_weight, labour_amount, remarks, created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          finalCompanyId,
          Number(karigarId),
          transactionId,
          lotNo,
          processLotNo,
          transactionType === "KARIGAR_ISSUE" ? grossWeight : 0,
          transactionType === "KARIGAR_RECEIVE" ? grossWeight : 0,
          transactionType === "KARIGAR_LOSS_ADJUSTMENT" ? grossWeight : 0,
          transactionType === "KARIGAR_LABOUR" ? cashAmount : 0,
          remarks,
          finalUserId
        ]
      );
    }

    await recalcPartyBalanceSummary(connection, finalCompanyId, partyId, transactionId);
    await connection.commit();

    return res.json({
      success: true,
      message: "Transaction created successfully",
      transactionId,
      voucherNo
    });
  } catch (error) {
    if (connection) await connection.rollback();
    console.error("Create transaction error:", error);
    return res.status(500).json({
      success: false,
      message: "Transaction create failed",
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

app.get("/transaction/transactions", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const partyId = Number(req.query.partyId || req.query.party_id || 0);
    const transactionType = normalizeTransactionType(req.query.transactionType || req.query.transaction_type);
    const params = [];
    const whereParts = [];

    if (companyId !== null) {
      whereParts.push("tm.company_id = ?");
      params.push(companyId);
    }

    if (partyId) {
      whereParts.push("tm.party_id = ?");
      params.push(partyId);
    }

    if (transactionType) {
      whereParts.push("tm.transaction_type = ?");
      params.push(transactionType);
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(" AND ")}` : "";
    const [rows] = await pool.query(
      `
      SELECT
        tm.*,
        pm.party_name,
        pm.party_code,
        COUNT(DISTINCT tl.id) AS total_lines,
        COALESCE(SUM(tl.line_amount), 0) AS total_line_amount,
        COALESCE(SUM(tl.qty), 0) AS total_qty,
        COALESCE(SUM(tl.gross_weight), 0) AS total_gross_weight,
        COALESCE(SUM(tl.fine_weight), 0) AS total_fine_weight,
        MAX(COALESCE(tl.metal_type, '')) AS metal_type,
        GROUP_CONCAT(DISTINCT NULLIF(TRIM(COALESCE(tl.item_name, '')), '') SEPARATOR ', ') AS item_names
      FROM transaction_master tm
      LEFT JOIN party_master pm ON pm.id = tm.party_id
      LEFT JOIN transaction_lines tl ON tl.transaction_id = tm.id
      ${whereClause}
      GROUP BY tm.id
      ORDER BY tm.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      transactions: rows
    });
  } catch (error) {
    console.error("Get transactions error:", error);
    return res.status(500).json({
      success: false,
      message: "Transaction fetch failed",
      error: error.message
    });
  }
});

app.get("/transaction/open-context", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const partyId = Number(req.query.partyId || req.query.party_id || 0);

    if (!partyId) {
      return res.json({
        success: true,
        openInvoices: [],
        recentReferences: []
      });
    }

    const openInvoiceParams = [];
    const recentReferenceParams = [];
    const openInvoiceCompanyFilter = companyId !== null ? "tm.company_id = ? AND " : "";
    const recentReferenceCompanyFilter = companyId !== null ? "tm.company_id = ? AND " : "";
    const settlementCompanyFilter = companyId !== null ? "AND ts.company_id = ?" : "";

    if (companyId !== null) {
      openInvoiceParams.push(companyId);
      recentReferenceParams.push(companyId);
    }

    openInvoiceParams.push(partyId);
    if (companyId !== null) {
      openInvoiceParams.push(companyId);
    }

    recentReferenceParams.push(partyId);

    const [invoiceRows] = await pool.query(
      `
      SELECT
        tm.id AS transaction_id,
        tm.voucher_no,
        tm.voucher_date,
        tm.reference_no,
        tm.invoice_no,
        tm.lot_no,
        tm.process_lot_no,
        COALESCE(SUM(tl.line_amount), 0) AS total_amount,
        COALESCE((
          SELECT SUM(COALESCE(ts.cash_amount, 0))
          FROM transaction_settlements ts
          WHERE ts.against_transaction_id = tm.id
          ${settlementCompanyFilter}
        ), 0) AS settled_amount
      FROM transaction_master tm
      LEFT JOIN transaction_lines tl ON tl.transaction_id = tm.id
      WHERE ${openInvoiceCompanyFilter} tm.party_id = ? AND tm.transaction_type = 'SALE_INVOICE'
      GROUP BY tm.id
      ORDER BY tm.id DESC
      LIMIT 25
      `,
      openInvoiceParams
    );

    const [referenceRows] = await pool.query(
      `
      SELECT
        tm.id AS transaction_id,
        tm.voucher_no,
        tm.voucher_date,
        tm.transaction_type,
        tm.reference_no,
        tm.invoice_no,
        tm.lot_no,
        tm.process_lot_no,
        tm.note,
        tm.remarks
      FROM transaction_master tm
      WHERE ${recentReferenceCompanyFilter} tm.party_id = ?
        AND (
          NULLIF(TRIM(COALESCE(tm.reference_no, '')), '') IS NOT NULL
          OR NULLIF(TRIM(COALESCE(tm.invoice_no, '')), '') IS NOT NULL
          OR NULLIF(TRIM(COALESCE(tm.lot_no, '')), '') IS NOT NULL
          OR NULLIF(TRIM(COALESCE(tm.process_lot_no, '')), '') IS NOT NULL
        )
      ORDER BY tm.id DESC
      LIMIT 12
      `,
      recentReferenceParams
    );

    const openInvoices = invoiceRows
      .map((row) => {
        const totalAmount = toNumber(row.total_amount);
        const settledAmount = toNumber(row.settled_amount);
        const openAmount = Math.max(totalAmount - settledAmount, 0);
        return {
          ...row,
          total_amount: totalAmount,
          settled_amount: settledAmount,
          open_amount: openAmount
        };
      })
      .filter((row) => row.open_amount > 0.009);

    return res.json({
      success: true,
      openInvoices,
      recentReferences: referenceRows
    });
  } catch (error) {
    console.error("Get transaction open context error:", error);
    return res.status(500).json({
      success: false,
      message: "Transaction open context fetch failed",
      error: error.message
    });
  }
});

app.get("/transaction/cash-ledger", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const partyId = Number(req.query.partyId || req.query.party_id || 0);
    const params = [];
    const whereParts = [];

    if (companyId !== null) {
      whereParts.push("cl.company_id = ?");
      params.push(companyId);
    }

    if (partyId) {
      whereParts.push("cl.party_id = ?");
      params.push(partyId);
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(" AND ")}` : "";
    const [rows] = await pool.query(
      `
      SELECT
        cl.*,
        pm.party_name,
        pm.party_type,
        tm.voucher_no,
        tm.transaction_type
      FROM cash_ledger cl
      LEFT JOIN party_master pm ON pm.id = cl.party_id
      LEFT JOIN transaction_master tm ON tm.id = cl.transaction_id
      ${whereClause}
      ORDER BY cl.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      ledger: rows
    });
  } catch (error) {
    console.error("Get cash ledger error:", error);
    return res.status(500).json({
      success: false,
      message: "Cash ledger fetch failed",
      error: error.message
    });
  }
});

app.get("/transaction/metal-ledger", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const partyId = Number(req.query.partyId || req.query.party_id || 0);
    const metalType = normalizeMetalType(req.query.metalType || req.query.metal_type);
    const params = [];
    const whereParts = [];

    if (companyId !== null) {
      whereParts.push("ml.company_id = ?");
      params.push(companyId);
    }

    if (partyId) {
      whereParts.push("ml.party_id = ?");
      params.push(partyId);
    }

    if (metalType) {
      whereParts.push("ml.metal_type = ?");
      params.push(metalType);
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(" AND ")}` : "";
    const [rows] = await pool.query(
      `
      SELECT
        ml.*,
        pm.party_name,
        pm.party_type,
        tm.voucher_no,
        tm.transaction_type
      FROM metal_ledger ml
      LEFT JOIN party_master pm ON pm.id = ml.party_id
      LEFT JOIN transaction_master tm ON tm.id = ml.transaction_id
      ${whereClause}
      ORDER BY ml.id DESC
      `,
      params
    );

    return res.json({
      success: true,
      ledger: rows
    });
  } catch (error) {
    console.error("Get metal ledger error:", error);
    return res.status(500).json({
      success: false,
      message: "Metal ledger fetch failed",
      error: error.message
    });
  }
});

app.get("/transaction/reports/party-ledger", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const fromDate = String(req.query.fromDate || req.query.from_date || "").trim();
    const toDate = String(req.query.toDate || req.query.to_date || "").trim();
    const partyId = Number(req.query.partyId || req.query.party_id || 0);
    const partyType = normalizePartyType(req.query.partyType || req.query.party_type);
    const transactionType = normalizeTransactionType(req.query.transactionType || req.query.transaction_type);
    const metalType = normalizeMetalType(req.query.metalType || req.query.metal_type);
    const invoiceNo = String(req.query.invoiceNo || req.query.invoice_no || "").trim();
    const lotNo = String(req.query.lotNo || req.query.lot_no || "").trim();
    const processLotNo = String(req.query.processLotNo || req.query.process_lot_no || "").trim();

    const baseParams = [];
    const baseWhere = [];

    if (companyId !== null) {
      baseWhere.push("tm.company_id = ?");
      baseParams.push(companyId);
    }

    if (partyId) {
      baseWhere.push("tm.party_id = ?");
      baseParams.push(partyId);
    }

    if (partyType) {
      baseWhere.push("tm.party_type = ?");
      baseParams.push(partyType);
    }

    if (transactionType) {
      baseWhere.push("tm.transaction_type = ?");
      baseParams.push(transactionType);
    }

    if (invoiceNo) {
      baseWhere.push("tm.invoice_no LIKE ?");
      baseParams.push(`%${invoiceNo}%`);
    }

    if (lotNo) {
      baseWhere.push("tm.lot_no LIKE ?");
      baseParams.push(`%${lotNo}%`);
    }

    if (processLotNo) {
      baseWhere.push("tm.process_lot_no LIKE ?");
      baseParams.push(`%${processLotNo}%`);
    }

    if (metalType) {
      baseWhere.push(`
        EXISTS (
          SELECT 1
          FROM transaction_lines tl_filter
          WHERE tl_filter.transaction_id = tm.id
            AND tl_filter.metal_type = ?
        )
      `);
      baseParams.push(metalType);
    }

    const rangeWhere = [...baseWhere];
    const rangeParams = [...baseParams];
    if (fromDate) {
      rangeWhere.push("tm.voucher_date >= ?");
      rangeParams.push(fromDate);
    }
    if (toDate) {
      rangeWhere.push("tm.voucher_date <= ?");
      rangeParams.push(toDate);
    }

    const rangeWhereClause = rangeWhere.length ? `WHERE ${rangeWhere.join(" AND ")}` : "";
    const [rows] = await pool.query(
      `
      SELECT
        tm.id,
        tm.voucher_date,
        tm.voucher_no,
        tm.transaction_type,
        tm.reference_no,
        tm.invoice_no,
        tm.lot_no,
        tm.process_lot_no,
        tm.remarks,
        tm.note,
        pm.party_name,
        COALESCE(cash.debit_amount, 0) AS cash_debit,
        COALESCE(cash.credit_amount, 0) AS cash_credit,
        COALESCE(gold.gross_in, 0) AS gold_in,
        COALESCE(gold.gross_out, 0) AS gold_out,
        COALESCE(silver.gross_in, 0) AS silver_in,
        COALESCE(silver.gross_out, 0) AS silver_out
      FROM transaction_master tm
      LEFT JOIN party_master pm ON pm.id = tm.party_id
      LEFT JOIN (
        SELECT transaction_id, SUM(debit_amount) AS debit_amount, SUM(credit_amount) AS credit_amount
        FROM cash_ledger
        GROUP BY transaction_id
      ) cash ON cash.transaction_id = tm.id
      LEFT JOIN (
        SELECT transaction_id, SUM(gross_in) AS gross_in, SUM(gross_out) AS gross_out
        FROM metal_ledger
        WHERE metal_type = 'GOLD'
        GROUP BY transaction_id
      ) gold ON gold.transaction_id = tm.id
      LEFT JOIN (
        SELECT transaction_id, SUM(gross_in) AS gross_in, SUM(gross_out) AS gross_out
        FROM metal_ledger
        WHERE metal_type = 'SILVER'
        GROUP BY transaction_id
      ) silver ON silver.transaction_id = tm.id
      ${rangeWhereClause}
      ORDER BY tm.voucher_date ASC, tm.id ASC
      `,
      rangeParams
    );

    let openingCashBalance = 0;
    if (fromDate) {
      const openingWhere = [...baseWhere, "tm.voucher_date < ?"];
      const openingParams = [...baseParams, fromDate];
      const openingWhereClause = openingWhere.length ? `WHERE ${openingWhere.join(" AND ")}` : "";
      const [openingRows] = await pool.query(
        `
        SELECT
          COALESCE(SUM(cl.debit_amount), 0) - COALESCE(SUM(cl.credit_amount), 0) AS opening_cash_balance
        FROM cash_ledger cl
        INNER JOIN transaction_master tm ON tm.id = cl.transaction_id
        ${openingWhereClause}
        `,
        openingParams
      );
      openingCashBalance = toNumber(openingRows[0]?.opening_cash_balance);
    }

    let runningCashBalance = openingCashBalance;
    let goldBalance = 0;
    let silverBalance = 0;

    const ledgerRows = rows.map((row) => {
      const cashDebit = toNumber(row.cash_debit);
      const cashCredit = toNumber(row.cash_credit);
      const goldIn = toNumber(row.gold_in);
      const goldOut = toNumber(row.gold_out);
      const silverIn = toNumber(row.silver_in);
      const silverOut = toNumber(row.silver_out);

      runningCashBalance += cashDebit - cashCredit;
      goldBalance += goldIn - goldOut;
      silverBalance += silverIn - silverOut;

      return {
        ...row,
        cash_debit: cashDebit,
        cash_credit: cashCredit,
        running_cash_balance: runningCashBalance,
        gold_in: goldIn,
        gold_out: goldOut,
        silver_in: silverIn,
        silver_out: silverOut
      };
    });

    return res.json({
      success: true,
      rows: ledgerRows,
      summary: {
        openingCashBalance,
        currentCashBalance: runningCashBalance,
        goldBalance,
        silverBalance
      }
    });
  } catch (error) {
    console.error("Get party ledger report error:", error);
    return res.status(500).json({
      success: false,
      message: "Party ledger report fetch failed",
      error: error.message
    });
  }
});

app.get("/transaction/reports/customer-due", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const asOnDate = String(req.query.asOnDate || req.query.as_on_date || getTodayDateOnly()).trim();
    const customerId = Number(req.query.customerId || req.query.customer_id || req.query.partyId || req.query.party_id || 0);
    const invoiceNo = String(req.query.invoiceNo || req.query.invoice_no || "").trim();
    const overdueOnly = parseBooleanLike(req.query.overdueOnly || req.query.overdue_only);
    const openOnly = parseBooleanLike(req.query.openOnly || req.query.open_only);

    const params = [];
    const whereParts = ["tm.transaction_type = 'SALE_INVOICE'"];

    if (companyId !== null) {
      whereParts.push("tm.company_id = ?");
      params.push(companyId);
    }

    whereParts.push("tm.voucher_date <= ?");
    params.push(asOnDate);

    if (customerId) {
      whereParts.push("tm.party_id = ?");
      params.push(customerId);
    }

    if (invoiceNo) {
      whereParts.push("tm.invoice_no LIKE ?");
      params.push(`%${invoiceNo}%`);
    }

    const settlementParams = [];
    const settlementCompanyFilter = companyId !== null ? "ts.company_id = ? AND" : "";
    if (companyId !== null) {
      settlementParams.push(companyId);
    }
    settlementParams.push(asOnDate);

    const [rows] = await pool.query(
      `
      SELECT
        tm.id AS transaction_id,
        pm.party_name AS customer_name,
        tm.invoice_no,
        tm.voucher_date AS invoice_date,
        tm.payment_status,
        tm.reference_no,
        COALESCE(lines.bill_amount, 0) AS bill_amount,
        COALESCE(settle.settled_amount, 0) AS settled_amount,
        settle.last_settlement_date
      FROM transaction_master tm
      LEFT JOIN party_master pm ON pm.id = tm.party_id
      LEFT JOIN (
        SELECT transaction_id, SUM(line_amount) AS bill_amount
        FROM transaction_lines
        GROUP BY transaction_id
      ) lines ON lines.transaction_id = tm.id
      LEFT JOIN (
        SELECT
          against_transaction_id,
          SUM(COALESCE(cash_amount, 0)) AS settled_amount,
          MAX(settlement_date) AS last_settlement_date
        FROM transaction_settlements ts
        WHERE ${settlementCompanyFilter} settlement_date <= ?
        GROUP BY against_transaction_id
      ) settle ON settle.against_transaction_id = tm.id
      WHERE ${whereParts.join(" AND ")}
      ORDER BY tm.voucher_date DESC, tm.id DESC
      `,
      [...settlementParams, ...params]
    );

    const filteredRows = rows
      .map((row) => {
        const billAmount = toNumber(row.bill_amount);
        const settledAmount = toNumber(row.settled_amount);
        const openDue = Math.max(billAmount - settledAmount, 0);
        const isOverdue = openDue > 0.009 && !!asOnDate && String(row.invoice_date || "") < asOnDate;

        return {
          ...row,
          bill_amount: billAmount,
          settled_amount: settledAmount,
          open_due: openDue,
          is_overdue: isOverdue
        };
      })
      .filter((row) => (openOnly ? row.open_due > 0.009 : true))
      .filter((row) => (overdueOnly ? row.is_overdue : true));

    const summary = filteredRows.reduce(
      (acc, row) => {
        acc.totalBilledAmount += row.bill_amount;
        acc.totalSettledAmount += row.settled_amount;
        acc.totalOpenDue += row.open_due;
        if (row.open_due > 0.009) {
          acc.customerSet.add(String(row.customer_name || ""));
        }
        return acc;
      },
      {
        totalBilledAmount: 0,
        totalSettledAmount: 0,
        totalOpenDue: 0,
        customerSet: new Set()
      }
    );

    return res.json({
      success: true,
      rows: filteredRows,
      summary: {
        totalCustomersWithDue: summary.customerSet.size,
        totalOpenDue: summary.totalOpenDue,
        totalBilledAmount: summary.totalBilledAmount,
        totalSettledAmount: summary.totalSettledAmount
      }
    });
  } catch (error) {
    console.error("Get customer due report error:", error);
    return res.status(500).json({
      success: false,
      message: "Customer due report fetch failed",
      error: error.message
    });
  }
});

app.get("/transaction/reports/metal-ledger", async (req, res) => {
  try {
    const access = await resolveAccessContext(req, {
      requireActingUser: true,
      requireCompanyScope: false,
      allowSuperAdminAll: true
    });

    if (!access.ok) {
      return sendAccessError(res, access);
    }

    const companyId = access.companyScope;
    const metalType = normalizeMetalType(req.query.metalType || req.query.metal_type);
    const fromDate = String(req.query.fromDate || req.query.from_date || "").trim();
    const toDate = String(req.query.toDate || req.query.to_date || "").trim();
    const partyId = Number(req.query.partyId || req.query.party_id || 0);
    const partyType = normalizePartyType(req.query.partyType || req.query.party_type);
    const transactionType = normalizeTransactionType(req.query.transactionType || req.query.transaction_type);
    const purity = String(req.query.purity || "").trim();
    const lotNo = String(req.query.lotNo || req.query.lot_no || "").trim();
    const processLotNo = String(req.query.processLotNo || req.query.process_lot_no || "").trim();

    if (!metalType) {
      return res.status(400).json({
        success: false,
        message: "metal_type is required"
      });
    }

    const params = [];
    const whereParts = ["ml.metal_type = ?"];
    params.push(metalType);

    if (companyId !== null) {
      whereParts.push("ml.company_id = ?");
      params.push(companyId);
    }

    if (fromDate) {
      whereParts.push("ml.entry_date >= ?");
      params.push(fromDate);
    }

    if (toDate) {
      whereParts.push("ml.entry_date <= ?");
      params.push(toDate);
    }

    if (partyId) {
      whereParts.push("ml.party_id = ?");
      params.push(partyId);
    }

    if (partyType) {
      whereParts.push("pm.party_type = ?");
      params.push(partyType);
    }

    if (transactionType) {
      whereParts.push("tm.transaction_type = ?");
      params.push(transactionType);
    }

    if (purity) {
      whereParts.push("CAST(ml.purity AS CHAR) LIKE ?");
      params.push(`${purity}%`);
    }

    if (lotNo) {
      whereParts.push("tm.lot_no LIKE ?");
      params.push(`%${lotNo}%`);
    }

    if (processLotNo) {
      whereParts.push("tm.process_lot_no LIKE ?");
      params.push(`%${processLotNo}%`);
    }

    const [rows] = await pool.query(
      `
      SELECT
        ml.id,
        ml.entry_date,
        ml.purity,
        ml.gross_in,
        ml.gross_out,
        ml.fine_in,
        ml.fine_out,
        ml.remarks,
        pm.party_name,
        pm.party_type,
        tm.voucher_no,
        tm.transaction_type,
        tm.reference_no,
        tm.invoice_no,
        tm.lot_no,
        tm.process_lot_no
      FROM metal_ledger ml
      LEFT JOIN party_master pm ON pm.id = ml.party_id
      LEFT JOIN transaction_master tm ON tm.id = ml.transaction_id
      WHERE ${whereParts.join(" AND ")}
      ORDER BY ml.entry_date DESC, ml.id DESC
      `,
      params
    );

    const normalizedRows = rows.map((row) => ({
      ...row,
      gross_in: toNumber(row.gross_in),
      gross_out: toNumber(row.gross_out),
      fine_in: toNumber(row.fine_in),
      fine_out: toNumber(row.fine_out),
      purity: toNumber(row.purity)
    }));

    const activePartySet = new Set();
    const summary = normalizedRows.reduce(
      (acc, row) => {
        acc.totalIn += row.gross_in;
        acc.totalOut += row.gross_out;
        if (row.party_name) {
          activePartySet.add(String(row.party_name));
        }
        return acc;
      },
      {
        totalIn: 0,
        totalOut: 0
      }
    );

    return res.json({
      success: true,
      rows: normalizedRows,
      summary: {
        metalType,
        totalIn: summary.totalIn,
        totalOut: summary.totalOut,
        netBalance: summary.totalIn - summary.totalOut,
        activePartiesCount: activePartySet.size
      }
    });
  } catch (error) {
    console.error("Get metal ledger report error:", error);
    return res.status(500).json({
      success: false,
      message: "Metal ledger report fetch failed",
      error: error.message
    });
  }
});

/* =========================
   404
========================= */
app.use((req, res) => {
  return res.status(404).json({
    success: false,
    message: "Route not found"
  });
});

/* =========================
   GLOBAL ERROR HANDLER
========================= */
app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  return res.status(500).json({
    success: false,
    message: "Internal server error",
    error: err.message
  });
});

/* =========================
   START SERVER
========================= */
app.listen(PORT, "0.0.0.0", async () => {
  console.log(`Server running on port ${PORT}`);

  try {
    await testDbConnection();
    console.log("MySQL Connected");
    await ensureSchema();
    await ensureSuperAdminExists();
  } catch (error) {
    console.error("MySQL connection failed:", error);
  }
});

