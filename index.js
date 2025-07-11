// Import required packages
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const axios = require("axios");
const { parseStringPromise } = require("xml2js");
const xlsx = require("xlsx");
const JSZip = require('jszip');
const { XMLBuilder } = require('fast-xml-parser');

const app = express();
app.use(cors());
app.use(bodyParser.json());
const PORT = process.env.PORT || 3000;
let results = [];
let createdFields = [];

/**
 * Escape special XML characters to prevent XML injection issues.
 */
function escapeXml(unsafe) {
  if (!unsafe) return "";
  return unsafe.toString().replace(/[<>&'"]/g, (c) => {
    switch (c) {
      case "<": return "&lt;";
      case ">": return "&gt;";
      case "&": return "&amp;";
      case "'": return "&apos;";
      case '"': return "&quot;";
      default: return c;
    }
  });
}

// Function to process all batches in parallel
async function createFieldsInBatchesParallel(baseUrl, sessionId, fields, isVisible) {
  const chunkSize = 10; // Salesforce limit
  const fieldChunks = chunkArray(fields, chunkSize); // Split the fields into batches

  const batchPromises = fieldChunks.map(chunk => processBatch(baseUrl, sessionId, chunk, isVisible));

  // Wait for all batches to complete in parallel
  const allResults = await Promise.all(batchPromises);

  // Flatten the results array (since each batch returns an array of results)
  return allResults.flat();
}

// Function to process field creation and permission assignments for a single batch
async function processBatch(baseUrl, sessionId, chunk, isVisible) {
  const results = [];
  const createdFields = [];

  try {
    console.log(`Processing batch of ${chunk.length} fields...`);
    const createResults = await createSalesforceFieldsBulk(baseUrl, sessionId, chunk);
    
    // Process creation results for the current batch
    for (let i = 0; i < createResults.length; i++) {
      const result = createResults[i];
      const field = chunk[i];
      
      if (result.success) {
        createdFields.push(`${field.objectName}.${field.fieldApiName}`);
        results.push({
          field: field.fieldApiName,
          success: true,
          response: result.response
        });
      } else {
        results.push({
          field: field.fieldApiName,
          success: false,
          error: result.error
        });
      }
    }

    // Assign permissions in bulk if requested and we have successful creations
    if (isVisible && createdFields.length > 0) {
      const permissionResults = await assignFieldVisibilityBulk(baseUrl, sessionId, createdFields);
      
      // Merge permission results with creation results
      for (const result of results) {
        if (result.success) {
          const fullFieldName = `${result.field.objectName}.${result.field.fieldApiName}`;
          result.visibility = permissionResults[fullFieldName] || [];
        }
      }
    }
  } catch (error) {
    console.error('Error processing batch:', error);
  }

  return results;
}

/**
 * POST /create-fields-excel - Create fields by parsing Excel data.
 */
app.post("/create-fields-excel", async (req, res) => {
  try {
    const { sessionId, baseUrl, objectName, file, isVisible } = req.body;
    console.log('isVisible', isVisible);

    if (!sessionId || !/^00D\w{12,15}\!.*$/.test(sessionId)) {
      return res.status(400).json({ error: "Invalid Salesforce session ID" });
    }

    if (!baseUrl || !file) {
      return res.status(400).json({ error: "Missing baseUrl or file data" });
    }

    if (typeof objectName !== "string") {
      return res.status(400).json({ error: "Invalid object name. Must be a string." });
    }

    const buffer = Buffer.from(file.split(",")[1], "base64");
    const workbook = xlsx.read(buffer, { type: "buffer" });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(sheet, { header: 1 });

    if (rows.length < 2) {
      return res.status(400).json({ error: "Excel file must have at least one row of data" });
    }

    const fields = [];
    const results = [];

    for (let i = 1; i < rows.length; i++) {
      const [label, type, picklistCsv] = rows[i];
      if (!label || !type) {
        results.push({
          field: `Row ${i + 1}`,
          success: false,
          error: "Missing label or type",
        });
        continue;
      }

      const fieldApiName = (
        label
          .trim()
          .replace(/[^a-zA-Z0-9]/g, "_")
          .replace(/_+/g, "_")
          .replace(/^_+|_+$/g, "")
          .slice(0, 40) + "__c"
      ).replace(/__+c$/, "__c");

      fields.push({
        objectName,
        fieldApiName,
        label,
        type,
        picklistValues: (type === "Picklist" || type === "MultiselectPicklist") ? picklistCsv?.split(",").map(v => v.trim()) : undefined,
      });
    }

    // If no valid fields, stop
    if (fields.length === 0) {
      return res.status(400).json({ error: "No valid fields found in Excel" });
    }

    // Use this function where you want to call the batch creation logic
const finalResults = await createFieldsInBatchesParallel(baseUrl, sessionId, fields, isVisible);

// Send the final response
res.json({ results: finalResults });
  } catch (error) {
    console.error("Excel upload error:", error);
    res.status(500).json({ error: "Failed to process Excel data", message: error.message });
  }
});


app.post("/create-fields", async (req, res) => {
  try {
    const { sessionId, baseUrl, fields, isVisible } = req.body;
    console.log('isVisible::', isVisible);

    if (!sessionId || !/^00D\w{12,15}\!.*$/.test(sessionId)) {
      return res.status(400).json({ error: "Invalid Salesforce session ID" });
    }

    if (!baseUrl || !Array.isArray(fields) || fields.length === 0) {
      return res.status(400).json({ error: "Missing baseUrl or fields" });
    }

    // Use this function where you want to call the batch creation logic
const finalResults = await createFieldsInBatchesParallel(baseUrl, sessionId, fields, isVisible);

// Send the final response
res.json({ results: finalResults });
  } catch (error) {
    console.error("Server error:", error);
    res.status(500).json({
      error: "Internal server error",
      message: error.message,
    });
  }
});

// Helper function to chunk the fields array into smaller arrays of size chunkSize
function chunkArray(array, chunkSize) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

// Bulk field creation
async function createSalesforceFieldsBulk(baseUrl, sessionId, fields) {
  const apiUrl = `${baseUrl.replace(/\/+$/, "")}/services/Soap/m/59.0`;

  const normalizeType = (type) => {
    const map = {
      "Date / Time": "DateTime",
      "Picklist (Multi-Select)": "MultiselectPicklist",
      "Text Area": "TextArea",
      "Text Area (Long)": "LongTextArea",
      "Text Area (Rich)": "Html"
    };
    return map[type] || type;
  };

  const fieldXmls = fields.map(field => {
    const type = normalizeType(field.type || "Text");

    let fieldXml = `
      <met:fullName>${escapeXml(field.objectName)}.${escapeXml(field.fieldApiName)}</met:fullName>
      <met:label>${escapeXml(field.label)}</met:label>
      <met:type>${escapeXml(type)}</met:type>
    `;

    switch (type) {
      case "Text":
        fieldXml += `<met:length>${field.length || 255}</met:length>`;
        break;
    
      case "Number":
      case "Percent":
      case "Currency":
        fieldXml += `
          <met:precision>${field.precision || 18}</met:precision>
          <met:scale>${field.scale || 0}</met:scale>
        `;
        break;
    
      case "Checkbox":
        fieldXml += `<met:defaultValue>${field.defaultValue || "false"}</met:defaultValue>`;
        break;
    
      case "Picklist":
      case "MultiselectPicklist":
        if (Array.isArray(field.picklistValues) && field.picklistValues.length) {
          fieldXml += `
            <met:valueSet>
              <met:valueSetDefinition>
                ${field.picklistValues.map(
                  v => `
                    <met:value>
                      <met:fullName>${escapeXml(v)}</met:fullName>
                      <met:default>false</met:default>
                    </met:value>
                  `
                ).join("")}
                <met:sorted>false</met:sorted>
              </met:valueSetDefinition>
              <met:restricted>true</met:restricted>
            </met:valueSet>
          `;
        }
        if (type === "MultiselectPicklist") {
          fieldXml += `<met:visibleLines>${field.visibleLines || 4}</met:visibleLines>`;
        }
        break;
    
      case "LongTextArea":
        fieldXml += `
          <met:visibleLines>${field.visibleLines || 3}</met:visibleLines>
          <met:length>${field.length || 32768}</met:length>
        `;
        break;
    
      case "Html":
        fieldXml += `
          <met:visibleLines>${field.visibleLines || 10}</met:visibleLines>
          <met:length>${field.length || 32768}</met:length>
        `;
        break;
    
      // These types typically don't require extra attributes
      case "Date":
      case "DateTime":
      case "Email":
      case "Phone":
        break;
    }
    

    return `<met:metadata xsi:type="met:CustomField" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      ${fieldXml}
    </met:metadata>`;
  });

  const soapEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:met="http://soap.sforce.com/2006/04/metadata">
  <soapenv:Header>
    <met:SessionHeader>
      <met:sessionId>${sessionId}</met:sessionId>
    </met:SessionHeader>
  </soapenv:Header>
  <soapenv:Body>
    <met:createMetadata>
      ${fieldXmls.join("")}
    </met:createMetadata>
  </soapenv:Body>
</soapenv:Envelope>`;

  try {
    const response = await axios.post(apiUrl, soapEnvelope, {
      headers: {
        "Content-Type": "text/xml; charset=UTF-8",
        SOAPAction: '""',
        Accept: "text/xml",
      },
      timeout: 30000,
    });

    const parsed = await parseStringPromise(response.data);
    const results = parsed["soapenv:Envelope"]["soapenv:Body"][0]["createMetadataResponse"][0]["result"];

    return results.map((result, index) => {
      if (result.success && result.success[0] === "true") {
        return {
          success: true,
          response: result
        };
      } else {
        return {
          success: false,
          error: result.errors?.[0]?.message?.[0] || "Unknown Salesforce error"
        };
      }
    });
  } catch (error) {
    console.warn("Bulk field creation failed, falling back to individual creations", error);
    return await createFieldsIndividually(baseUrl, sessionId, fields);
  }
}

async function createSalesforceField(baseUrl, sessionId, field) {
  const apiUrl = `${baseUrl.replace(/\/+$/, "")}/services/Soap/m/59.0`;

  const type = (function normalizeType(type) {
    const map = {
      "Date / Time": "DateTime",
      "Picklist (Multi-Select)": "MultiselectPicklist",
      "Text Area": "TextArea",
      "Text Area (Long)": "LongTextArea",
      "Text Area (Rich)": "Html"
    };
    return map[type] || type;
  })(field.type || "Text");

  let fieldXml = `
    <met:fullName>${escapeXml(field.objectName)}.${escapeXml(field.fieldApiName)}</met:fullName>
    <met:label>${escapeXml(field.label)}</met:label>
    <met:type>${escapeXml(type)}</met:type>
  `;

  switch (type) {
    case "Text":
      fieldXml += `<met:length>${field.length || 255}</met:length>`;
      break;
    case "Number":
    case "Percent":
    case "Currency":
      fieldXml += `<met:precision>${field.precision || 18}</met:precision><met:scale>${field.scale || 0}</met:scale>`;
      break;
    case "Checkbox":
      fieldXml += `<met:defaultValue>${field.defaultValue || "false"}</met:defaultValue>`;
      break;
    case "Picklist":
    case "MultiselectPicklist":
      if (Array.isArray(field.picklistValues) && field.picklistValues.length) {
        fieldXml += `
          <met:valueSet>
            <met:valueSetDefinition>
              ${field.picklistValues.map(v => `
                <met:value>
                  <met:fullName>${escapeXml(v)}</met:fullName>
                  <met:default>false</met:default>
                </met:value>
              `).join('')}
              <met:sorted>false</met:sorted>
            </met:valueSetDefinition>
            <met:restricted>true</met:restricted>
          </met:valueSet>
        `;
      }
      if (type === "MultiselectPicklist") {
        fieldXml += `<met:visibleLines>${field.visibleLines || 4}</met:visibleLines>`;
      }
      break;
    case "LongTextArea":
      fieldXml += `<met:visibleLines>${field.visibleLines || 3}</met:visibleLines><met:length>${field.length || 32768}</met:length>`;
      break;
    case "Html":
      fieldXml += `<met:visibleLines>${field.visibleLines || 10}</met:visibleLines><met:length>${field.length || 32768}</met:length>`;
      break;
    
  }

  const soapEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:met="http://soap.sforce.com/2006/04/metadata">
  <soapenv:Header>
    <met:SessionHeader>
      <met:sessionId>${sessionId}</met:sessionId>
    </met:SessionHeader>
  </soapenv:Header>
  <soapenv:Body>
    <met:createMetadata>
      <met:metadata xsi:type="met:CustomField" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        ${fieldXml}
      </met:metadata>
    </met:createMetadata>
  </soapenv:Body>
</soapenv:Envelope>`;

  return axios.post(apiUrl, soapEnvelope, {
    headers: {
      "Content-Type": "text/xml; charset=UTF-8",
      SOAPAction: '""',
      Accept: "text/xml",
    },
    timeout: 30000,
  });
}



// Fallback for individual field creation
async function createFieldsIndividually(baseUrl, sessionId, fields) {
  const results = [];
  
  for (const field of fields) {
    try {
      const response = await createSalesforceField(baseUrl, sessionId, field);
      results.push({
        success: true,
        response: response.data
      });
    } catch (error) {
      results.push({
        success: false,
        error: error.message
      });
    }
  }
  
  return results;
}

async function assignFieldVisibilityBulk(baseUrl, sessionId, fieldNames) {
  console.log('[Permission Service] Starting optimized bulk permission assignment');

  try {
    // 1. Retrieve all profiles using the Metadata API
    console.log('[Permission Service] Fetching profile API names...');
    const listMetadataEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:met="http://soap.sforce.com/2006/04/metadata">
  <soapenv:Header>
    <met:SessionHeader>
      <met:sessionId>${sessionId}</met:sessionId>
    </met:SessionHeader>
  </soapenv:Header>
  <soapenv:Body>
    <met:listMetadata>
      <met:queries>
        <met:type>Profile</met:type>
      </met:queries>
      <met:asOfVersion>59.0</met:asOfVersion>
    </met:listMetadata>
  </soapenv:Body>
</soapenv:Envelope>`;

    const listMetadataResponse = await axios.post(
      `${baseUrl}/services/Soap/m/59.0`,
      listMetadataEnvelope,
      {
        headers: {
          'Content-Type': 'text/xml; charset=UTF-8',
          SOAPAction: '""',
        },
        timeout: 30000,
      }
    );

    const parsedListMetadata = await parseStringPromise(listMetadataResponse.data);
    const metadataResults = parsedListMetadata['soapenv:Envelope']['soapenv:Body'][0]['listMetadataResponse'][0]['result'];

    const profiles = metadataResults.map((profile) => ({
      fullName: profile.fullName[0],
    }));

    console.log(`[Permission Service] Found ${profiles.length} profiles`);

    // 2. Prepare the field permission XML chunks
    const fieldPermissionXml = fieldNames.map(fullName => {
      const [obj, field] = fullName.split('.');
      return `
        <met:fieldPermissions>
          <met:field>${obj}.${field}</met:field>
          <met:readable>true</met:readable>
          <met:editable>true</met:editable>
        </met:fieldPermissions>
      `;
    }).join('');

    // 3. Process profiles in parallel batches
    const BATCH_SIZE = 1;
    const apiUrl = `${baseUrl}/services/Soap/m/59.0`;
    const results = {};
    fieldNames.forEach(f => results[f] = []);

    console.log('[Permission Service] Starting parallel batch processing...');
    const profileBatches = chunkArray(profiles, BATCH_SIZE);

    // Process all batches in parallel
    await Promise.all(profileBatches.map(async (batch, index) => {
      console.log(`Starting batch ${index + 1}/${profileBatches.length}`);

      const metadataUpdates = batch.map(profile => `
        <met:metadata xsi:type="met:Profile" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          <met:fullName>${profile.fullName}</met:fullName>
          ${fieldPermissionXml}
        </met:metadata>
      `).join('');

      const updateMetadataEnvelope = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:met="http://soap.sforce.com/2006/04/metadata">
  <soapenv:Header>
    <met:SessionHeader>
      <met:sessionId>${sessionId}</met:sessionId>
    </met:SessionHeader>
  </soapenv:Header>
  <soapenv:Body>
    <met:updateMetadata>
      ${metadataUpdates}
    </met:updateMetadata>
  </soapenv:Body>
</soapenv:Envelope>`;

      try {
        const response = await axios.post(apiUrl, updateMetadataEnvelope, {
          headers: {
            'Content-Type': 'text/xml; charset=UTF-8',
            SOAPAction: '""',
          },
          timeout: 45000,
        });

        const parsed = await parseStringPromise(response.data);
        const responseResults = parsed['soapenv:Envelope']['soapenv:Body'][0]['updateMetadataResponse'][0]['result'];

        batch.forEach((profile, i) => {
          const result = responseResults[i];
          const status = result?.success?.[0] === 'true' ? 'success' : 'failed';

          fieldNames.forEach(fieldName => {
            results[fieldName].push({
              profile: profile.fullName,
              success: status === 'success',
              error: status === 'failed' ? (result.errors?.[0]?.message?.[0] || 'Unknown error') : undefined,
            });
          });

          if (status === 'success') {
            console.log(`✓ ${profile.fullName} (Batch ${index + 1})`);
          } else {
            console.log(`✗ ${profile.fullName} (Batch ${index + 1}): ${result.errors?.[0]?.message?.[0]}`);
          }
        });
      } catch (batchError) {
        console.error(`Batch ${index + 1} failed:`, batchError.message);
        // Mark all profiles in this batch as failed
        batch.forEach(profile => {
          fieldNames.forEach(fieldName => {
            results[fieldName].push({
              profile: profile.fullName,
              success: false,
              error: batchError.message,
            });
          });
        });
      }
    }));

    console.log('[Permission Service] Completed all permission updates');
    return results;
  } catch (error) {
    console.error('[Permission Service] Critical error:', error);
    throw error;
  }
}

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});