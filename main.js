// CSV processing: hook up the "Process CSV" button
document.getElementById("processCSV").addEventListener("click", function () {
  const fileInput = document.getElementById("csvFile");
  if (fileInput.files.length === 0) {
    alert("Please select a CSV file");
    return;
  }
  const file = fileInput.files[0];
  Papa.parse(file, {
    header: true,
    dynamicTyping: false,
    complete: function (results) {
      console.log(results.data);
      const dataDictionary = generateDataDictionary(results.data);
      document.getElementById("dataDictionary").textContent = JSON.stringify(dataDictionary, null, 2);
    },
    error: function (error) {
      console.error("Error parsing CSV:", error);
    },
  });
});

// Data dictionary generator: analyze each CSV column to determine a type and an example value.
// (This is a simple approximation inspired by your Python logic.)
function generateDataDictionary(data) {
  if (data.length === 0) return {};
  const headers = Object.keys(data[0]);
  let dictionary = [];
  headers.forEach((header) => {
    let typeCounts = { text: 0, int: 0, float: 0, bool: 0, datetime: 0, date: 0 };
    let example = null;
    data.forEach((row) => {
      const value = row[header];
      if (value !== "" && value != null && example === null) {
        example = value;
      }
      let detectedType = detectType(value);
      // Increment the detected type
      typeCounts[detectedType] = (typeCounts[detectedType] || 0) + 1;
    });
    let chosenType = chooseType(typeCounts);
    dictionary.push({
      column: header,
      type: chosenType,
      example: example,
      description: "",
    });
  });
  return dictionary;
}

// A basic type detection by testing with regular expressions and JavaScript Date parsing.
function detectType(value) {
  if (value === "" || value == null || (typeof value === "string" && value.trim() === "") || value === "NA" || value === "NULL") {
    return "text"; // treat empty and NA-like values as "text" by default
  }
  // Boolean detection (very basic)
  if (["true", "false", "True", "False", "0", "1"].includes(value)) {
    return "bool";
  }
  // Integer detection: a string of digits possibly preceded by a minus sign
  if (/^-?\d+$/.test(value)) {
    return "int";
  }
  // Float detection: numbers with a decimal point
  if (/^-?\d*\.\d+$/.test(value)) {
    return "float";
  }
  // Date/datetime detection: try to parse with the Date object.
  const dateObj = new Date(value);
  if (!isNaN(dateObj.getTime())) {
    // If the time part is 00:00:00 then treat it as a date.
    if (
      dateObj.getHours() === 0 &&
      dateObj.getMinutes() === 0 &&
      dateObj.getSeconds() === 0
    ) {
      return "date";
    }
    return "datetime";
  }
  // Default to text.
  return "text";
}

// A simple chooser that picks the type with the highest count.
function chooseType(typeCounts) {
  let max = 0;
  let selected = "text";
  for (const [type, count] of Object.entries(typeCounts)) {
    if (count > max) {
      max = count;
      selected = type;
    }
  }
  return selected;
}

// CKAN API interaction: handle the submission of the CKAN parameters form.
document.getElementById("ckanForm").addEventListener("submit", async function (event) {
  event.preventDefault();

  const siteUrl = document.getElementById("siteUrl").value;
  const apiKey = document.getElementById("apiKey").value;

  // For demonstration, we’ll call the 'package_list' endpoint.
  try {
    const response = await fetch(`${siteUrl}/api/3/action/package_list`, {
      headers: {
        Authorization: apiKey,
      },
    });
    const data = await response.json();
    document.getElementById("ckanResponse").textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    document.getElementById("ckanResponse").textContent = "Error: " + error;
  }
});

import {
  compareSchemas,
  cloneDataDictionary,
  getFields,
} from "./ckan.js";

// compare
document.getElementById("btnCompare").addEventListener("click", async () => {
  const site = document.getElementById("siteUrl").value;
  const key  = document.getElementById("apiKey").value;
  const src  = document.getElementById("srcRes").value.trim();
  const dst  = document.getElementById("dstRes").value.trim();
  try {
    const { sameNames, typeMismatches } = await compareSchemas(site, src, dst, key);
    document.getElementById("cmpOut").textContent =
      sameNames && !typeMismatches.length
        ? "✔ Schemas match perfectly"
        : `⚠ Differences:\n${typeMismatches.join("\n") || "Field lists differ"}`;
  } catch (e) {
    document.getElementById("cmpOut").textContent = "Error: " + e.message;
  }
});

// clone
document.getElementById("btnClone").addEventListener("click", async () => {
  const site = document.getElementById("siteUrl").value;
  const key  = document.getElementById("apiKey").value;
  const src  = document.getElementById("cloneSrc").value.trim();
  const dst  = document.getElementById("cloneDst").value.trim();
  try {
    const res = await cloneDataDictionary(site, src, dst, key);
    document.getElementById("cloneOut").textContent =
      "Done. Fields now:\n" + JSON.stringify(res.fields, null, 2);
  } catch (e) {
    document.getElementById("cloneOut").textContent = "Error: " + e.message;
  }
});


let deferredPrompt;
const installBtn = document.getElementById("installPWA");

window.addEventListener("beforeinstallprompt", e => {
  e.preventDefault();
  deferredPrompt = e;
  installBtn.style.display = "inline-block";
});

installBtn.addEventListener("click", async () => {
  if (!deferredPrompt) return;
  deferredPrompt.prompt();
  const { outcome } = await deferredPrompt.userChoice;
  console.log("PWA install:", outcome);
  installBtn.style.display = "none";
  deferredPrompt = null;
});


const statusBadge = document.getElementById("ckanStatus");

function showStatus(ok, msg) {
  statusBadge.textContent = msg;
  statusBadge.className = ok ? "badge ok" : "badge error";
}

// in your CKAN test-call listener (replace the old output logic)
try {
  const data = await response.json();
  if (data.success) {
    showStatus(true, "✓ connected to CKAN 2.10");
    document.getElementById("ckanResponse")
            .textContent = JSON.stringify(data.result, null, 2);
  } else {
    throw new Error(data.error?.message || "Unknown CKAN error");
  }
} catch (err) {
  showStatus(false, err.message);
}

const dropZone = document.getElementById("dropZone");
const fileInput = document.getElementById("csvFile");

["dragenter", "dragover"].forEach(evt =>
  dropZone.addEventListener(evt, e => {
    e.preventDefault(); e.dataTransfer.dropEffect = "copy";
    dropZone.classList.add("dragover");
  })
);
["dragleave", "drop"].forEach(evt =>
  dropZone.addEventListener(evt, () => dropZone.classList.remove("dragover"))
);

dropZone.addEventListener("drop", e => {
  e.preventDefault();
  const file = e.dataTransfer.files[0];
  if (file && file.name.endsWith(".csv")) {
    fileInput.files = e.dataTransfer.files;          // delegate to existing logic
  } else {
    alert("Please drop a CSV file.");
  }
});

// existing “Process CSV” button keeps working unmodified

// ---------- CSV helper ----------
function fieldsToCSV(fields) {
  const cols = ["id", "type", "label", "description", "example"];
  const header = cols.join(",") + "\n";
  const rows = fields
    .filter(f => f.id !== "_id")
    .map(f => {
      const info = f.info || {};
      return [
        f.id,
        f.type,
        (info.label || "").replace(/"/g,'""'),
        (info.notes || "").replace(/"/g,'""'),
        info.example ? String(info.example).replace(/"/g,'""') : ""
      ]
        .map(v => `"${v}"`)          // simple CSV quoting
        .join(",");
    })
    .join("\n");
  return header + rows;
}

// ---------- download listener ----------
import { getDataDictionary } from "./ckan.js";

document
  .getElementById("btnDownloadDD")
  .addEventListener("click", async () => {
    const site = document.getElementById("siteUrl").value;
    const resId = document.getElementById("ddRes").value.trim();
    const msg   = document.getElementById("ddMsg");
    msg.textContent = "Fetching…";
    try {
      const fields = await getDataDictionary(site, resId, "");
      if (!fields) throw new Error("No data dictionary found.");
      const csv = fieldsToCSV(fields);
      const blob = new Blob([csv], { type: "text/csv" });
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement("a");
      a.href = url;
      a.download = `${resId}-data-dictionary.csv`;
      a.click();
      URL.revokeObjectURL(url);
      msg.textContent = "✔ downloaded";
    } catch (e) {
      msg.textContent = "Error: " + e.message;
    }
  });



