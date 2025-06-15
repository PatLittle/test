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