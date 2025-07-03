const textarea = document.getElementById("sparqlQuery");
const results = document.getElementById("results");

const tableData = [[[table_rows_js_array]]];
const tableHeaders = [[[table_headers_js_array]]];
const rowsPerPage = 50;
let currentPage = 0;

function renderTable() {
  const tableHeader = document.getElementById("tableHeader");
  const tableBody = document.getElementById("tableBody");
  const pageInfo = document.getElementById("pageInfo");

  tableHeader.innerHTML = "";
  for (const h of tableHeaders) {
    const th = document.createElement("th");
    th.textContent = h;
    tableHeader.appendChild(th);
  }

  tableBody.innerHTML = "";
  const start = currentPage * rowsPerPage;
  const end = Math.min(start + rowsPerPage, tableData.length);

  for (let i = start; i < end; i++) {
    const row = tableData[i];
    const tr = document.createElement("tr");

    for (let cell of row) {
      const td = document.createElement("td");

      if (cell.startsWith("<") && cell.endsWith(">")) {
        const uri = cell.substring(1, cell.length - 1);
        const a = document.createElement("a");
        a.href = `/entity/<${uri}>`;
        a.textContent = `<${uri}>`;
        a.target = "_blank";
        td.appendChild(a);
      } else {
        td.textContent = cell;
      }
      tr.appendChild(td);
    }
    tableBody.appendChild(tr);
  }

  pageInfo.textContent = `Page ${currentPage + 1} of ${Math.ceil(
    tableData.length / rowsPerPage
  )}`;
}

//RESULTS NAVIGATION
document.getElementById("prevBtn").addEventListener("click", () => {
  if (currentPage > 0) {
    currentPage--;
    renderTable();
  }
});
document.getElementById("nextBtn").addEventListener("click", () => {
  if ((currentPage + 1) * rowsPerPage < tableData.length) {
    currentPage++;
    renderTable();
  }
});

//TEXTBOX
textarea.addEventListener("keydown", function (e) {
  if (e.key === "Tab") {
    e.preventDefault();
    const start = this.selectionStart;
    const end = this.selectionEnd;
    this.value =
      this.value.substring(0, start) + "\t" + this.value.substring(end);
    this.selectionStart = this.selectionEnd = start + 1;
  }
});

document.getElementById("queryForm").addEventListener("submit", (e) => {
  e.preventDefault();
  const encodedQuery = encodeURIComponent(textarea.value);
  const encodedMode = encodeURIComponent(modeInput.value);
  const baseUrl = window.location.origin + window.location.pathname;
  window.location.href = `${baseUrl}?query=${encodedQuery}&mode=${encodedMode}`;
});

//MODE CONTROL
const modeSwitch = document.getElementById("modeSwitch");
const modeInput = document.getElementById("modeInput");
const modeLabel = document.getElementById("modeLabel");

modeSwitch.addEventListener("change", () => {
  if (modeSwitch.checked) {
    modeInput.value = "update";
    modeLabel.textContent = "Update Mode";
  } else {
    modeInput.value = "query";
    modeLabel.textContent = "Query Mode";
  }
});

//Checkbox logic
function updateDownloadButtonClass() {
  const checkboxes = document.querySelectorAll(
    '#queryHistoryList input[type="checkbox"]'
  );
  const downloadBtn = document.getElementById("download");

  // Check if any checkbox is checked
  const anyChecked = Array.from(checkboxes).some((cb) => cb.checked);

  // Toggle classes
  if (anyChecked) {
    downloadBtn.classList.remove("btn-secondary");
    downloadBtn.classList.add("btn-success");
  } else {
    downloadBtn.classList.remove("btn-success");
    downloadBtn.classList.add("btn-secondary");
  }
}

function addCheckboxListeners() {
  const checkboxes = document.querySelectorAll(
    '#queryHistoryList input[type="checkbox"]'
  );
  checkboxes.forEach((cb) => {
    cb.addEventListener("change", updateDownloadButtonClass);
  });
}

//LOCAL STORAGE
const STORAGE_KEY = "sparql_query_history";

function saveQueryIfSuccess() {
  const successAlert = document.querySelector(".alert-success");
  if (!successAlert) return;

  const currentQuery = textarea.value.trim();
  if (!currentQuery) return;

  let history = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  history = history.filter((item) => item.query !== currentQuery);
  history.unshift({
    query: currentQuery,
    timestamp: Date.now(),
    mode: modeInput.value,
  });

  localStorage.setItem(STORAGE_KEY, JSON.stringify(history));
}

function renderQueryHistory() {
  const container = document.getElementById("queryHistoryList");
  let history = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");

  container.innerHTML = "";

  if (history.length === 0) {
    container.innerHTML = "<p>No previous successful queries.</p>";
    return;
  }
  for (const [index, item] of history.entries()) {
    const div = document.createElement("div");
    div.className =
      "mb-3 border rounded p-2 bg-grey d-flex align-items-start gap-2";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.style = "width: 20; height: 20";
    checkbox.className = "form-check-input mt-1";
    checkbox.dataset.index = index;

    const preview = item.query.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    const mode = item.mode;
    const date = new Date(item.timestamp);
    const dateString = date.toLocaleString();

    const queryContent = document.createElement("div");

    queryContent.className = "flex-grow-1";
    queryContent.style = " width: 80%;";
    queryContent.innerHTML = `
      <div style="font-family: monospace; font-size: 0.85rem; white-space: pre-wrap;">${preview}</div>
      <small class="text-muted">${dateString}</small>
      <div class="mt-1 d-flex justify-content-end gap-2">
        <button class="btn btn-sm btn-primary btn-run">Use</button>
        <button class="btn btn-sm btn-danger btn-delete">Delete</button>
      </div>
    `;

    queryContent.querySelector(".btn-run").addEventListener("click", () => {
      textarea.value = item.query;
      modeInput.value = mode;
      modeSwitch.checked = mode === "update";
      modeLabel.textContent = mode === "update" ? "Update Mode" : "Query Mode";
    });

    queryContent.querySelector(".btn-delete").addEventListener("click", () => {
      history.splice(index, 1);
      localStorage.setItem(STORAGE_KEY, JSON.stringify(history));
      renderQueryHistory();
    });

    div.appendChild(checkbox);
    div.appendChild(queryContent);
    container.appendChild(div);
  }
  addCheckboxListeners();
}

document.getElementById("download").addEventListener("click", () => {
  const history = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  const checkboxes = document.querySelectorAll(
    '#queryHistoryList input[type="checkbox"]'
  );
  const selectedQueries = [];

  checkboxes.forEach((checkbox) => {
    if (checkbox.checked) {
      const index = parseInt(checkbox.dataset.index, 10);
      const item = history[index];
      selectedQueries.push(`###${item.mode}###\n${item.query.trim()}`);
    }
  });

  if (selectedQueries.length === 0) {
    alert("Please select at least one query to download.");
    return;
  }

  const blob = new Blob([selectedQueries.reverse().join("\n\n")], {
    type: "text/sparql",
  });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = "queries.sparql";
  a.click();

  URL.revokeObjectURL(url);
});

document.getElementById("importQueriesBtn").addEventListener("click", () => {
  document.getElementById("importQueriesInput").click();
});

document
  .getElementById("importQueriesInput")
  .addEventListener("change", (event) => {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      const content = e.target.result;
      const parts = content
        .split(/###(query|update)###/i)
        .filter((part) => part.trim() !== "");

      if (parts.length % 2 !== 0) {
        alert(
          "Malformed input file. Every query must be preceded by ###mode###."
        );
        return;
      }

      let history = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
      const now = Date.now();

      for (let i = 0; i < parts.length; i += 2) {
        const mode = parts[i].trim().toLowerCase();
        const query = parts[i + 1].trim();

        if (
          !history.some((item) => item.query === query && item.mode === mode)
        ) {
          history.unshift({
            query,
            mode,
            timestamp: now + i,
          });
        }
      }

      localStorage.setItem(STORAGE_KEY, JSON.stringify(history));
      renderQueryHistory();
    };

    reader.readAsText(file);
  });

window.addEventListener("DOMContentLoaded", () => {
  const params = new URLSearchParams(window.location.search);
  const query = params.get("query");
  const mode = params.get("mode") || "query";

  if (mode === "update") {
    results.style = "display: none;";
  }

  textarea.value =
    query && query.trim() !== ""
      ? query
      : `SELECT * WHERE {
?s ?p ?o
}`;

  if (mode === "update") {
    modeSwitch.checked = true;
    modeInput.value = "update";
    modeLabel.textContent = "Update Mode";
  }

  if (tableData.length > 0) renderTable();

  saveQueryIfSuccess();
  renderQueryHistory();
});

///AI TERRITORY
async function generateSPARQLQuery(input, apiKey = "[[api_key]]") {
  const modelId = "gemini-2.0-flash-lite";
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${modelId}:streamGenerateContent?key=${apiKey}`;

  const body = {
    contents: [
      {
        role: "user",
        parts: [{ text: input }],
      },
    ],
    generationConfig: {
      responseMimeType: "text/plain",
    },
    systemInstruction: {
      role: "system",
      parts: [
        {
          text: `Purpose and Goals:
  
  * Act as an expert in writing SPARQL queries.
  * Understand natural language descriptions of data retrieval needs.
  * Correct and complete incomplete or erroneous SPARQL queries.
  * Provide only the functional SPARQL query as a response, without any additional text, explanations, or conversational elements.
  
  Behaviors and Rules:
  
  1) Input Interpretation:
  a) Analyze the user's request, whether it's a natural language description or a partial/incorrect query, to ascertain the exact data requirements.
  b) Identify the entities, properties, and relationships implied in the user's request for constructing the query.
  c) Determine the target ontology or dataset if not explicitly stated, or assume a general knowledge graph context if no specific one is provided.
  
  2) Query Generation/Correction:
  a) If the input is a description, construct a complete and syntactically correct SPARQL query that precisely fulfills the described data retrieval.
  b) If the input is an incomplete or incorrect query, identify the errors (syntax, logic, missing clauses) and provide the fully corrected and functional query.
  c) Ensure the generated query adheres to best practices for SPARQL, including proper use of prefixes, variables, filters, and graph patterns.
  d) Prioritize conciseness and efficiency in the generated query where possible, without compromising correctness.
  e) Return a full request, define all of the prefixes that are going to be used 
  f) The datasets are mostly annotated with schema.org predicates and classes
  
  3) Output Format:
  a) The response MUST contain ONLY the working SPARQL query.
  b) DO NOT include any introductory phrases like 'Here is the query:' or concluding remarks.
  c) DO NOT include any explanations, comments, or conversational text.
  d) The query should be presented as plain text, ready for direct execution.
  
  Overall Tone:
  * Strictly objective and technical.
  * Direct and precise in its output.
  * Unfailingly accurate in query generation.`,
        },
      ],
    },
  };

  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `HTTP error! Status: ${response.status}, Body: ${errorText}`
    );
  }

  const responseText = await response.text();

  // Parse the full response as JSON array (not streaming lines)
  const jsonArray = JSON.parse(responseText);

  let result = "";

  for (const item of jsonArray) {
    const parts = item.candidates?.[0]?.content?.parts;
    if (parts) {
      for (const part of parts) {
        result += part.text;
      }
    }
  }

  // Clean up the triple backticks if any (optional)
  return result
    .trim()
    .replace(/^```sparql\s*/i, "")
    .replace(/```$/, "")
    .trim();
}

document
  .getElementById("ai_helper")
  .addEventListener("click", async (event) => {
    console.log(textarea.value);
    textarea.value = await generateSPARQLQuery(textarea.value);
  });
