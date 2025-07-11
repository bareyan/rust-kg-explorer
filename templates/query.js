const textarea = document.getElementById("sparqlQuery");
const secondary = document.getElementById("secondaryQuery");
secondary.value = "";
const results = document.getElementById("results");
const modeInput = document.getElementById("modeInput");

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
        a.href = `/entity/<${uri.replaceAll("#", "%23")}>`;
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

document.getElementById("download-csv").addEventListener("click", () => {
  const downloadData = [
    tableHeaders.join(";"),
    ...tableData.map((p) => p.join(";")),
  ];

  const blob = new Blob([downloadData.join("\n")], {
    type: "text/csv",
  });

  const durl = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = durl;
  a.download = "result.csv";
  a.click();

  URL.revokeObjectURL(durl);
});
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

secondary.addEventListener("keydown", function (e) {
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
  const encodedQuery = encodeURIComponent(textarea.value).replaceAll(
    "#",
    "%23"
  );
  const secondaryQuery =
    modeInput.value === "advanced"
      ? "&secondary=" +
        encodeURIComponent(secondary.value).replaceAll("#", "%23")
      : "";
  // console.log;
  const encodedMode = encodeURIComponent(modeInput.value);
  const baseUrl = window.location.origin + window.location.pathname;
  window.location.href =
    `${baseUrl}?query=${encodedQuery}&mode=${encodedMode}` + secondaryQuery;
  // console.log(window.location.href);
});

function handleModeChange() {
  if (modeInput.value == "advanced") {
    secondary.style = "";
  } else {
    secondary.style = "display:none";
  }
}

modeInput.addEventListener("change", (e) => handleModeChange());

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

  let currentQuery = textarea.value.trim();
  if (modeInput.value === "advanced") {
    currentQuery = secondary.value.trim() + "\n#\n" + currentQuery;
  }
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
    div.className = "mb-3  rounded p-2 bg-grey d-flex align-items-start gap-2";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.style = "width: 20; height: 20";
    checkbox.className = "form-check-input mt-1";
    checkbox.dataset.index = index;

    let [q1, q2] = item.query.split("\n#\n");

    const preview = q1.replaceAll("<", "&lt;").replaceAll(">", "&gt;");

    let preview2 = "";
    const mode = item.mode;
    if (mode == "advanced" && q2) {
      q2 = q2.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
      preview2 = `<div style="font-family: monospace; font-size: 0.85rem; white-space: pre-wrap; margin-top: 10px; padding: 5px; border-radius: 5px; border:1px solid grey;">${q2}</div>`;
    }
    const date = new Date(item.timestamp);
    const dateString = date.toLocaleString();

    const queryContent = document.createElement("div");

    queryContent.className = "flex-grow-1";
    queryContent.style = " width: 80%;";
    queryContent.innerHTML = `
      <div style="font-family: monospace; font-size: 0.85rem; white-space: pre-wrap; border:1px solid grey;padding: 5px; border-radius: 5px;">${preview}</div>
  ${preview2}
  <small class="text-muted">${dateString}</small>
      <div class="mt-1 d-flex justify-content-end gap-2">

        <button class="btn btn-sm btn-primary btn-run">Use</button>
        <button class="btn btn-sm btn-danger btn-delete">Delete</button>
      </div>
    `;

    queryContent.querySelector(".btn-run").addEventListener("click", () => {
      if (mode === "advanced") {
        let [q1, q2] = item.query.split("\n#\n");
        textarea.value = q2;
        secondary.value = q1;
      } else {
        textarea.value = item.query;
      }
      modeInput.value = mode;

      handleModeChange();
      // modeSwitch.checked = mode === "update";
      // modeLabel.textContent = mode === "update" ? "Update Mode" : "Query Mode";
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
document.getElementById("download").addEventListener("click", async () => {
  const history = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  const checkboxes = document.querySelectorAll(
    '#queryHistoryList input[type="checkbox"]'
  );

  const selected = Array.from(checkboxes)
    .filter((checkbox) => checkbox.checked)
    .map((checkbox) => {
      const index = parseInt(checkbox.dataset.index, 10);
      return history[index];
    });

  if (selected.length === 0) {
    alert("Please select at least one query to download.");
    return;
  }

  const selectedQueries = await Promise.all(
    selected.map(async (item) => {
      const name = await name_query(item.query.trim());
      return `## ${name}\n${item.query.trim()}`;
    })
  );

  const blob = new Blob(
    ["### Exported\n\n" + selectedQueries.reverse().join("\n\n")],
    {
      type: "text/sparql",
    }
  );
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
  const sec = params.get("secondary");
  const mode = params.get("mode") || "query";

  if (mode === "update") {
    results.style = "display: none;";
  }

  textarea.value =
    query && query.trim() !== ""
      ? query.replaceAll("%23", "#")
      : `SELECT * WHERE {
?s ?p ?o
}`;
  secondary.value = sec && sec.trim() !== "" ? sec.replaceAll("%23", "#") : "";
  modeInput.value = mode;
  if (mode == "advanced") {
    secondary.style = "";
  }
  if (tableData.length > 0) renderTable();

  saveQueryIfSuccess();
  renderQueryHistory();
});

///AI TERRITORY
async function generateSPARQLQuery(input) {
  return ai_req(
    input,
    `Purpose and Goals:
  
  * Act as an expert in writing SPARQL queries.
  * Understand natural language descriptions of data retrieval needs.
  * Correct and complete incomplete or erroneous SPARQL queries.
  * Provide only the functional SPARQL query as a response, without any additional text, explanations, or conversational elements.
  * Work only with select, delete, insert queries, no construct queries
  * There are no named graphs
  
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
  * Unfailingly accurate in query generation.`
  );
}

async function name_query(input) {
  return ai_req(
    input,
    `You are an expert SPARQL analyst. Your task is to provide a short, descriptive, one-line name for a given SPARQL procedure.

**Core Logic:**

You will receive one of two types of input:

1.  **Single Query:** A standard SPARQL query (\`SELECT\`, \`INSERT\`, \`DELETE\`, etc.). Your name should summarize its primary function.
2.  **Looping Query (Special Case):** A \`SELECT\` query, followed by \`\\n#\\n\`, followed by an \`UPDATE\` query.

**Handling the Special Case:**

This two-part structure defines a loop:
- The \`SELECT\` query identifies a set of target rows.
- The \`UPDATE\` query is then executed for each row, with placeholders like \`{{variable}}\` being replaced by the values from that row.

For this case, your name **must** describe the **entire, high-level procedure**. Synthesize the "find" action of the \`SELECT\` and the "change" action of the \`UPDATE\` into a single, cohesive description of the overall goal.

**Example (Special Case):**

*   **Query:**
    \`\`\`sparql
    SELECT ?s1 ?s2 WHERE {
        ?s1 <http://schema.org/item> ?o.
        ?s2 <http://schema.org/item> ?o.
        FILTER(STR(?s1) < STR(?s2))
    }
    #
    DELETE { {{s2}} ?p ?o } INSERT { {{s1}} ?p ?o } WHERE  { {{s2}} ?p ?o };
    DELETE { ?sub ?pred {{s2}} } INSERT { ?sub ?pred {{s1}} } WHERE  { ?sub ?pred {{s2}} }
    \`\`\`
*   **Correct Name:** Merge duplicate entities into a single canonical entity.

**Output Format:**
- Your response must be **only the one-line name**.
- Do not add any introductory text, explanations, or quotation marks.`
  );
}

async function ai_req(input, systemInstructions, apiKey = "[[api_key]]") {
  const modelId = "gemini-2.5-flash-lite-preview-06-17";
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
          text: systemInstructions,
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
