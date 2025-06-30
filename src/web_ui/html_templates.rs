use crate::utils::escape_html;

pub(crate) fn explore_page(data:&str, navigation:&str)->String{

    return format!(
    r#"<html data-bs-theme="dark">
        <head>  
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>KG Explorer</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-LN+7fdVzj6u52u30Kp6M/trliBMCMKTyK833zpbD+pXdCLuTusPj697FH4R/5mcr" crossorigin="anonymous">
        </head>
        <body>
            <div class="container py-5">
                <div class="text-center mb-5">
                    <a href="/" class="text-decoration-none"><h1 class="mb-4 text-center">Entity Explorer</h1></a>
                </div>
    {navigation}
                <div class="row">
{}
                
                 </div>
    {navigation}
            </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/js/bootstrap.bundle.min.js" integrity="sha384-ndDqU0Gzau9qJ1lfW4pNLlhNTkCfHzAVBReH9diLvGRem5+R9g2FzA8ZGN954O5Q" crossorigin="anonymous"></script>
    </body>
    </html>
    "#, data
    );
}
pub(crate) fn query_page(nb_results:usize, table_rows_js_array:&str, table_headers_js_array: &str) -> String{

    format!(r#"
<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SPARQL Query Interface</title>
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
    rel="stylesheet"
  />
  <style>
    td {{ max-width: 300px; overflow-x: auto; word-break: break-word; }}
  </style>
</head>
<body >

<div class="container py-5">
  <a href="/" class="text-decoration-none"><h1 class="mb-4 text-center">SPARQL Query Engine</h1></a>

  <form method="GET" id="queryForm">
    <div class="mb-3">
      <label for="sparqlQuery" class="form-label">Enter SPARQL Query:</label>
      <textarea
        class="form-control"
        id="sparqlQuery"
        name="query"
        rows="10"
        placeholder="WRITE YOUR SPARQL QUERY HERE..."
      ></textarea>
    </div>
    <div class="text-end">
      <button type="submit" class="btn btn-primary">Execute Query</button>
    </div>
  </form>

  <div class="mt-4">
    <p>Found {nb_results} rows </p>
    <h5>Results:</h5>
    <div class="table-responsive">
      <table class="table table-bordered table-hover bg-white" id="resultsTable">
        <thead class="table-light">
          <tr id="tableHeader"></tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
    <div class="d-flex justify-content-between mt-3">
      <button class="btn btn-secondary" id="prevBtn">Previous</button>
      <span id="pageInfo" class="align-self-center"></span>
      <button class="btn btn-secondary" id="nextBtn">Next</button>
    </div>
  </div>
</div>

<script>
  const textarea = document.getElementById("sparqlQuery");
  const tableData = [{table_rows_js_array}];
  const tableHeaders = [{table_headers_js_array}];
  const rowsPerPage = 50;
  let currentPage = 0;

  function renderTable() {{
    const tableHeader = document.getElementById("tableHeader");
    const tableBody = document.getElementById("tableBody");
    const pageInfo = document.getElementById("pageInfo");

    // Header
    tableHeader.innerHTML = "";
    for (const h of tableHeaders) {{
      const th = document.createElement("th");
      th.textContent = h;
      tableHeader.appendChild(th);
    }}

    // Body
    tableBody.innerHTML = "";
    const start = currentPage * rowsPerPage;
    const end = Math.min(start + rowsPerPage, tableData.length);

    for (let i = start; i < end; i++) {{
      const row = tableData[i];
      const tr = document.createElement("tr");

      for (let cell of row) {{
        const td = document.createElement("td");

        if (cell.startsWith("<") && cell.endsWith(">")) {{
          const uri = cell.substring(1, cell.length - 1);
          const a = document.createElement("a");
          a.href = `/entity/<${{uri}}>`;
          a.textContent = `<${{uri}}>`;
          a.target = "_blank";
          td.appendChild(a);
        }} else {{
          td.textContent = cell;
        }}
        tr.appendChild(td);
      }}
      tableBody.appendChild(tr);
    }}

    pageInfo.textContent = `Page ${{currentPage + 1}} of ${{Math.ceil(tableData.length / rowsPerPage)}}`;
  }}

  document.getElementById("prevBtn").addEventListener("click", () => {{
    if (currentPage > 0) {{
      currentPage--;
      renderTable();
    }}
  }});

  document.getElementById("nextBtn").addEventListener("click", () => {{
    if ((currentPage + 1) * rowsPerPage < tableData.length) {{
      currentPage++;
      renderTable();
    }}
  }});

  // SPARQL query editor tab support
  textarea.addEventListener("keydown", function(e) {{
    if (e.key === "Tab") {{
      e.preventDefault();
      const start = this.selectionStart;
      const end = this.selectionEnd;
      this.value = this.value.substring(0, start) + "\t" + this.value.substring(end);
      this.selectionStart = this.selectionEnd = start + 1;
    }}
  }});

  window.addEventListener("DOMContentLoaded", () => {{
    const params = new URLSearchParams(window.location.search);
    const query = params.get("query");
    const defaultQuery = `SELECT * WHERE {{
  ?s ?p ?o
}}`;
    textarea.value = (query && query.trim() !== "") ? query : defaultQuery;
    if (tableData.length > 0) renderTable();
  }});
</script>

</body>
</html>
"#)

}
pub(crate) fn entity_page(uri:&str, name:&str, description:&str, otype:&str, image:&str, table_1:&str, table_2: &str) ->String{
    format!(r#"<html data-bs-theme="dark">
        <head>  
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>KG Explorer</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-LN+7fdVzj6u52u30Kp6M/trliBMCMKTyK833zpbD+pXdCLuTusPj697FH4R/5mcr" crossorigin="anonymous">
        </head>
        <body>

<div class="container py-5">
            <a href="/" class="text-decoration-none"><h1 class="mb-4 text-center">Entity Explorer</h1></a>
{image}
<div class="alert alert-light"><p>{}</p></div>
             <div class="alert alert-info">
      <strong>Type(s):</strong> {otype}
    </div>
<h1>{name}</h1>
<p>{description}</p>
            <h3>Outgoing Triples</h3>
        <div class="table-responsive">
            <table class="table table-bordered table-hover bg-white" id="resultsTable">
                <thead class="table-light">
                    <tr id="table1Header">
                        <td>Predicate</td>
                        <td>Object</td>
                    </tr>
                </thead>
                <tbody id="table1Body">
{table_1}
                </tbody>
            </table>
        </div>
        <h3 class="mt-5">Incoming Triples</h3>
        <div class="table-responsive">
            <table class="table table-bordered table-hover bg-white" id="resultsTable">
                <thead class="table-light">
                    <tr id="table1Header">
                        <td>Subject</td>
                        <td>Predicate</td>
                    </tr>
                </thead>
                <tbody id="table1Body">
{table_2}
                </tbody>
            </table>
        </div>
        </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.7/dist/js/bootstrap.bundle.min.js" integrity="sha384-ndDqU0Gzau9qJ1lfW4pNLlhNTkCfHzAVBReH9diLvGRem5+R9g2FzA8ZGN954O5Q" crossorigin="anonymous"></script>
    </body>
    </html>
    "#
, escape_html(uri.to_string())
    )
}
pub(crate) fn object_card(name:&str, description:&str, image: &str, id:&str)->String{
    format!(
        r#"
        <div class="col-md-4 mb-4">
            <div class="card h-100" style="cursor: pointer;" >
                <div class="mb-3">
                    <img src="{}" class="d-block w-100" alt="{}" style="height: 200px; object-fit: contain;" 
                            onerror="this.style.display='none';">
                </div>
                <div class="card-body">
        <a href="entity/<{}>">
                    <h5 class="card-title">{}</h5></a>
                    <p class="card-text">{}</p>
                </div>
            </div>
        </div>"#,
        image, name, id, name, description
    )
}