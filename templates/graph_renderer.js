/**
 * Renders a D3.js force-directed graph with HTML content in nodes.
 * @param {string} containerId - The ID of the div where the graph will be rendered.
 * @param {object} graphData - An object containing nodes and links.
 */
function renderD3Graph(containerId, graphData) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`Container with id "${containerId}" not found.`);
    return;
  }
  const width = container.clientWidth;
  const height = container.clientHeight;

  const nodeWidth = 500;
  const nodeHeight = 280;

  // Create an SVG container
  const svg = d3
    .select(`#${containerId}`)
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  // Create a group to hold zoomable content
  const g = svg.append("g");

  // Add zoom and pan behavior
  const zoom = d3
    .zoom()
    .scaleExtent([0.1, 4])
    .on("zoom", (event) => {
      g.attr("transform", event.transform);
    });

  svg.call(zoom);

  // --- Force Simulation ---
  const simulation = d3
    .forceSimulation(graphData.nodes)
    .force(
      "link",
      d3
        .forceLink(graphData.links)
        .id((d) => d.id)
        .distance(1000)
        .strength(0.2)
    )
    .force("charge", d3.forceManyBody().strength(-10000))
    .force("collide", d3.forceCollide().radius(300).strength(1))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .alphaDecay(0.05); // faster stabilization

  const link = g
    .append("g")
    .attr("stroke", "#999")
    .attr("stroke-opacity", 0.8)
    .selectAll("line")
    .data(graphData.links)
    .join("line")
    .attr("stroke-width", 2.5);

  // --- Draw Edges ---
  const linkLabelGroup = g
    .append("g")
    .selectAll(".link-label-group")
    .data(graphData.links)
    .join("g")
    .attr("class", "link-label-group");

  const linkLabelText = linkLabelGroup
    .append("text")
    .attr("class", "edge-label-text")
    .text((d) => d.label);

  linkLabelGroup
    .insert("rect", "text")
    .attr("class", "edge-label-box")
    .each(function (d) {
      const textNode = d3.select(this.parentNode).select("text").node();
      if (textNode) {
        const bbox = textNode.getBBox();
        const padding = 10;

        d3.select(this)
          .attr("x", bbox.x - padding / 2)
          .attr("y", bbox.y - padding / 2)
          .attr("width", bbox.width + padding)
          .attr("height", bbox.height + padding)
          .attr("rx", 4) // rounded corners
          .attr("ry", 4);
      }
    });

  // --- Draw Nodes ---
  const node = g
    .append("g")
    .selectAll("g")
    .data(graphData.nodes)
    .join("g")
    .call(drag(simulation));

  // Use foreignObject to embed HTML content in each node
  const foreignObject = node
    .append("foreignObject")
    .attr("width", nodeWidth)
    .attr("height", nodeHeight)
    .style("overflow", "visible");

  const htmlContent = foreignObject
    .append("xhtml:div")
    .attr("class", "node-html-content");

  htmlContent
    .append("xhtml:a")
    .attr("href", (d) => d.url)
    .attr("target", "_blank")
    .text((d) => d.name);

  htmlContent.append("xhtml:div").attr("class", "separator");

  htmlContent
    .append("xhtml:img")
    .attr("src", (d) => d.image)
    .style("display", (d) => {
      if (d.hasOwnProperty("image")) {
        return "block";
      } else {
        return "none";
      }
    })
    .style("width", "80%")
    .style("margin-left", "10%");

  htmlContent
    .append("xhtml:div")
    .attr("class", "attributes")
    .html((d) => {
      let attributeString = "";
      for (const [_, [key, value]] of Object.entries(d.attributes)) {
        attributeString += `<p><strong>${key}</strong>: ${value}</p>\n`;
      }
      return attributeString;
    });

  // --- Tick Function ---
  simulation.on("tick", () => {
    link
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);

    linkLabelGroup.attr("transform", (d) => {
      const midX = (d.source.x + d.target.x) / 2;
      const midY = (d.source.y + d.target.y) / 2;
      return `translate(${midX}, ${midY})`;
    });

    node.attr(
      "transform",
      (d) => `translate(${d.x - nodeWidth / 2}, ${d.y - nodeHeight / 2})`
    );
  });

  // --- Drag Handlers ---
  function drag(simulation) {
    function dragstarted(event, d) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    }
    function dragged(event, d) {
      d.fx = event.x;
      d.fy = event.y;
    }
    function dragended(event, d) {
      if (!event.active) simulation.alphaTarget(0);
      // d.fx = null;
      // d.fy = null;
    }
    return d3
      .drag()
      .on("start", dragstarted)
      .on("drag", dragged)
      .on("end", dragended);
  }
}
