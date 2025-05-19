---
layout: default
title: Company Incorporations
---

# Company Incorporations

<!-- Filter buttons will appear here -->
<div id="filter-buttons" style="margin-bottom:20px; display: flex; flex-wrap: wrap; gap: 8px;"></div>

<table id="fund-table" border="1" style="width:100%;border-collapse:collapse;">
  <thead></thead>
  <tbody></tbody>
</table>

<script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>
<script>
// Which column to use for filtering (e.g., "Source", "Status", "SIC Codes", etc.)
const filterColumn = "Source"; // Change to your keyword column if needed

let allData = [];
let uniqueKeywords = [];

// Fetch and process the CSV file
fetch('/assets/data/master_companies.csv')
  .then(response => response.text())
  .then(csv => {
    Papa.parse(csv, {
      header: true,
      skipEmptyLines: true,
      complete: function(results) {
        allData = results.data.filter(row => Object.values(row).some(v => v && v.trim()));
        uniqueKeywords = Array.from(new Set(allData.map(row => row[filterColumn]).filter(Boolean))).sort();
        renderButtons();
        renderTable(allData);
      }
    });
  });

// Render filter buttons across the top ("All" at far left)
function renderButtons() {
  const container = document.getElementById('filter-buttons');
  container.innerHTML = '';

  // "All" button (default)
  let allBtn = document.createElement('button');
  allBtn.textContent = 'All';
  allBtn.onclick = () => {
    setActiveButton(allBtn);
    renderTable(allData);
  };
  allBtn.className = "active-filter-btn";
  container.appendChild(allBtn);

  // Keyword buttons
  uniqueKeywords.forEach(keyword => {
    let btn = document.createElement('button');
    btn.textContent = keyword;
    btn.onclick = () => {
      setActiveButton(btn);
      renderTable(allData.filter(row => row[filterColumn] === keyword));
    };
    container.appendChild(btn);
  });
}

// Highlight the active filter button
function setActiveButton(activeBtn) {
  const buttons = document.getElementById('filter-buttons').querySelectorAll('button');
  buttons.forEach(btn => btn.classList.remove('active-filter-btn'));
  activeBtn.classList.add('active-filter-btn');
}

// Render table data
function renderTable(data) {
  const columns = Object.keys(data[0] || {});
  const thead = document.querySelector('#fund-table thead');
  const tbody = document.querySelector('#fund-table tbody');
  thead.innerHTML = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
  tbody.innerHTML = data.map(row =>
    '<tr>' + columns.map(c => `<td>${row[c]||""}</td>`).join('') + '</tr>'
  ).join('');
}

// Add simple styling for the active button (Cayman uses green for accents)
const style = document.createElement('style');
style.innerHTML = `
  #filter-buttons button {
    background: #f5f5f5;
    border: 1px solid #cccccc;
    border-radius: 4px;
    padding: 6px 14px;
    cursor: pointer;
    font-size: 1rem;
    transition: background 0.2s;
  }
  #filter-buttons button:hover, #filter-buttons .active-filter-btn {
    background: #1abc9c;
    color: #fff;
    border-color: #1abc9c;
  }
  #fund-table th, #fund-table td {
    padding: 6px 10px;
    border: 1px solid #d1d5da;
    text-align: left;
  }
  #fund-table th {
    background: #eaecef;
  }
`;
document.head.appendChild(style);
</script>
