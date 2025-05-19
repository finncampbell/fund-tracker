---
layout: default
title: Company Incorporations
---

# Company Incorporations

<div id="filter-buttons" style="margin-bottom:15px;"></div>

<table id="fund-table" border="1">
  <thead></thead>
  <tbody></tbody>
</table>

<script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>
<script>
const sourceColumn = "Source";
const sicSourceValue = "SIC Codes"; // Change this if your "Source" column uses a different value for SIC code entries

let allData = [];

fetch('/assets/data/master_companies.csv')
  .then(response => response.text())
  .then(csv => {
    Papa.parse(csv, {
      header: true,
      complete: function(results) {
        // Remove empty rows if any
        allData = results.data.filter(row => Object.values(row).some(v => v && v.trim()));
        renderButtons();
        renderTable(allData);
      }
    });
  });

function renderButtons() {
  const container = document.getElementById('filter-buttons');
  container.innerHTML = '';

  // "All" button
  let allBtn = document.createElement('button');
  allBtn.textContent = 'All';
  allBtn.onclick = () => renderTable(allData);
  allBtn.style.marginRight = '8px';
  container.appendChild(allBtn);

  // "SIC Codes" button
  let sicBtn = document.createElement('button');
  sicBtn.textContent = 'SIC Codes';
  sicBtn.onclick = () => renderTable(allData.filter(row => row[sourceColumn] === sicSourceValue));
  sicBtn.style.marginRight = '8px';
  container.appendChild(sicBtn);
}

function renderTable(data) {
  const columns = Object.keys(data[0] || {});
  const thead = document.querySelector('#fund-table thead');
  const tbody = document.querySelector('#fund-table tbody');
  thead.innerHTML = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
  tbody.innerHTML = data.map(row =>
    '<tr>' + columns.map(c => `<td>${row[c]||""}</td>`).join('') + '</tr>'
  ).join('');
}
</script>
