// assets/js/dashboard.js
$(document).ready(function() {
  Promise.all([
    fetch('assets/data/relevant_companies.csv').then(r => r.text()),
    fetch('assets/data/directors.json').then(r => r.json())
  ]).then(([csvText, directorMap]) => {
    // 1) Parse CSV and attach Directors arrays
    const allData = Papa.parse(csvText, { header: true }).data
      .filter(r => r['Company Number'])
      .map(r => {
        r.Directors = directorMap[r['Company Number']] || [];
        return r;
      });

    // 2) Regex for Fund Entities tab
    const fundEntitiesRE = /\bFund\b|\bG[\.\-\s]?P\b|\bL[\.\-\s]?L[\.\-\s]?P\b|\bL[\.\-\s]?P\b/i;

    // 3) Initialize main companies table
    const companyTable = $('#companies').DataTable({
      data: allData,
      columns: [
        { data: 'Company Name' },
        { data: 'Company Number' },
        { data: 'Incorporation Date' },
        { data: 'Category' },
        { data: 'Date Downloaded' },
        {
          data: 'Directors',
          title: 'Directors',
          orderable: false,
          render: (dirs, type) => type === 'display'
            ? `<button class="expand-btn">Expand for Directors</button>`
            : dirs
        }
      ],
      order: [[2, 'desc']],
      pageLength: 25,
      responsive: true
    });

    // 4) Initialize SIC‐enhanced table (only rows with SIC Description)
    const sicData = allData.filter(r => r['SIC Description']);
    const sicTable = $('#sic-companies').DataTable({
      data: sicData,
      columns: [
        { data: 'Company Name' },
        { data: 'Company Number' },
        { data: 'Incorporation Date' },
        { data: 'Category' },
        { data: 'Date Downloaded' },
        { data: 'SIC Codes' },
        { data: 'SIC Description' },
        { data: 'Typical Use Case' },
        {
          data: 'Directors',
          title: 'Directors',
          orderable: false,
          render: (dirs, type) => type === 'display'
            ? `<button class="expand-btn">Expand for Directors</button>`
            : dirs
        }
      ],
      order: [[2, 'desc']],
      pageLength: 25,
      responsive: true
    });

    // 5) Expand/Collapse handler for both tables
    function toggleDirectors(ev) {
      const tbl = ev.delegateTarget.closest('table');
      const dt  = $(tbl).hasClass('display') && tbl.id === 'companies'
        ? companyTable
        : sicTable;
      const tr  = $(this).closest('tr');
      const row = dt.row(tr);
      if (row.child.isShown()) {
        row.child.hide();
        $(this).text('Expand for Directors');
      } else {
        const dirs = row.data().Directors;
        let html = '<table class="child-table"><tr>'
          +'<th>Name</th><th>Snippet</th><th>Apps</th>'
          +'<th>Role</th><th>Nationality</th><th>Occupation</th><th>Link</th>'
          +'</tr>';
        dirs.forEach(d => {
          html += `<tr>
            <td>${d.title||''}</td>
            <td>${d.snippet||''}</td>
            <td>${d.appointmentCount||''}</td>
            <td>${d.officerRole||''}</td>
            <td>${d.nationality||''}</td>
            <td>${d.occupation||''}</td>
            <td><a href="https://api.company-information.service.gov.uk${d.selfLink}">Details</a></td>
          </tr>`;
        });
        html += '</table>';
        row.child(html).show();
        $(this).text('Hide Directors');
      }
    }
    $('#companies tbody').on('click', '.expand-btn', toggleDirectors);
    $('#sic-companies tbody').on('click', '.expand-btn', toggleDirectors);

    // 6) Global filter hook applies to main table only
    $.fn.dataTable.ext.search.push((settings, rowData) => {
      // only apply to #companies
      if (settings.nTable.id !== 'companies') return true;
      const active = $('.ft-btn.active').data('filter') || '';
      if (!active)                return rowData[3] !== 'Other';        // All
      if (active === 'SIC')       return false;                         // SIC hides main
      if (active === 'Fund Entities') return fundEntitiesRE.test(rowData[0]);
      return rowData[3] === active;                                    // Category tabs
    });

    // 7) Tab click handler
    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const filter = $(this).data('filter') || '';
      // show/hide containers
      $('#companies-container').toggle(filter !== 'SIC');
      $('#sic-companies-container').toggle(filter === 'SIC');
      // redraw main on non‐SIC tab
      if (filter !== 'SIC') {
        companyTable.draw();
      }
    });
  });
});
