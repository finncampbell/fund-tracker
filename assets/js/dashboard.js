// assets/js/dashboard.js
$(document).ready(function() {
  Promise.all([
    fetch('assets/data/relevant_companies.csv').then(r => r.text()),
    fetch('assets/data/directors.json').then(r => r.json())
  ]).then(([csvText, directorMap]) => {
    let data = Papa.parse(csvText, { header: true }).data
      .filter(r => r['Company Number'])
      .map(r => {
        r.Directors = directorMap[r['Company Number']] || [];
        return r;
      });

    const fundEntitiesRE = /\bFund\b|\bG[\.\-\s]?P\b|\bL[\.\-\s]?L[\.\-\s]?P\b|\bL[\.\-\s]?P\b/i;

    const filters = [
      { key:'',              label:'All',           test: r => r['Category']!=='Other' },
      { key:'Ventures',      label:'Ventures',      test: r => /\bVenture(s)?\b/i.test(r['Company Name']+r['Category']) },
      { key:'Capital',       label:'Capital',       test: r => /\bCapital\b/i.test(r['Company Name']+r['Category']) },
      { key:'Equity',        label:'Equity',        test: r => /\bEquity\b/i.test(r['Company Name']+r['Category']) },
      { key:'Advisors',      label:'Advisors',      test: r => /\bAdvisors\b/i.test(r['Company Name']+r['Category']) },
      { key:'Partners',      label:'Partners',      test: r => /\bPartners\b/i.test(r['Company Name']+r['Category']) },
      { key:'SIC',           label:'SIC Codes',     test: r => !!r['SIC Description'] },
      { key:'Fund Entities', label:'Fund Entities', test: r => fundEntitiesRE.test(r['Company Name']) },
      { key:'Investments',   label:'Investments',   test: r => /\bInvestment(s)?\b/i.test(r['Company Name']+r['Category']) }
    ];

    const companyTable = $('#companies').DataTable({
      data,
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

    $('#companies tbody').on('click', '.expand-btn', function() {
      const tr  = $(this).closest('tr');
      const row = companyTable.row(tr);
      if (row.child.isShown()) {
        row.child.hide();
        $(this).text('Expand for Directors');
      } else {
        const dirs = row.data().Directors;
        let html = '<table class="child-table"><tr><th>Name</th><th>Snippet</th><th>Apps</th><th>Role</th><th>Nationality</th><th>Occupation</th><th>Link</th></tr>';
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
    });

    $.fn.dataTable.ext.search.push((settings, rowData) => {
      const active = $('.ft-btn.active').data('filter') || '';
      if (!active) return rowData[3] !== 'Other';
      if (active === 'SIC') return false;
      if (active === 'Fund Entities') return fundEntitiesRE.test(rowData[0]);
      return rowData[3] === active;
    });

    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const filter = $(this).data('filter') || '';
      $('#companies-container').toggle(filter !== 'SIC');
      $('#sic-companies-container').toggle(filter === 'SIC');
      if (filter !== 'SIC') companyTable.draw();
    });
  });
});
