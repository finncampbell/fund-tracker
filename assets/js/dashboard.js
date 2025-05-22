// assets/js/dashboard.js
$(document).ready(function() {
  // 1) Load CSV & Directors JSON in parallel
  Promise.all([
    fetch('assets/data/relevant_companies.csv').then(r => r.text()),
    fetch('assets/data/directors.json').then(r => r.json())
  ]).then(([csvText, directorMap]) => {
    // 2) Parse CSV and attach Directors arrays
    const allData = Papa.parse(csvText, { header: true }).data
      .filter(r => r['Company Number'])
      .map(r => ({
        ...r,
        Directors: directorMap[r['Company Number']] || []
      }));

    // 3) Pre‐compile regexes
    const fundEntitiesRE = /\bFund\b|\bG[\.\-\s]?P\b|\bL[\.\-\s]?L[\.\-\s]?P\b|\bL[\.\-\s]?P\b/i;
    const ventureRE      = /\bVenture(s)?\b/i;
    const investRE       = /\bInvestment(s)?\b/i;
    const capitalRE      = /\bCapital\b/i;
    const equityRE       = /\bEquity\b/i;
    const advisorsRE     = /\bAdvisors\b/i;
    const partnersRE     = /\bPartners\b/i;

    // 4) Define filter tests by data-filter value
    const filterTests = {
      '':                r => r['Category'] !== 'Other',
      'Ventures':        r => ventureRE.test(r['Company Name']) || ventureRE.test(r['Category']),
      'Capital':         r => capitalRE.test(r['Company Name'])   || capitalRE.test(r['Category']),
      'Equity':          r => equityRE.test(r['Company Name'])    || equityRE.test(r['Category']),
      'Advisors':        r => advisorsRE.test(r['Company Name'])  || advisorsRE.test(r['Category']),
      'Partners':        r => partnersRE.test(r['Company Name'])  || partnersRE.test(r['Category']),
      'Investments':     r => investRE.test(r['Company Name'])    || investRE.test(r['Category']),
      'Fund Entities':   r => fundEntitiesRE.test(r['Company Name']),
      'SIC':             r => !!r['SIC Description']
    };

    // 5) Initialize main DataTable
    const companyTable = $('#companies').DataTable({
      data: allData,
      columns: [
        { data: 'Company Name'       },
        { data: 'Company Number'     },
        { data: 'Incorporation Date' },
        { data: 'Category'           },
        { data: 'Date Downloaded'    },
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

    // 6) Initialize SIC‐only DataTable
    const sicTable = $('#sic-companies').DataTable({
      data: allData.filter(r => r['SIC Description']),
      columns: [
        { data: 'Company Name'       },
        { data: 'Company Number'     },
        { data: 'Incorporation Date' },
        { data: 'Category'           },
        { data: 'Date Downloaded'    },
        { data: 'SIC Codes'          },
        { data: 'SIC Description'    },
        { data: 'Typical Use Case'   },
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

    // 7) Expand/collapse handler for both tables
    function toggleDirectors() {
      const $btn = $(this);
      const $tr  = $btn.closest('tr');
      const tableId = $btn.closest('table').attr('id');
      const dt     = tableId === 'sic-companies' ? sicTable : companyTable;
      const row    = dt.row($tr);

      if (row.child.isShown()) {
        row.child.hide();
        $btn.text('Expand for Directors');
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
        $btn.text('Hide Directors');
      }
    }
    $('#companies tbody').on('click',  '.expand-btn', toggleDirectors);
    $('#sic-companies tbody').on('click','.expand-btn', toggleDirectors);

    // 8) Global filter hook (applies to main table only)
    $.fn.dataTable.ext.search.push((settings, rowData) => {
      if (settings.nTable.id !== 'companies') return true;
      const active = $('.ft-btn.active').data('filter') || '';
      const testFn = filterTests[active] || (() => true);
      return testFn(rowData);
    });

    // 9) Tab click handler
    $('.ft-filters').on('click', '.ft-btn', function() {
      $('.ft-btn').removeClass('active');
      $(this).addClass('active');
      const active = $(this).data('filter') || '';
      $('#companies-container').toggle(active !== 'SIC');
      $('#sic-companies-container').toggle(active === 'SIC');
      if (active !== 'SIC') {
        companyTable.draw();
      }
    });
  });
});
