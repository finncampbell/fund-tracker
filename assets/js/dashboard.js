$(document).ready(function() {
  const url = 'assets/data/relevant_companies.csv';
  const fundEntitiesRE = /\bFund\b|\bG\W*P\b|\bL\W*P\b|\bL\W*L\W*P\b/i;

  Papa.parse(url, { download: true, header: true,
    complete: function(results) {
      // Companies table
      const companyTable = $('#companies').DataTable({
        data: results.data,
        columns: [
          { data: 'Company Name' },
          { data: 'Company Number' },
          { data: 'Incorporation Date' },
          { data: 'Category' },
          { data: 'Date Downloaded' }
        ],
        order: [[2,'desc']], pageLength:25, responsive:true
      });

      // SIC-enhanced companies table
      const sicTable = $('#sic-companies').DataTable({
        data: results.data.filter(r => r['SIC Codes']),
        columns: [
          { data: 'Company Name' },
          { data: 'Company Number' },
          { data: 'Incorporation Date' },
          { data: 'Category' },
          { data: 'Date Downloaded' },
          { data: 'SIC Codes' },
          { data: 'SIC Description' },
          { data: 'Typical Use Case' }
        ],
        order: [[2,'desc']], pageLength:25, responsive:true
      });

      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        const filter = $(this).data('filter') || '';

        $('#sic-companies-container').toggle(filter==='SIC');
        $('#companies-container').toggle(filter!=='SIC');

        if (filter && filter!=='SIC') companyTable.draw();
      });

      // global search hook for non-SIC filters
      $.fn.dataTable.ext.search.push((settings, data) => {
        const active = $('.ft-btn.active').data('filter') || '';
        if (!active || active==='SIC') return active!=='SIC';
        const name = data[0], cat = data[3];
        if (active==='Fund Entities') return fundEntitiesRE.test(name);
        const re = new RegExp('\\b'+active+'\\b','i');
        return re.test(name) || re.test(cat);
      });
    }
  });
});
