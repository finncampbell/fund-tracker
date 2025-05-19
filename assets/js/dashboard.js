// 🔥 Quick check that this file *is* loading:
console.log('🔥 dashboard.js loaded');

$(document).ready(function() {
  const csvUrl = 'assets/data/master_companies.csv';

  // inject a visible debug banner
  $('body').prepend(`
    <div id="ft-debug"
         style="background:#ffecec;color:#900;padding:0.5em;text-align:center;">
      Loading…
    </div>
  `);

  Papa.parse(csvUrl, {
    download: true,
    header: true,
    complete: function(results) {
      // report how many rows we got
      $('#ft-debug').text(\`Debug: loaded \${results.data.length} rows\`);
      if (!results.data.length) {
        console.error('🏷️ CSV parsed to zero rows', results);
        return;
      }

      // initialize DataTable
      const table = $('#companies').DataTable({
        data: results.data,
        columns: [
          { data: 'Company Name' },
          { data: 'Company Number' },
          { data: 'Incorporation Date' },
          { data: 'Status' },
          { data: 'Source' },
          { data: 'Date Downloaded' },
          { data: 'Time Discovered' }
        ],
        order: [[2, 'desc']],
        pageLength: 25,
        responsive: true
      });

      // wire up filter buttons
      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        const f = $(this).data('filter') || '';
        table.column(4).search(f).draw();
      });
    },
    error: function(err) {
      $('#ft-debug').text('❌ Error loading CSV—see console');
      console.error('CSV load error', err);
    }
  });
});
