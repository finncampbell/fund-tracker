// Debug at load
console.log("🔥 dashboard.js loaded");

$(document).ready(function() {
  // Absolute URL to your CSV
  const csvUrl = '/fund-tracker/assets/data/master_companies.csv';

  // Inject debug banner
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
      $('#ft-debug').text(
        `Debug: loaded ${results.data.length} rows from CSV`
      );

      if (!results.data.length) {
        console.error('👉 Fund Tracker: CSV parsed to zero rows', results);
        return;
      }

      // Initialize DataTable
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

      // Wire up filters
      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        const filter = $(this).data('filter') || '';
        table.column(4).search(filter).draw();
      });
    },
    error: function(err) {
      $('#ft-debug').text('Error loading CSV—see console.');
      console.error('👉 Fund Tracker: PapaParse error', err);
    }
  });
});
