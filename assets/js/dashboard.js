// 🔥 Check that the file loads:
console.log('🔥 dashboard.js loaded');

$(document).ready(function() {
  var csvUrl = 'assets/data/master_companies.csv';

  // inject a debug banner at the top
  $('body').prepend(
    '<div id="ft-debug" ' +
    'style="background:#ffecec;color:#900;padding:0.5em;text-align:center;">' +
    'Loading…' +
    '</div>'
  );

  Papa.parse(csvUrl, {
    download: true,
    header: true,
    complete: function(results) {
      var count = results.data.length;
      $('#ft-debug').text('Debug: loaded ' + count + ' rows');
      if (count === 0) {
        console.error('🚨 CSV parsed to zero rows', results);
        return;
      }

      var table = $('#companies').DataTable({
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

      // filter buttons
      $('.ft-btn').on('click', function() {
        $('.ft-btn').removeClass('active');
        $(this).addClass('active');
        var filter = $(this).data('filter') || '';
        table.column(4).search(filter).draw();
      });
    },
    error: function(err) {
      $('#ft-debug').text('❌ Error loading CSV—check console');
      console.error('CSV load error', err);
    }
  });
});
