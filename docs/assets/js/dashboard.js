$(document).ready(function() {
  // Repository parameters
  const repoOwner   = 'finncampbell';
  const repoName    = 'fund-tracker';
  const dataBranch  = 'data';   // for JSON
  const pagesBranch = 'main';   // for CSV

  // Raw GitHub URLs for data (always fetch fresh with a timestamp param)
  const csvUrl = `https://raw.githubusercontent.com/${repoOwner}/${repoName}/${pagesBranch}/docs/assets/data/relevant_companies.csv?v=${Date.now()}`;
  const directorsUrl = `https://raw.githubusercontent.com/${repoOwner}/${repoName}/${dataBranch}/docs/assets/data/directors.json?v=${Date.now()}`;

  // Regex to detect “Fund Entities” by company name
  const fundEntitiesRE = /\bFund\b|\bG[.\-\s]?P\b|\bL[.\-\s]?L[.\-\s]?P\b|\bL[.\-\s]?P\b/i;

  // Utility: format "LASTNAME, Firstnames" to "Firstnames LASTNAME"
  function formatName(name) {
    if (!name) return '';
    const parts = name.split(',');
    if (parts.length === 2) {
      const last = parts[0].trim();
      const first = parts[1].trim();
      return `${first} ${last}`;
    }
    return name;
  }

  // 1) Load the CSV from the Pages-served main branch
  Papa.parse(csvUrl, {
    download: true,
    header: true,
    complete(results) {
      // Filter out any rows lacking a CompanyNumber
      const raw = results.data.filter(r => r['CompanyNumber']);

      let directorsMap = {};
      // 2) Load directors.json from the data branch
      fetch(directorsUrl)
        .then(r => r.json())
        .then(json => {
          directorsMap = Object.fromEntries(
            Object.entries(json).map(([k, v]) => [k.trim(), v])
          );
          initTables();
        })
        .catch(err => {
          console.error('Failed to load directors.json', err);
          initTables();
        });

      function initTables() {
        // Build a normalized array of objects from the parsed CSV rows
        const data = raw.map(r => {
          const num = r['CompanyNumber'].trim();
          return {
            CompanyName:       r['CompanyName'] || '',
            CompanyNumber:     num,
            IncorporationDate: r['IncorporationDate'] || '',
            Category:          r['Category'] || '',
            DateDownloaded:    r['DateDownloaded'] || '',
            SICCodes:          r['SIC Codes'] || '',
            SICDescription:    r['SIC Description'] || '',
            TypicalUseCase:    r['Typical Use Case'] || '',
            Directors:         directorsMap[num] || []
          };
        });

        // MAIN TABLE: show rows where Category != "Other" and != pure "SIC"
        const mainData = data.filter(r => {
          return r.Category !== 'Other' && r.Category !== 'SIC';
        });

        const companyTable = $('#companies').DataTable({
          data: mainData,
          columns: [
            { data: 'CompanyName',       title: 'Company Name' },
            { data: 'CompanyNumber',     title: 'Company Number' },
            { data: 'IncorporationDate', title: 'Incorporation Date' },
            { data: 'Category',          title: 'Category' },
            { data: 'DateDownloaded',    title: 'Date Downloaded' },
            {
              data: 'Directors',
              title: 'Directors',
              orderable: false,
              render: (dirs, type) =>
                type === 'display'
                  ? `<button class="expand-btn">Expand for Directors</button>`
                  : dirs
            }
          ],
          order: [[2, 'desc']],
          pageLength: 25,
          responsive: true
        });

        // SIC TABLE: show rows where Category includes "SIC"
        const sicData = data.filter(r => {
          const parts = r.Category.split(',').map(s => s.trim());
          return parts.includes('SIC');
        });

        const sicTable = $('#sic-companies').DataTable({
          data: sicData,
          columns: [
            { data: 'CompanyName',       title: 'Company Name' },
            { data: 'CompanyNumber',     title: 'Company Number' },
            { data: 'IncorporationDate', title: 'Incorporation Date' },
            { data: 'Category',          title: 'Category' },
            { data: 'DateDownloaded',    title: 'Date Downloaded' },
            { data: 'SICCodes',          title: 'SIC Codes' },
            { data: 'SICDescription',    title: 'SIC Description' },
            { data: 'TypicalUseCase',    title: 'Typical Use Case' },
            {
              data: 'Directors',
              title: 'Directors',
              orderable: false,
              render: (dirs, type) =>
                type === 'display'
                  ? `<button class="expand-btn">Expand for Directors</button>`
                  : dirs
            }
          ],
          order: [[2, 'desc']],
          pageLength: 25,
          responsive: true
        });

        // Expand/Collapse directors rows
        function toggleDirectors() {
          const $btn    = $(this);
          const tableId = $btn.closest('table').attr('id');
          const dt      = tableId === 'sic-companies' ? sicTable : companyTable;
          const row     = dt.row($btn.closest('tr'));

          if (row.child.isShown()) {
            row.child.hide();
            $btn.removeClass('active').text('Expand for Directors');
          } else {
            const dirs = row.data().Directors || [];
            let html = '<table class="child-table"><tr>' +
                       '<th>Director Name</th>' +
                       '<th>Appointment</th>' +
                       '<th>Date of Birth</th>' +
                       '<th># of Appointments</th>' +
                       '<th>Officer Role</th>' +
                       '<th>Nationality</th>' +
                       '<th>Occupation</th>' +
                       '<th>Details Link</th>' +
                       '</tr>';

            if (dirs.length === 0) {
              html += '<tr>' + '<td>No Data</td>'.repeat(8) + '</tr>';
            } else {
              dirs.forEach(d => {
                const formattedName = formatName(d.title || '');
                html +=
                  '<tr>' +
                  `<td>${formattedName}</td>` +
                  `<td>${d.appointment || ''}</td>` +
                  `<td>${d.dateOfBirth || ''}</td>` +
                  `<td>${d.appointmentCount || ''}</td>` +
                  `<td>${d.officerRole || ''}</td>` +
                  `<td>${d.nationality || ''}</td>` +
                  `<td>${d.occupation || ''}</td>` +
                  `<td><a href="https://api.company-information.service.gov.uk${d.selfLink}" target="_blank">Details</a></td>` +
                  '</tr>';
              });
            }
            html += '</table>';
            row.child(html).show();
            $btn.addClass('active').text('Hide Directors');
          }
        }

        $('#companies tbody').on('click', '.expand-btn', toggleDirectors);
        $('#sic-companies tbody').on('click', '.expand-btn', toggleDirectors);

        // Search hook for MAIN table
        $.fn.dataTable.ext.search.push((settings, rowData) => {
          if (settings.nTable.id !== 'companies') return true;
          const active = $('.ft-btn.active').data('filter') || '';
          if (!active) {
            return rowData[3] !== 'Other' && rowData[3] !== 'SIC';
          }
          if (active === 'SIC') return false;
          if (active === 'Fund Entities') return fundEntitiesRE.test(rowData[0]);
          const parts = rowData[3].split(',').map(s => s.trim());
          return parts.includes(active);
        });

        // Filter-tab click handler
        $('.ft-filters').on('click', '.ft-btn', function() {
          $('.ft-btn').removeClass('active');
          $(this).addClass('active');
          const filter = $(this).data('filter') || '';
          $('#companies-container').toggle(filter !== 'SIC');
          $('#sic-companies-container').toggle(filter === '
SIC');
          if (filter !== 'SIC') {
            companyTable.draw();
          } else {
            sicTable.columns.adjust().draw();
          }
        });
      }
    }
  });

  // Flatpickr range picker & backfill button
  flatpickr("#backfill-range", {
    mode: "range",
    dateFormat: "Y-m-d",
    minDate: "2000-01-01",
    maxDate: new Date(),
    onChange: function(selectedDates, dateStr, instance) {
      const btn = document.getElementById("run-backfill");
      if (selectedDates.length === 2) {
        btn.disabled = false;
        btn.dataset.start = instance.formatDate(selectedDates[0], "Y-m-d");
        btn.dataset.end   = instance.formatDate(selectedDates[1], "Y-m-d");
      } else {
        btn.disabled = true;
        delete btn.dataset.start;
        delete btn.dataset.end;
      }
    }
  });

  // Run Historical Backfill via Netlify function
  document.getElementById("run-backfill").addEventListener("click", () => {
    const btn   = document.getElementById("run-backfill");
    const start = btn.dataset.start;
    const end   = btn.dataset.end;
    if (!start || !end) return;

    fetch('https://fund-tracker-functions.netlify.app/.netlify/functions/backfill', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start_date: start, end_date: end })
    })
      .then(r => {
        if (!r.ok) throw new Error("Dispatch failed");
        alert(`Backfill dispatched for ${start} → ${end}`);
        btn.disabled = true;
        document.getElementById("backfill-range").value = "";
      })
      .catch(err => {
        console.error(err);
        alert("Error dispatching backfill; see console.");
      });
  });

  // Fetch Directors Now button logic
  document.getElementById("run-fetch-directors").addEventListener("click", async () => {
    const btn = document.getElementById("run-fetch-directors");
    btn.disabled = true;
    btn.textContent = 'Fetching Directors…';

    try {
      const resp = await fetch(
        'https://fund-tracker-functions.netlify.app/.netlify/functions/trigger-fetch-directors',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        }
      );

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      alert('Fetch Directors workflow dispatched successfully!');
    } catch (err) {
      console.error('Error dispatching Fetch Directors:', err);
      alert('Failed to dispatch Fetch Directors. See console for details.');
    } finally {
      btn.disabled = false;
      btn.textContent = 'Fetch Directors Now';
    }
  });

  // 9) Display “Next scheduled run” (every 10 minutes) in local time
  function updateNextRunDisplay() {
    const now = new Date();

    // Compute minutes to next multiple of 10
    const minutes = now.getMinutes();
    const remainder = minutes % 10;
    let nextMinuteBucket = minutes - remainder + 10;
    let nextHour = now.getHours();
    let nextDay = now.getDate();
    let nextMonth = now.getMonth();      // zero-based (Jan = 0)
    let nextYear = now.getFullYear();

    if (nextMinuteBucket >= 60) {
      nextMinuteBucket = 0;
      nextHour += 1;
      if (nextHour >= 24) {
        nextHour = 0;
        // Advance to next day (accounting for month/year rollovers)
        now.setDate(now.getDate() + 1);
        nextYear = now.getFullYear();
        nextMonth = now.getMonth();
        nextDay = now.getDate();
      }
    }

    // Construct a Date object for the next run
    const nextRun = new Date(
      nextYear,
      nextMonth,
      nextDay,
      nextHour,
      nextMinuteBucket,
      0,   // seconds
      0    // milliseconds
    );

    // Format as "YYYY-MM-DD HH:mm"
    const pad = (n) => String(n).padStart(2, "0");
    const yyyy = nextRun.getFullYear();
    const mm   = pad(nextRun.getMonth() + 1); // Jan=0
    const dd   = pad(nextRun.getDate());
    const hh   = pad(nextRun.getHours());
    const mins = pad(nextRun.getMinutes());

    const formatted = `${yyyy}-${mm}-${dd} ${hh}:${mins}`;
    document.getElementById("next-run-timestamp").textContent = formatted;
  }

  // Initial call
  updateNextRunDisplay();
  // Refresh every 30 seconds
  setInterval(updateNextRunDisplay, 30000);

}); // <-- end of document.ready
