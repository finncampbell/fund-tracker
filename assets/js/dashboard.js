--- a/assets/js/dashboard.js
+++ b/assets/js/dashboard.js
@@ function toggleDirectors() {
-      let html = '<table class="child-table"><tr>'
-        +'<th>Director Name</th>'
-        +'<th>Appointment</th>'
-        +'<th># of Appointments</th>'
-        +'<th>Officer Role</th>'
-        +'<th>Nationality</th>'
-        +'<th>Occupation</th>'
-        +'<th>Details Link</th>'
-        +'</tr>';
+      let html = '<table class="child-table"><tr>'
+        +'<th>Director Name</th>'
+        +'<th>Appointment</th>'
+        +'<th>Date of Birth</th>'
+        +'<th># of Appointments</th>'
+        +'<th>Officer Role</th>'
+        +'<th>Nationality</th>'
+        +'<th>Occupation</th>'
+        +'<th>Details Link</th>'
+        +'</tr>';

@@ dirs.forEach(d => {
-          html+=`<tr><td>${d.title||''}</td><td>${d.snippet||''}</td><td>${d.appointmentCount||''}</td><td>${d.officerRole||''}</td><td>${d.nationality||''}</td><td>${d.occupation||''}</td><td><a href="https://api.company-information.service.gov.uk${d.selfLink}">Details</a></td></tr>`;
+          html+=`<tr>`
+            +`<td>${d.title||''}</td>`
+            +`<td>${d.snippet||''}</td>`
+            +`<td>${d.dateOfBirth||''}</td>`
+            +`<td>${d.appointmentCount||''}</td>`
+            +`<td>${d.officerRole||''}</td>`
+            +`<td>${d.nationality||''}</td>`
+            +`<td>${d.occupation||''}</td>`
+            +`<td><a href="https://api.company-information.service.gov.uk${d.selfLink}">Details</a></td>`
+          +`</tr>`;
