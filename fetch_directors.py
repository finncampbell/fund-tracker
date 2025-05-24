--- fetch_directors.py
+++ fetch_directors.py
@@ def fetch_one(number):
-        items = resp.json().get('items', [])
+        # only keep active directors (no resigned_on date)
+        items = [
+            off for off in resp.json().get('items', [])
+            if off.get('resigned_on') is None
+        ]
@@ def fetch_one(number):
-        directors.append({
-            'title':            off.get('name'),
-            'snippet':          off.get('snippet'),
-            'appointmentCount': off.get('appointment_count'),
-            'selfLink':         off['links'].get('self'),
-            'officerRole':      off.get('officer_role'),
-            'nationality':      off.get('nationality'),
-            'occupation':       off.get('occupation'),
-        })
+        # build a dob string from the nested date_of_birth object
+        dob = off.get('date_of_birth') or {}
+        year, month = dob.get('year'), dob.get('month')
+        if year and month:
+            dob_str = f"{year}-{int(month):02d}"
+        elif year:
+            dob_str = str(year)
+        else:
+            dob_str = ''
+
+        directors.append({
+            'title':            off.get('name'),
+            'snippet':          off.get('snippet'),
+            'dateOfBirth':      dob_str,
+            'appointmentCount': off.get('appointment_count'),
+            'selfLink':         off['links'].get('self'),
+            'officerRole':      off.get('officer_role'),
+            'nationality':      off.get('nationality'),
+            'occupation':       off.get('occupation'),
+        })
