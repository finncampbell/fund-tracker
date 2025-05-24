--- a/fetch_directors.py
+++ b/fetch_directors.py
@@ def fetch_one(number):
-    try:
-        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
-        resp.raise_for_status()
-        items = resp.json().get('items', [])
+    try:
+        resp = requests.get(url, auth=(CH_KEY, ''), params=params, timeout=10)
+        resp.raise_for_status()
+        # only keep active directors (no resigned_on date)
+        items = [
+            off for off in resp.json().get('items', [])
+            if off.get('resigned_on') is None
+        ]
     except Exception as e:
         log.warning(f"{number}: fetch error: {e}")
         return None

-    directors = []
+    directors = []
     for off in items:
-        directors.append({
-            'title':            off.get('name'),
-            'snippet':          off.get('snippet'),
-            'appointmentCount': off.get('appointment_count'),
-            'selfLink':         off['links'].get('self'),
-            'officerRole':      off.get('officer_role'),
-            'nationality':      off.get('nationality'),
-            'occupation':       off.get('occupation'),
-        })
+        # format date of birth into YYYY-MM (or just YYYY)
+        dob = off.get('date_of_birth') or {}
+        year  = dob.get('year')
+        month = dob.get('month')
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
     return directors
