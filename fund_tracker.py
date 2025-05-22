    # … after writing master CSV/XLSX …

-   # Build filtered relevant subset (if you’d accidentally changed this)
-   mask_cat = df_all['Category'] != 'Other'
-   mask_sic = df_all['SIC Description'].astype(bool)
-   df_rel   = df_all[mask_cat | mask_sic]
+   # Build filtered relevant subset: only those matching a keyword OR having a SIC description
+   mask_cat = df_all['Category'] != 'Other'
+   mask_sic = df_all['SIC Description'].astype(bool)
+   df_rel   = df_all[mask_cat | mask_sic]

    # Write relevant outputs
    df_rel.to_csv(RELEVANT_CSV, index=False)
    df_rel.to_excel(RELEVANT_XLSX, index=False, engine='openpyxl')
    log.info(f"Wrote relevant CSV/XLSX ({len(df_rel)} rows)")
