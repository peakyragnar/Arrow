-- Refresh COMMENT ON data_quality_flags.flag_type to list the known
-- flag types that exist after the Layer 1 SOFT / CF subtotal-component
-- drift addition. Pure comment update; no schema change.
--
-- Types:
--   cf_subtotal_component_drift — Layer 1 SOFT, inline during mainline
--                                 ingest. CF subtotal (cfo/cfi/cff) does
--                                 not tie to sum of FMP's own component
--                                 fields. Row loaded verbatim; analyst
--                                 reviews via scripts/review_flags.py.
--   layer3_q_sum_vs_fy          — Layer 3 side rail. Q1+Q2+Q3+Q4 ≠ FY
--                                 for a flow concept; amendment agent
--                                 couldn't safely supersede.
--   xbrl_sanity_bound           — Layer 3 side rail. An XBRL
--                                 supersession candidate violated Rule B
--                                 (sign flip, >50% delta).
--   layer5_xbrl_anchor          — Layer 5 side rail. FMP and SEC XBRL
--                                 disagree on a top-line anchor beyond
--                                 the 0.5% tolerance.
--   layer4_*                    — reserved for formula-guard diagnostics
--                                 when Layer 4 is implemented.

COMMENT ON COLUMN data_quality_flags.flag_type IS
  'Known types: cf_subtotal_component_drift (Layer 1 SOFT, inline from mainline ingest), layer3_q_sum_vs_fy, xbrl_sanity_bound (Layer 3 side rail), layer5_xbrl_anchor (Layer 5 side rail). Layer 4 types reserved.';
