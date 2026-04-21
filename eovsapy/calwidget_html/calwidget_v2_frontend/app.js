(function () {
  const html = htm.bind(React.createElement);
  const useEffect = React.useEffect;
  const useRef = React.useRef;
  const useState = React.useState;

  // Toggle to re-enable the legacy Sum Amplitude / Sum Phase (Band-Averaged
  // Amplitude & Band-Averaged Phase Difference) overview panels. Off by default
  // because these are redundant with the Inband-Fit / Refcal-vs-Phacal panels
  // below; flip to `true` in-source if you need them back.
  const SHOW_SUM_OVERVIEW_PANELS = false;
  const OVERVIEW_SECTIONS = [
    ...(SHOW_SUM_OVERVIEW_PANELS
      ? [
          {
            id: "sum_amp",
            label: "Sum X & Y Amplitude",
            showLegend: false,
            panelHeight: 116,
          },
          {
            id: "sum_pha",
            label: "Sum X & Y Phase [rad]",
            showLegend: false,
            panelHeight: 116,
          },
        ]
      : []),
    {
      id: "phacal_phase_compare",
      label: "Refcal vs Phacal Phase",
      showLegend: false,
      panelHeight: 116,
      phacalOnly: true,
    },
    {
      id: "inband_fit",
      label: "Inband Fit",
      showLegend: true,
      panelHeight: 138,
    },
    {
      id: "inband_relative_phase",
      label: "Relative Phase + Fit",
      showLegend: true,
      panelHeight: 138,
      refcalOnly: true,
    },
    {
      id: "inband_residual_phase_band",
      label: "Per-Band Residual Phase",
      showLegend: true,
      panelHeight: 138,
    },
    {
      id: "inband_residual_delay_band",
      label: "Residual Delay Per Band",
      showLegend: true,
      panelHeight: 126,
    },
  ];
  const TIME_FLAG_SCOPES = [
    { id: "selected", label: "Selected" },
    { id: "this_ant", label: "This Ant" },
    { id: "this_band", label: "This Band" },
    { id: "higher_bands", label: "Higher Bands" },
    { id: "all", label: "All" },
  ];
  const INBAND_SCOPE_OPTIONS = [
    { id: "selected", label: "Selected" },
    { id: "all", label: "All" },
  ];
  const WALL_ABBREVIATIONS = [
    [/Anchor-Referenced/g, "Anchor-Ref."],
    [/\bRelative\b/g, "Rel."],
    [/\bResidual\b/g, "Res."],
    [/\bFrequency\b/g, "Freq."],
    [/\bDifference\b/g, "Diff."],
    [/\bDiagnostics\b/g, "Diag."],
  ];

  const COLOR_X = "#1f77b4";
  const COLOR_Y = "#ff7f0e";
  const PANEL_GRID_PANEL_WIDTH = 200;
  const TIME_FLAG_MIN_DRAG_PX = 6;
  const ACTION_PROGRESS = {
    refcal: {
      label: "Analyzing Refcal",
      success: "Refcal analysis complete",
      paceMs: 18000,
      waitForPlots: true,
      stages: [
        "Opening scan and averaging channels",
        "Applying legacy calibration corrections",
        "Fitting in-band delay solution",
        "Refreshing browser products",
      ],
    },
    phacal: {
      label: "Analyzing Phacal",
      success: "Phacal analysis complete",
      paceMs: 22000,
      waitForPlots: true,
      stages: [
        "Opening scan and aligning reference data",
        "Applying refcal and drift corrections",
        "Solving phase differences and multiband delay",
        "Refreshing browser products",
      ],
    },
    combine_refcal: {
      label: "Combining Refcals",
      success: "Refcal combination complete",
      paceMs: 12000,
      waitForPlots: true,
      stages: [
        "Loading selected refcals",
        "Merging compatible calibration products",
        "Updating combined scan state",
        "Refreshing browser products",
      ],
    },
    time_flag: {
      label: "Updating Time Flags",
      success: "Time-flag update complete",
      paceMs: 9000,
      waitForPlots: true,
      stages: [
        "Applying time-flag interval",
        "Re-averaging masked channels",
        "Refreshing plot products",
        "Updating browser display",
      ],
    },
    inband_mask: {
      label: "Applying In-Band Mask",
      success: "In-band mask applied",
      paceMs: 7000,
      waitForPlots: false,
      stages: [
        "Applying staged band mask",
        "Recomputing active in-band delay",
        "Refreshing relative-phase diagnostics",
        "Updating display panels",
      ],
    },
    active_delay: {
      label: "Updating In-Band Delay",
      success: "In-band delay updated",
      paceMs: 8000,
      waitForPlots: false,
      stages: [
        "Updating active delay values",
        "Recomputing corrected channels",
        "Refreshing affected diagnostics",
        "Updating display panels",
      ],
    },
    relative_delay: {
      label: "Updating Relative-Phase Fit",
      success: "Relative-phase fit updated",
      paceMs: 6000,
      waitForPlots: false,
      stages: [
        "Updating manual fit-delay values",
        "Recomputing fit overlays",
        "Refreshing residual diagnostics",
        "Updating display panels",
      ],
    },
    save_sql: {
      label: "Saving SQL",
      success: "Saved scan to SQL",
      paceMs: 7000,
      waitForPlots: false,
      stages: [
        "Preparing calibrated scan product",
        "Writing calibration arrays to SQL",
        "Refreshing saved-state metadata",
        "Updating browser state",
      ],
    },
    save_calibeovsa_npz: {
      label: "Saving calibeovsa NPZ",
      success: "calibeovsa NPZ bundle saved",
      paceMs: 7000,
      waitForPlots: false,
      stages: [
        "Collecting active refcal and analyzed phacals",
        "Building SQL-equivalent export arrays",
        "Writing calibeovsa bundle under /common/webplots/phasecal",
        "Updating browser state",
      ],
    },
  };

  async function jsonFetch(url, options) {
    const response = await fetch(url, options || {});
    const text = await response.text();
    let data = {};
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (_err) {
        if (!response.ok) {
          throw new Error(text || response.statusText || "Request failed");
        }
        throw new Error("Invalid JSON response.");
      }
    }
    if (!response.ok) {
      throw new Error((data && data.detail) || text || response.statusText || "Request failed");
    }
    return data;
  }

  function todayIso() {
    return new Date().toISOString().slice(0, 10);
  }

  function trimZeros(text) {
    return text.replace(/\.?0+$/, "");
  }

  function formatNumber(value) {
    if (!Number.isFinite(value)) {
      return "";
    }
    const abs = Math.abs(value);
    if (abs >= 100 || abs === 0) {
      return String(Math.round(value));
    }
    if (abs >= 10) {
      return trimZeros(value.toFixed(1));
    }
    if (abs >= 1) {
      return trimZeros(value.toFixed(2));
    }
    return trimZeros(value.toFixed(3));
  }

  function formatEditorValue(value) {
    return Number.isFinite(Number(value)) ? formatNumber(Number(value)) : "";
  }

  function jdToDate(jd) {
    if (!Number.isFinite(jd)) {
      return null;
    }
    return new Date((jd - 2440587.5) * 86400000);
  }

  function formatUtcTime(jd) {
    const date = jdToDate(jd);
    if (!date || Number.isNaN(date.getTime())) {
      return "--:--:--";
    }
    return date.toISOString().slice(11, 19);
  }

  function formatJd(jd) {
    if (!Number.isFinite(jd)) {
      return "";
    }
    return jd.toFixed(6);
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function parseOptionalFloat(value) {
    if (value === null || value === undefined || value === "") {
      return null;
    }
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function hexToRgb(hex) {
    const clean = String(hex || "").replace("#", "");
    const full = clean.length === 3
      ? clean
          .split("")
          .map(function (digit) {
            return digit + digit;
          })
          .join("")
      : clean;
    const intValue = parseInt(full, 16);
    return {
      r: (intValue >> 16) & 255,
      g: (intValue >> 8) & 255,
      b: intValue & 255,
    };
  }

  function rgbToHex(rgb) {
    function part(value) {
      const clamped = clamp(Math.round(value), 0, 255);
      return clamped.toString(16).padStart(2, "0");
    }
    return "#" + part(rgb.r) + part(rgb.g) + part(rgb.b);
  }

  function interpolateColor(a, b, t) {
    return rgbToHex({
      r: a.r + (b.r - a.r) * t,
      g: a.g + (b.g - a.g) * t,
      b: a.b + (b.b - a.b) * t,
    });
  }

  const VIRIDIS_STOPS = [
    { t: 0.0, color: "#440154" },
    { t: 0.13, color: "#482777" },
    { t: 0.25, color: "#3f4a8a" },
    { t: 0.38, color: "#31688e" },
    { t: 0.5, color: "#26828e" },
    { t: 0.63, color: "#1f9e89" },
    { t: 0.75, color: "#35b779" },
    { t: 0.88, color: "#6ece58" },
    { t: 1.0, color: "#fde725" },
  ];

  function viridisColor(norm) {
    const t = clamp(norm, 0, 1);
    for (let idx = 1; idx < VIRIDIS_STOPS.length; idx += 1) {
      const hi = VIRIDIS_STOPS[idx];
      const lo = VIRIDIS_STOPS[idx - 1];
      if (t <= hi.t + 1e-9) {
        const localT = (t - lo.t) / Math.max(hi.t - lo.t, 1e-9);
        return interpolateColor(hexToRgb(lo.color), hexToRgb(hi.color), localT);
      }
    }
    return VIRIDIS_STOPS[VIRIDIS_STOPS.length - 1].color;
  }

  function linearTicks(min, max, count) {
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return [];
    }
    if (Math.abs(max - min) < 1e-9) {
      return [min];
    }
    const out = [];
    for (let idx = 0; idx < count; idx += 1) {
      out.push(min + ((max - min) * idx) / (count - 1));
    }
    return out;
  }

  function panelSelectionKey(sectionId, rowIdx, panelIdx) {
    return [sectionId, rowIdx, panelIdx].join(":");
  }

  function targetMaskKey(rowIdx, panelIdx) {
    return [rowIdx, panelIdx].join(":");
  }

  function normalizeJdInterval(startJd, endJd) {
    return {
      start_jd: Math.min(Number(startJd), Number(endJd)),
      end_jd: Math.max(Number(startJd), Number(endJd)),
    };
  }

  function timeFlagScopeLabel(scope) {
    const match = TIME_FLAG_SCOPES.find(function (item) {
      return item.id === scope;
    });
    return match ? match.label : String(scope || "");
  }

  function mergedSectionHeading(rowLabels) {
    const labels = Array.isArray(rowLabels) ? rowLabels.filter(Boolean) : [];
    if (!labels.length) {
      return "";
    }
    if (labels.length === 2) {
      const first = String(labels[0]).replace(/^XX?\s+/i, "").trim();
      const second = String(labels[1]).replace(/^YY?\s+/i, "").trim();
      if (first && first === second) {
        return abbreviateWallLabel("X & Y " + first);
      }
    }
    return abbreviateWallLabel(labels.join(" · "));
  }

  function abbreviateWallLabel(text) {
    let next = String(text || "");
    WALL_ABBREVIATIONS.forEach(function (item) {
      next = next.replace(item[0], item[1]);
    });
    return next;
  }

  function workflowGuideState(options) {
    const state = options && options.state ? options.state : null;
    const checkedScans = options && Array.isArray(options.checkedScans) ? options.checkedScans : [];
    const compareData = options ? options.compareData : null;
    const isPhacalScan = !!(options && options.isPhacalScan);
    const activeRef = state && state.active_refcal ? state.active_refcal : null;
    const activePhacal = state && state.active_phacal ? state.active_phacal : null;
    const scans = state && Array.isArray(state.scans) ? state.scans : [];
    const sameFeedPair =
      checkedScans.length === 2 &&
      String(checkedScans[0].feed_kind || "") === String(checkedScans[1].feed_kind || "");
    const analyzedSecondaryNotUsed =
      !!activeRef &&
      scans.some(function (scan) {
        return (
          intOrNull(scan.scan_id) !== intOrNull(activeRef.scan_id) &&
          (!!scan.analyzed || String(scan.status || "").indexOf("refcal") >= 0) &&
          String(scan.feed_kind || "") === String(activeRef.feed_kind || "") &&
          intOrNull(scan.scan_id) !== intOrNull(state.secondary_ref_scan_id)
        );
      });

    if (!state) {
      return {
        title: "Load a day",
        next: "Pick a date and wait for the scan list to load.",
        steps: [
          "Choose a candidate refcal scan first.",
          "If you have morning and evening refcals of the same feed, you will analyze them separately before comparing them.",
        ],
      };
    }

    if (compareData) {
      return {
        title: "Compare 2 same-feed refcals",
        next: "Choose the cleaner scan as Canonical. Set the other as Secondary only if it helps donor antennas or drift transport.",
        steps: [
          "Compare Sum X & Y Phase first. Prefer the scan with fewer missing antennas and fewer obvious bad columns.",
          "Then compare Inband Fit and Relative Phase + Fit. The better canonical anchor is the one with cleaner in-band structure and more usable antennas after tuning.",
          "Set Secondary only if it helps donor antennas or drift. Otherwise leave it unset; it contributes nothing.",
          "After Canonical and optional Secondary are chosen, close compare and tune the canonical refcal before analyzing any phacal.",
        ],
      };
    }

    if (!activeRef) {
      return {
        title: "Start with a refcal",
        next: sameFeedPair
          ? "Analyze the two selected same-feed refcals one by one. Then click Compare 2 Anchors."
          : "Select one refcal candidate and click Analyze Refcal. If it looks good, click Set Refcal.",
        steps: [
          "One-anchor path: analyze one refcal, inspect it, then Set Refcal when you trust it.",
          "Two-anchor same-feed path: analyze morning and evening separately, multi-select the two rows, click Compare 2 Anchors, then choose Canonical and optional Secondary.",
          "HI and LO are handled independently. The LO/HI Combine 2 Refcals path is only for true feed-pair combination, not for morning/evening comparison.",
        ],
      };
    }

    if (isPhacalScan && activePhacal) {
      return {
        title: "Solve phacal against the anchor",
        next: "Read Refcal vs Phacal Phase first, then fix obvious slope failures directly in Anchor-Ref. Phase with masking and refit.",
        steps: [
          "If Refcal phase is bad but Phacal phase is good for one antenna, use Temp Fallback for that antenna only.",
          "Anchor-Ref. Phase is the primary solve surface. Mask bad frequencies there, Preview, then Commit when the slope looks right.",
          "Use the residual panels only after the multiband solve looks reasonable. They are advanced QA, not the first solve step.",
          "When the phacal solve is acceptable, Save SQL. Save calibeovsa NPZ whenever you want a SQL-equivalent bundle of the active refcal and analyzed phacals.",
        ],
        note: activePhacal.secondary_anchor_scan_id
          ? "Secondary anchor is active as donor/drift support only. The canonical anchor remains the model source."
          : analyzedSecondaryNotUsed
            ? "Another same-feed refcal has been analyzed, but it is not part of the solution because you did not set it as Secondary."
            : "If you later want donor/drift support, set a same-feed secondary refcal and re-analyze the phacal.",
      };
    }

    if (isPhacalScan && !activePhacal) {
      return {
        title: "Analyze the selected phacal",
        next: "Keep the current Canonical refcal, then click Analyze Phacal on the selected phacal row.",
        steps: [
          "Do not move to phacal until the canonical refcal looks trustworthy.",
          "If you have a second same-feed refcal that helps missing antennas or drift, set it as Secondary before analyzing the phacal.",
          "After Analyze Phacal, start with Refcal vs Phacal Phase, then move to Anchor-Ref. Phase.",
        ],
      };
    }

    return {
      title: "Tune the canonical refcal",
      next: "Inspect the canonical refcal in order: Sum X & Y Phase, Inband Fit, Relative Phase + Fit, then Per-Band Residual Phase. When it is stable, move to phacals.",
      steps: [
        "If you have two same-feed refcals, analyze both separately first. Use Compare 2 Anchors to choose Canonical and optional Secondary.",
        "Canonical is the real anchor. Secondary is donor/drift support only, mainly for missing or unusable antennas in the canonical anchor.",
        "Tune gross in-band delay first, then multiband relative phase, then residual cleanup.",
        "Once you are satisfied with the refcal, select a phacal row and click Analyze Phacal.",
      ],
      note: state.secondary_ref_scan_id !== null
        ? "Secondary anchor is already set. It will be used only for donor patching and drift transport where applicable."
        : analyzedSecondaryNotUsed
          ? "A second same-feed refcal has been analyzed, but it is not part of the solution until you explicitly set it as Secondary."
          : "If one morning/evening same-feed refcal has better donor antennas than the canonical anchor, set it as Secondary before moving to phacal.",
    };
  }

  function intOrNull(value) {
    return value === null || value === undefined ? null : Number(value);
  }

  function sameKeptRanges(left, right) {
    const a = Array.isArray(left) ? left : [];
    const b = Array.isArray(right) ? right : [];
    if (a.length !== b.length) {
      return false;
    }
    for (let idx = 0; idx < a.length; idx += 1) {
      if (
        Number(a[idx].start_band) !== Number(b[idx].start_band) ||
        Number(a[idx].end_band) !== Number(b[idx].end_band)
      ) {
        return false;
      }
    }
    return true;
  }

  function useMeasuredWidth(ref, fallbackWidth) {
    const [width, setWidth] = useState(fallbackWidth);

    useEffect(
      function () {
        if (!ref.current || typeof window === "undefined") {
          return;
        }
        function updateWidth() {
          if (!ref.current) {
            return;
          }
          const nextWidth = Math.max(fallbackWidth, Math.round(ref.current.getBoundingClientRect().width || fallbackWidth));
          setWidth(nextWidth);
        }
        updateWidth();
        let resizeObserver = null;
        if (typeof ResizeObserver !== "undefined") {
          resizeObserver = new ResizeObserver(updateWidth);
          resizeObserver.observe(ref.current);
        }
        window.addEventListener("resize", updateWidth);
        return function () {
          window.removeEventListener("resize", updateWidth);
          if (resizeObserver) {
            resizeObserver.disconnect();
          }
        };
      },
      [ref, fallbackWidth]
    );

    return width;
  }

  function expandTimeFlagTargets(interval, layout) {
    if (!interval || !layout) {
      return [];
    }
    const ant = clamp(Number(interval.antenna || 0), 0, Math.max(0, Number(layout.nsolant || 1) - 1));
    const band = clamp(Number(interval.band || 0), 0, Math.max(0, Number(layout.maxnbd || 1) - 1));
    const scope = String(interval.scope || "selected");
    if (scope === "selected") {
      return [{ antenna: ant, band: band }];
    }
    if (scope === "this_ant") {
      return Array.from({ length: Number(layout.maxnbd || 0) }).map(function (_item, idx) {
        return { antenna: ant, band: idx };
      });
    }
    if (scope === "this_band") {
      return Array.from({ length: Number(layout.nsolant || 0) }).map(function (_item, idx) {
        return { antenna: idx, band: band };
      });
    }
    if (scope === "higher_bands") {
      return Array.from({ length: Math.max(0, Number(layout.maxnbd || 0) - band) }).map(function (_item, idx) {
        return { antenna: ant, band: band + idx };
      });
    }
    if (scope === "all") {
      const out = [];
      for (let antIdx = 0; antIdx < Number(layout.nsolant || 0); antIdx += 1) {
        for (let bandIdx = 0; bandIdx < Number(layout.maxnbd || 0); bandIdx += 1) {
          out.push({ antenna: antIdx, band: bandIdx });
        }
      }
      return out;
    }
    return [];
  }

  function mergeTimeFlagIntervals(intervals, nextInterval) {
    const normalized = normalizeJdInterval(nextInterval.start_jd, nextInterval.end_jd);
    const mergedSeed = Object.assign({}, nextInterval, normalized);
    const tol = 1.0e-9;
    const kept = [];
    let merged = mergedSeed;
    (intervals || []).forEach(function (item) {
      const sameTarget =
        Number(item.antenna) === Number(merged.antenna)
        && Number(item.band) === Number(merged.band)
        && String(item.scope) === String(merged.scope);
      const overlaps = !(
        Number(item.end_jd) < Number(merged.start_jd) - tol
        || Number(merged.end_jd) < Number(item.start_jd) - tol
      );
      if (sameTarget && overlaps) {
        merged = Object.assign({}, merged, {
          temp_id: String(item.temp_id || merged.temp_id),
          start_jd: Math.min(Number(merged.start_jd), Number(item.start_jd)),
          end_jd: Math.max(Number(merged.end_jd), Number(item.end_jd)),
        });
      } else {
        kept.push(item);
      }
    });
    kept.push(merged);
    kept.sort(function (a, b) {
      return Number(a.start_jd) - Number(b.start_jd) || Number(a.end_jd) - Number(b.end_jd);
    });
    return kept;
  }

  function intersectKeptRanges(rangesA, rangesB, bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    const maskA = bandMaskFromRanges(rangesA, bandEdges);
    const maskB = bandMaskFromRanges(rangesB, bandEdges);
    return keptRangesFromMask(
      maskA.map(function (value, idx) {
        return Boolean(value) && Boolean(maskB[idx]);
      }),
      bandEdges
    );
  }

  function defaultKeptRanges(bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    return [
      {
        start_band: Number(bandEdges[0].band),
        end_band: Number(bandEdges[bandEdges.length - 1].band),
      },
    ];
  }

  function bandMaskFromRanges(ranges, bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    const effectiveRanges = ranges && ranges.length ? ranges : defaultKeptRanges(bandEdges);
    return bandEdges.map(function (edge) {
      return effectiveRanges.some(function (range) {
        return Number(edge.band) >= Number(range.start_band) && Number(edge.band) <= Number(range.end_band);
      });
    });
  }

  function keptRangesFromMask(mask, bandEdges) {
    if (!bandEdges || !bandEdges.length || !mask || !mask.length) {
      return [];
    }
    const ranges = [];
    let startBand = null;
    let previousBand = null;
    bandEdges.forEach(function (edge, idx) {
      const band = Number(edge.band);
      if (mask[idx]) {
        if (startBand === null) {
          startBand = band;
        } else if (previousBand !== null && band !== previousBand + 1) {
          ranges.push({ start_band: startBand, end_band: previousBand });
          startBand = band;
        }
        previousBand = band;
      } else if (startBand !== null && previousBand !== null) {
        ranges.push({ start_band: startBand, end_band: previousBand });
        startBand = null;
        previousBand = null;
      }
    });
    if (startBand !== null && previousBand !== null) {
      ranges.push({ start_band: startBand, end_band: previousBand });
    }
    return ranges;
  }

  function excludedRangesFromKeptRanges(ranges, bandEdges) {
    if (!bandEdges || !bandEdges.length) {
      return [];
    }
    const keptMask = bandMaskFromRanges(ranges, bandEdges);
    return keptRangesFromMask(
      keptMask.map(function (value) {
        return !value;
      }),
      bandEdges
    );
  }

  function formatKeptRangesLabel(ranges, bandEdges) {
    const effectiveRanges = ranges && ranges.length ? ranges : defaultKeptRanges(bandEdges);
    if (!effectiveRanges.length) {
      return "—";
    }
    return effectiveRanges
      .map(function (range) {
        return Number(range.start_band) === Number(range.end_band)
          ? String(range.start_band)
          : String(range.start_band) + "-" + String(range.end_band);
      })
      .join(", ");
  }

  function updateKeptRanges(ranges, bandEdges, startBand, endBand, mode) {
    if (!bandEdges || !bandEdges.length) {
      return ranges || [];
    }
    const lo = Math.min(Number(startBand), Number(endBand));
    const hi = Math.max(Number(startBand), Number(endBand));
    const selectionMask = bandEdges.map(function (edge) {
      const band = Number(edge.band);
      return band >= lo && band <= hi;
    });
    const candidateMask = selectionMask.map(function (selected) {
      return !selected;
    });
    if (!candidateMask.some(Boolean)) {
      return defaultKeptRanges(bandEdges);
    }
    return keptRangesFromMask(candidateMask, bandEdges);
  }

  function excludeRangeFromKeptRanges(ranges, bandEdges, startBand, endBand) {
    if (!bandEdges || !bandEdges.length) {
      return ranges || [];
    }
    const currentMask = bandMaskFromRanges(ranges, bandEdges);
    const lo = Math.min(Number(startBand), Number(endBand));
    const hi = Math.max(Number(startBand), Number(endBand));
    const candidateMask = currentMask.map(function (keep, idx) {
      const band = Number(bandEdges[idx].band);
      return Boolean(keep) && !(band >= lo && band <= hi);
    });
    if (!candidateMask.some(Boolean)) {
      return ranges && ranges.length ? ranges : defaultKeptRanges(bandEdges);
    }
    return keptRangesFromMask(candidateMask, bandEdges);
  }

  function optimisticPanelUpdate(panel, nextRanges, bandEdges) {
    const nextPanel = Object.assign({}, panel, { kept_ranges: nextRanges });
    if (panel && typeof panel.annotation === "string" && panel.annotation.indexOf("| kept ") >= 0) {
      nextPanel.annotation = panel.annotation.replace(/\| kept .*/, "| kept " + formatKeptRangesLabel(nextRanges, bandEdges));
    }
    if (panel && Array.isArray(panel.series)) {
      const keptMask = bandMaskFromRanges(nextRanges, bandEdges);
      const keptBandSet = new Set();
      (bandEdges || []).forEach(function (edge, idx) {
        if (keptMask[idx]) {
          keptBandSet.add(Number(edge.band));
        }
      });
      nextPanel.series = panel.series.map(function (series) {
        if (!series || series.role !== "data" || series.band === undefined) {
          return series;
        }
        const inKept = keptBandSet.has(Number(series.band));
        return Object.assign({}, series, { opacity: inKept ? 0.95 : 0.18 });
      });
    }
    return nextPanel;
  }

  function mergeSectionAntennaPanels(currentSection, updateSection, antennaIndex) {
    if (!updateSection || !updateSection.panels) {
      return updateSection || currentSection;
    }
    const sparseAntennas = Array.isArray(updateSection.sparse_antennas)
      ? updateSection.sparse_antennas
          .map(function (value) {
            return Number(value);
          })
          .filter(function (value) {
            return Number.isFinite(value) && value >= 0;
          })
      : null;
    const antennaSet =
      sparseAntennas && sparseAntennas.length
        ? new Set(sparseAntennas)
        : Number.isFinite(antennaIndex) && antennaIndex >= 0
        ? new Set([antennaIndex])
        : null;
    if (!antennaSet) {
      return updateSection || currentSection;
    }
    const currentPanels = currentSection && currentSection.panels ? currentSection.panels : [];
    const mergedPanels = (updateSection.panels || []).map(function (row, rowIdx) {
      const currentRow = currentPanels[rowIdx] || [];
      return row.map(function (panel, panelIdx) {
        if (antennaSet.has(panelIdx)) {
          return panel || currentRow[panelIdx] || panel;
        }
        return currentRow[panelIdx] || panel;
      });
    });
    return Object.assign({}, currentSection || {}, updateSection, { panels: mergedPanels });
  }

  function mergeOverviewAntennaUpdates(current, updates, antennaIndex) {
    if (!updates) {
      return current;
    }
    const next = Object.assign({}, current || {});
    Object.keys(updates).forEach(function (sectionId) {
      next[sectionId] = mergeSectionAntennaPanels(next[sectionId], updates[sectionId], antennaIndex);
    });
    return next;
  }

  function legendItem(label, color, mode) {
    return html`
      <div className="series-legend-item" key=${label + "-" + color + "-" + (mode || "points")}>
        <span className=${"series-swatch " + (mode === "line" ? "series-swatch-line" : "series-swatch-point")} style=${{ "--swatch-color": color }}></span>
        <span>${label}</span>
      </div>
    `;
  }

  function seriesPairs(series) {
    const xs = (series && series.x) || [];
    const ys = (series && series.y) || [];
    const npts = Math.min(xs.length, ys.length);
    const out = [];
    for (let idx = 0; idx < npts; idx += 1) {
      const x = xs[idx];
      const y = ys[idx];
      if (Number.isFinite(x) && Number.isFinite(y)) {
        out.push([x, y]);
      }
    }
    return out;
  }

  function panelHasAnySeriesData(panel) {
    return !!((panel && panel.series) || []).some(function (series) {
      return seriesPairs(series).length > 0;
    });
  }

  function extractAnnotationDelayNs(panel) {
    if (!panel || typeof panel.annotation !== "string") {
      return null;
    }
    const match = panel.annotation.match(/Δdelay=([-+0-9.eE]+)/);
    if (!match) {
      return null;
    }
    const value = Number(match[1]);
    return Number.isFinite(value) ? value : null;
  }

  function columnPanels(section, antennaIndex) {
    if (!section || !Array.isArray(section.panels)) {
      return [];
    }
    return section.panels
      .map(function (row) {
        return Array.isArray(row) ? row[antennaIndex] : null;
      })
      .filter(Boolean);
  }

  function columnHasAnyData(section, antennaIndex) {
    return columnPanels(section, antennaIndex).some(function (panel) {
      return panelHasAnySeriesData(panel);
    });
  }

  function columnHasUsableMultibandSuggestion(section, antennaIndex, isPhacal) {
    return columnPanels(section, antennaIndex).some(function (panel) {
      const delayNs = extractAnnotationDelayNs(panel);
      if (delayNs !== null && Math.abs(delayNs) > 1e-9) {
        return true;
      }
      if (
        isPhacal &&
        panel &&
        typeof panel.annotation === "string" &&
        /\bfit\b/i.test(panel.annotation) &&
        panelHasAnySeriesData(panel)
      ) {
        return true;
      }
      return false;
    });
  }

  function polylinePath(points, xMap, yMap) {
    if (!points.length) {
      return "";
    }
    return points
      .map(function (point, idx) {
        return (idx === 0 ? "M" : "L") + xMap(point[0]).toFixed(2) + " " + yMap(point[1]).toFixed(2);
      })
      .join(" ");
  }

  function wrapPhaseResidual(value) {
    return Math.atan2(Math.sin(value), Math.cos(value));
  }

  function nearestSeriesY(points, targetX) {
    if (!points.length) {
      return null;
    }
    let bestY = null;
    let bestDist = Infinity;
    for (let idx = 0; idx < points.length; idx += 1) {
      const pair = points[idx];
      const dist = Math.abs(pair[0] - targetX);
      if (dist < bestDist) {
        bestDist = dist;
        bestY = pair[1];
      }
    }
    return bestY;
  }

  function buildInbandResidualData(data, antIdx) {
    if (!data || data.message || !data.panels || antIdx < 0) {
      return null;
    }
    const panels = [];
    let xMin = Infinity;
    let xMax = -Infinity;
    let maxAbs = 0.3;
    for (let rowIdx = 0; rowIdx < 2; rowIdx += 1) {
      const row = data.panels[rowIdx] || [];
      const panel = row[antIdx];
      const rawSeries = panel
        ? (panel.series || []).filter(function (series) {
            return series.role === "raw" || series.label === "Raw phase";
          })
        : [];
      const fitSeries = panel
        ? (panel.series || []).filter(function (series) {
            return series.role === "fit" || series.label === "Fit";
          })
        : [];
      const rawPairs = rawSeries
        .map(seriesPairs)
        .reduce(function (acc, pairs) {
          return acc.concat(pairs);
        }, []);
      const fitPairs = fitSeries
        .map(seriesPairs)
        .reduce(function (acc, pairs) {
          return acc.concat(pairs);
        }, []);
      fitPairs.sort(function (a, b) {
        return a[0] - b[0];
      });
      const residualPairs = rawPairs
        .map(function (pair) {
          const fitY = nearestSeriesY(fitPairs, pair[0]);
          if (!Number.isFinite(fitY)) {
            return null;
          }
          const residual = wrapPhaseResidual(pair[1] - fitY);
          maxAbs = Math.max(maxAbs, Math.abs(residual));
          xMin = Math.min(xMin, pair[0]);
          xMax = Math.max(xMax, pair[0]);
          return [pair[0], residual];
        })
        .filter(Boolean);
      panels.push([
        {
          title: panel && panel.title ? panel.title : "Ant " + String(antIdx + 1),
          annotation: panel && panel.annotation ? panel.annotation : null,
          series: [
            {
              label: rowIdx === 0 ? "X residual" : "Y residual",
              mode: "points",
              color: rowIdx === 0 ? COLOR_X : COLOR_Y,
              x: residualPairs.map(function (pair) {
                return pair[0];
              }),
              y: residualPairs.map(function (pair) {
                return pair[1];
              }),
            },
          ],
        },
      ]);
    }
    if (!Number.isFinite(xMin) || !Number.isFinite(xMax)) {
      return {
        message: "No fitted in-band residuals are available for this antenna.",
        title: "Inband Fit Residuals",
      };
    }
    const lim = Math.min(Math.PI, Math.max(0.4, maxAbs * 1.15));
    return {
      message: null,
      type: "panel-grid",
      title: "Detrended Phase Residuals",
      x_label: data.x_label || "Frequency [GHz]",
      row_labels: ["X Residual [rad]", "Y Residual [rad]"],
      x_limits: [xMin, xMax],
      x_ticks: data.x_ticks || { values: linearTicks(xMin, xMax, 4), labels: [] },
      y_limits: [[-lim, lim], [-lim, lim]],
      panels: panels,
      legend: [],
      auto_scale_rows: false,
    };
  }

  function HeatmapPlot(props) {
    const data = props.data;
    const [hoverCell, setHoverCell] = useState(null);
    const shellRef = useRef(null);
    const shellWidth = useMeasuredWidth(shellRef, 300);
    if (!data) {
      return html`<div className="plot-placeholder">Loading heatmap...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }
    const width = shellWidth > 0 ? shellWidth : 300;
    const height = Math.round(width * 1.05);
    const margin = { left: 48, right: 42, top: 22, bottom: 58 };
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const cellWidth = plotWidth / data.nsolant;
    const cellHeight = plotHeight / data.maxnbd;
    const selectedAnt =
      props.selectedAnt !== null && props.selectedAnt !== undefined ? props.selectedAnt : data.selected_ant;
    const selectedBand =
      props.selectedBand !== null && props.selectedBand !== undefined ? props.selectedBand : data.selected_band;
    const vmin = Number.isFinite(data.vmin) ? data.vmin : 0.0;
    const vmax = Number.isFinite(data.vmax) ? data.vmax : 2.0;
    const colorLevels = Array.isArray(data.color_levels) && data.color_levels.length ? data.color_levels : null;
    const colorBins = Array.isArray(data.color_bins) && data.color_bins.length ? data.color_bins : null;
    const pendingCellSet = new Set(
      (props.pendingCells || []).map(function (cell) {
        return String(cell.antenna) + ":" + String(cell.band);
      })
    );
    const appliedCellSet = new Set(
      ((data && data.applied_cells) || []).map(function (cell) {
        return String(cell.antenna) + ":" + String(cell.band);
      })
    );
    function colorFor(value) {
      if (colorBins) {
        for (let idx = 0; idx < colorBins.length; idx += 1) {
          const bin = colorBins[idx];
          const min = Number(bin.min);
          const max = Number(bin.max);
          if (value >= min && (value < max || (idx === colorBins.length - 1 && value <= max))) {
            return bin.color;
          }
        }
      }
      if (colorLevels) {
        const match = colorLevels.find(function (item) {
          return Number(item.value) === Number(value);
        });
        if (match) {
          return match.color;
        }
      }
      const norm = (value - vmin) / Math.max(vmax - vmin, 1e-9);
      return viridisColor(norm);
    }
    const yTicks = [];
    for (let value = 0; value <= data.maxnbd; value += 10) {
      yTicks.push(value);
    }
    const barX = margin.left + plotWidth + 16;
    const barY = margin.top;
    const barWidth = 8;
    const barHeight = plotHeight;
    const colorbarTicks = colorBins
      ? colorBins.map(function (item) {
          return 0.5 * (Number(item.min) + Number(item.max));
        })
      : colorLevels
      ? colorLevels.map(function (item) { return Number(item.value); })
      : [vmin, (vmin + vmax) / 2.0, vmax];
    const majorX = [];
    for (let idx = 0; idx <= data.nsolant; idx += 5) {
      majorX.push(idx);
    }
    if (majorX[majorX.length - 1] !== data.nsolant) {
      majorX.push(data.nsolant);
    }
    const majorY = [];
    for (let idx = 0; idx <= data.maxnbd; idx += 10) {
      majorY.push(idx);
    }
    if (majorY[majorY.length - 1] !== data.maxnbd) {
      majorY.push(data.maxnbd);
    }
    const tooltipValue =
      hoverCell && data.values && data.values[hoverCell.band] ? data.values[hoverCell.band][hoverCell.ant] : null;
    const tooltipWidth = 106;
    const tooltipHeight = 44;
    const tooltipCellX = hoverCell ? margin.left + hoverCell.ant * cellWidth : 0;
    const tooltipCellY = hoverCell ? margin.top + plotHeight - (hoverCell.band + 1) * cellHeight : 0;
    const tooltipX = clamp(tooltipCellX + cellWidth + 8, margin.left + 4, width - tooltipWidth - 6);
    const tooltipY = clamp(tooltipCellY - tooltipHeight - 6, 18, height - tooltipHeight - 18);
    function cellFromPointer(event) {
      const rect = event.currentTarget.getBoundingClientRect();
      if (!rect.width || !rect.height) {
        return null;
      }
      const localX = ((event.clientX - rect.left) / rect.width) * plotWidth;
      const localY = ((event.clientY - rect.top) / rect.height) * plotHeight;
      const antIdx = Math.max(0, Math.min(data.nsolant - 1, Math.floor(localX / cellWidth)));
      const bandFromTop = Math.max(0, Math.min(data.maxnbd - 1, Math.floor(localY / cellHeight)));
      const bandIdx = data.maxnbd - 1 - bandFromTop;
      return { ant: antIdx, band: bandIdx };
    }
    function updateHover(event) {
      const next = cellFromPointer(event);
      setHoverCell(function (current) {
        if (
          current &&
          next &&
          current.ant === next.ant &&
          current.band === next.band
        ) {
          return current;
        }
        return next;
      });
    }
    return html`
      <div className="js-plot-shell heatmap-shell" ref=${shellRef}>
        <svg
          viewBox=${"0 0 " + width + " " + height}
          className="svg-plot"
          preserveAspectRatio="xMidYMid meet"
          style=${{ aspectRatio: width + " / " + height }}
          role="img"
          aria-label=${data.title}
        >
          <defs>
            ${!colorLevels
              ? html`
                  <linearGradient id="viridis-heatmap-bar" x1="0%" y1="100%" x2="0%" y2="0%">
                    ${VIRIDIS_STOPS.map(function (stop) {
                      return html`<stop key=${"stop-" + stop.t} offset=${String(stop.t * 100) + "%"} stopColor=${stop.color} />`;
                    })}
                  </linearGradient>
                `
              : null}
          </defs>
          ${Array.from({ length: data.nsolant }).map(function (_, antIdx) {
            const x = margin.left + antIdx * cellWidth + cellWidth / 2;
            return html`
              <text
                key=${"xtick-" + antIdx}
                x=${x}
                y=${margin.top + plotHeight + 18}
                textAnchor="end"
                transform=${"rotate(-35 " + x + " " + (margin.top + plotHeight + 18) + ")"}
                className="axis-label"
              >
                ${String(antIdx + 1)}
              </text>
            `;
          })}

          ${yTicks.map(function (tick) {
            const y = margin.top + plotHeight - (tick / data.maxnbd) * plotHeight;
            return html`
              <g key=${"ytick-" + tick}>
                <line x1=${margin.left} x2=${margin.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
                <line x1=${margin.left - 6} x2=${margin.left} y1=${y} y2=${y} className="axis-line" />
                <text x=${margin.left - 10} y=${y + 4} textAnchor="end" className="axis-label">${String(tick)}</text>
              </g>
            `;
          })}

          ${data.values.map(function (row, bandIdx) {
            return row.map(function (value, antIdx) {
              const x = margin.left + antIdx * cellWidth;
              const y = margin.top + plotHeight - (bandIdx + 1) * cellHeight;
              return html`
                <rect
                  key=${"cell-" + antIdx + "-" + bandIdx}
                  x=${x}
                  y=${y}
                  width=${cellWidth}
                  height=${cellHeight}
                  fill=${colorFor(value)}
                  className="heatmap-cell"
                />
              `;
            });
          })}

          ${data.values.map(function (row, bandIdx) {
            return row.map(function (_value, antIdx) {
              const key = String(antIdx) + ":" + String(bandIdx);
              if (!appliedCellSet.has(key)) {
                return null;
              }
              return html`<rect
                key=${"applied-" + key}
                x=${margin.left + antIdx * cellWidth + 0.9}
                y=${margin.top + plotHeight - (bandIdx + 1) * cellHeight + 0.9}
                width=${Math.max(cellWidth - 1.8, 0.8)}
                height=${Math.max(cellHeight - 1.8, 0.8)}
                fill="none"
                stroke="rgba(0, 113, 227, 0.58)"
                strokeWidth="1.15"
              />`;
            });
          })}

          ${data.values.map(function (row, bandIdx) {
            return row.map(function (_value, antIdx) {
              const key = String(antIdx) + ":" + String(bandIdx);
              if (!pendingCellSet.has(key)) {
                return null;
              }
              return html`<rect
                key=${"pending-" + key}
                x=${margin.left + antIdx * cellWidth + 0.5}
                y=${margin.top + plotHeight - (bandIdx + 1) * cellHeight + 0.5}
                width=${Math.max(cellWidth - 1.0, 1.0)}
                height=${Math.max(cellHeight - 1.0, 1.0)}
                fill="none"
                stroke="#1f6feb"
                strokeWidth="2.2"
              />`;
            });
          })}

          ${majorX.map(function (idx) {
            const x = margin.left + idx * cellWidth;
            return html`<line key=${"vx-major-" + idx} x1=${x} x2=${x} y1=${margin.top} y2=${margin.top + plotHeight} className="heatmap-major-line" />`;
          })}
          ${majorY.map(function (idx) {
            const y = margin.top + plotHeight - idx * cellHeight;
            return html`<line key=${"hy-major-" + idx} x1=${margin.left} x2=${margin.left + plotWidth} y1=${y} y2=${y} className="heatmap-major-line" />`;
          })}

          ${selectedAnt !== null && selectedBand !== null
            ? html`<rect
                x=${margin.left + selectedAnt * cellWidth}
                y=${margin.top + plotHeight - (selectedBand + 1) * cellHeight}
                width=${cellWidth}
                height=${cellHeight}
                fill="none"
                stroke="#ff2f00"
                strokeWidth="2.5"
              />`
            : null}
          ${hoverCell && (selectedAnt !== hoverCell.ant || selectedBand !== hoverCell.band)
            ? html`<rect
                x=${margin.left + hoverCell.ant * cellWidth}
                y=${margin.top + plotHeight - (hoverCell.band + 1) * cellHeight}
                width=${cellWidth}
                height=${cellHeight}
                className="heatmap-hover-box"
              />`
            : null}

          <rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} className="plot-frame" />

          <text x=${margin.left + plotWidth / 2} y=${height - 8} textAnchor="middle" className="axis-label">${data.x_label}</text>
          <text
            x="18"
            y=${margin.top + plotHeight / 2}
            transform=${"rotate(-90 18 " + (margin.top + plotHeight / 2) + ")"}
            className="axis-label"
          >
            ${data.y_label}
          </text>

          ${colorBins
            ? colorBins.map(function (bin, idx) {
                const denom = Math.max(vmax - vmin, 1e-9);
                const yTop = barY + (1.0 - (Number(bin.max) - vmin) / denom) * barHeight;
                const yBottom = barY + (1.0 - (Number(bin.min) - vmin) / denom) * barHeight;
                return html`
                  <rect
                    key=${"bar-bin-" + idx}
                    x=${barX}
                    y=${Math.min(yTop, yBottom)}
                    width=${barWidth}
                    height=${Math.max(Math.abs(yBottom - yTop), 1)}
                    fill=${bin.color}
                  />
                `;
              })
            : colorLevels
            ? colorLevels
                .slice()
                .sort(function (a, b) {
                  return Number(a.value) - Number(b.value);
                })
                .map(function (level, idx, items) {
                  const bandHeight = barHeight / Math.max(items.length, 1);
                  const displayIdx = items.length - 1 - idx;
                  return html`
                    <rect
                      key=${"bar-fill-" + level.value}
                      x=${barX}
                      y=${barY + displayIdx * bandHeight}
                      width=${barWidth}
                      height=${bandHeight}
                      fill=${level.color}
                    />
                  `;
                })
            : html`<rect x=${barX} y=${barY} width=${barWidth} height=${barHeight} fill="url(#viridis-heatmap-bar)" />`}
          ${colorBins
            ? colorBins.map(function (bin, idx) {
                const y = barY + (1.0 - (0.5 * (Number(bin.min) + Number(bin.max)) - vmin) / Math.max(vmax - vmin, 1e-9)) * barHeight;
                return html`
                  <g key=${"bar-bin-label-" + idx}>
                    <line x1=${barX + barWidth} x2=${barX + barWidth + 6} y1=${y} y2=${y} className="axis-line" />
                    <text x=${barX + barWidth + 10} y=${y + 4} className="axis-label">${String(bin.label)}</text>
                  </g>
                `;
              })
            : colorLevels
            ? colorLevels
                .slice()
                .sort(function (a, b) {
                  return Number(a.value) - Number(b.value);
                })
                .map(function (level, idx, items) {
                  const bandHeight = barHeight / Math.max(items.length, 1);
                  const displayIdx = items.length - 1 - idx;
                  const y = barY + (displayIdx + 0.5) * bandHeight;
                  return html`
                    <g key=${"bar-tick-" + level.value}>
                      <line x1=${barX + barWidth} x2=${barX + barWidth + 6} y1=${y} y2=${y} className="axis-line" />
                      <text x=${barX + barWidth + 10} y=${y + 4} className="axis-label">${String(Math.round(Number(level.value)))}</text>
                    </g>
                  `;
                })
            : colorbarTicks.map(function (tick) {
                const y = barY + (1.0 - (tick - vmin) / Math.max(vmax - vmin, 1e-9)) * barHeight;
                return html`
                  <g key=${"bar-" + tick}>
                    <line x1=${barX + barWidth} x2=${barX + barWidth + 6} y1=${y} y2=${y} className="axis-line" />
                    <text x=${barX + barWidth + 10} y=${y + 4} className="axis-label">${String(Math.round(tick))}</text>
                  </g>
                `;
              })}
          <rect x=${barX} y=${barY} width=${barWidth} height=${barHeight} className="plot-frame" />
          <text x=${barX + barWidth / 2} y=${barY - 8} textAnchor="middle" className="axis-label">
            ${data.colorbar_label || ""}
          </text>
          <rect
            x=${margin.left}
            y=${margin.top}
            width=${plotWidth}
            height=${plotHeight}
            fill="rgba(0,0,0,0)"
            className="heatmap-hit-area"
            onMouseMove=${updateHover}
            onMouseLeave=${function () {
              setHoverCell(null);
            }}
            onClick=${function (event) {
              const next = cellFromPointer(event);
              if (next) {
                props.onSelect(next.ant, next.band);
              }
            }}
          />
          ${hoverCell
            ? html`<g className="heatmap-tooltip">
                <rect x=${tooltipX} y=${tooltipY} width=${tooltipWidth} height=${tooltipHeight} rx="9" className="heatmap-tooltip-box" />
                <text x=${tooltipX + 8} y=${tooltipY + 16} className="heatmap-tooltip-text">
                  ${"Ant " + String(hoverCell.ant + 1) + ", Band " + String(hoverCell.band + 1)}
                </text>
                <text x=${tooltipX + 8} y=${tooltipY + 32} className="heatmap-tooltip-subtext">
                  ${"Flag Sum: " + formatNumber(Number(tooltipValue))}
                </text>
              </g>`
            : null}
        </svg>
      </div>
    `;
  }

  function MiniPanelPlot(props) {
    const [dragState, setDragState] = useState(null);
    const [slopeState, setSlopeState] = useState(null);
    const [showHint, setShowHint] = useState(false);
    const [hintPos, setHintPos] = useState({ x: 0, y: 0 });
    const hintTimerRef = useRef(null);
    const containerRef = useRef(null);
    const clipIdRef = useRef("mini-clip-" + Math.random().toString(36).slice(2));
    const panel = props.panel || {};
    const xLimits = props.xLimits || [0, 1];
    const yLimits = props.autoYLimits || props.yLimits || [-1, 1];
    const xTicks = (props.xTicks && props.xTicks.values) || [];
    const xTickLabels = (props.xTicks && props.xTicks.labels) || [];
    const bandEdges = props.bandEdges || [];
    const width = PANEL_GRID_PANEL_WIDTH;
    const height = props.panelHeight || 128;
    const showTitle = props.showTitle !== false;
    const showXAxisLabels = props.showXAxisLabels !== false;
    const margin = {
      left: 8,
      right: 8,
      top: 8,
      bottom: showXAxisLabels ? 24 : 8,
    };
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    const xMin = xLimits[0];
    const xMax = xLimits[1];
    const yMin = yLimits[0];
    const yMax = yLimits[1];
    const yTicks = linearTicks(yMin, yMax, 3);
    const activeRanges = panel.kept_ranges || [];
    const interactionMode = props.interactionMode || null;
    const interactiveTitle =
      interactionMode === "bandselect"
        ? "Drag to stage flagged range. Click " + (props.bandSelectApplyLabel || "Apply Mask") + " to apply."
            + (props.onDoubleClick ? " Double-click to clear mask." : "")
        : interactionMode === "zoom"
          ? "Drag to zoom. Shift + drag to pan. Double-click to reset."
          : props.onDoubleClick
            ? "Double-click for detailed residual view"
            : null;
    const hintText =
      interactionMode === "bandselect"
        ? "Drag to stage flagged range · Click " + (props.bandSelectApplyLabel || "Apply Mask")
            + (props.onDoubleClick ? " · Double-click to clear mask" : "")
        : interactionMode === "zoom"
          ? "Drag to zoom · Shift+drag to pan · Double-click to reset"
          : null;
    function xMap(value) {
      const denom = xMax - xMin || 1.0;
      return margin.left + ((value - xMin) / denom) * plotWidth;
    }
    function yMap(value) {
      const denom = yMax - yMin || 1.0;
      return margin.top + ((yMax - value) / denom) * plotHeight;
    }
    function xValueFromClientX(event) {
      // The hitbox <rect> has width == plotWidth in SVG user units, so the
      // pixel fraction within the rect maps directly to the plot fraction —
      // no margin offset or full viewBox width is involved.
      const rect = event.currentTarget.getBoundingClientRect();
      if (!rect.width) {
        return null;
      }
      const fraction = clamp((event.clientX - rect.left) / rect.width, 0, 1);
      return xMin + fraction * (xMax - xMin || 1.0);
    }
    function yValueFromClientY(event) {
      const rect = event.currentTarget.getBoundingClientRect();
      if (!rect.height) {
        return null;
      }
      const fraction = clamp((event.clientY - rect.top) / rect.height, 0, 1);
      return yMax - fraction * (yMax - yMin || 1.0);
    }
    function bandFromXValue(xValue) {
      if (!Number.isFinite(xValue) || !bandEdges.length) {
        return null;
      }
      for (let idx = 0; idx < bandEdges.length; idx += 1) {
        const edge = bandEdges[idx];
        if (xValue >= edge.x_min && xValue <= edge.x_max) {
          return edge.band;
        }
      }
      let bestBand = null;
      let bestDist = Infinity;
      bandEdges.forEach(function (edge) {
        const dist = Math.abs(xValue - edge.x_center);
        if (dist < bestDist) {
          bestDist = dist;
          bestBand = edge.band;
        }
      });
      return bestBand;
    }
    function rangeRect(startBand, endBand) {
      if (!Number.isFinite(startBand) || !Number.isFinite(endBand) || !bandEdges.length) {
        return null;
      }
      const lo = Math.min(startBand, endBand);
      const hi = Math.max(startBand, endBand);
      const included = bandEdges.filter(function (edge) {
        return edge.band >= lo && edge.band <= hi;
      });
      if (!included.length) {
        return null;
      }
      const x0 = xMap(included[0].x_min);
      const x1 = xMap(included[included.length - 1].x_max);
      return { x: x0, width: Math.max(x1 - x0, 1.5) };
    }
    function rangeRects(ranges) {
      return (ranges || [])
        .map(function (item) {
          return rangeRect(item.start_band, item.end_band);
        })
        .filter(Boolean);
    }
    function isFullRangeSelection(ranges) {
      if (!bandEdges.length) {
        return true;
      }
      if (!ranges || ranges.length !== 1) {
        return false;
      }
      return (
        Number(ranges[0].start_band) === Number(bandEdges[0].band) &&
        Number(ranges[0].end_band) === Number(bandEdges[bandEdges.length - 1].band)
      );
    }
    function fullRangeRect() {
      if (!bandEdges.length) {
        return null;
      }
      const x0 = xMap(bandEdges[0].x_min);
      const x1 = xMap(bandEdges[bandEdges.length - 1].x_max);
      return { x: x0, width: Math.max(x1 - x0, 1.5) };
    }
    function beginPointerDrag(event) {
      if (event.shiftKey && props.onSlopeGesture) {
        // Shift+click slope gesture (Anchor-Ref. Phase, phacal only). Takes
        // precedence over drag-to-mask / zoom for this panel while Shift is
        // held. Two clicks commit a (delay, offset) seed to the parent.
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        event.stopPropagation();
        if (event.preventDefault) {
          event.preventDefault();
        }
        if (slopeState && slopeState.point1) {
          const point1 = slopeState.point1;
          const point2 = { x: xValue, y: yValue };
          setSlopeState(null);
          props.onSlopeGesture(point1, point2);
        } else {
          setSlopeState({ point1: { x: xValue, y: yValue }, cursor: { x: xValue, y: yValue } });
        }
        return;
      }
      if (slopeState) {
        // A non-Shift click cancels a staged first point.
        setSlopeState(null);
      }
      if (interactionMode === "bandselect") {
        if (!props.onBandWindowSelect || !bandEdges.length) {
          return;
        }
        const xValue = xValueFromClientX(event);
        const band = bandFromXValue(xValue);
        if (!Number.isFinite(band)) {
          return;
        }
        setDragState({
          kind: "band",
          startBand: band,
          currentBand: band,
        });
      } else if (interactionMode === "zoom") {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        setDragState(
          event.shiftKey
            ? {
                kind: "pan",
                startX: xValue,
                startY: yValue,
                currentX: xValue,
                currentY: yValue,
                baseX: [xMin, xMax],
                baseY: [yMin, yMax],
              }
            : {
                kind: "zoom",
                startX: xValue,
                startY: yValue,
                currentX: xValue,
                currentY: yValue,
              }
        );
      } else {
        return;
      }
      if (event.currentTarget && event.currentTarget.setPointerCapture) {
        event.currentTarget.setPointerCapture(event.pointerId);
      }
    }
    function movePointerDrag(event) {
      if (slopeState && slopeState.point1) {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        if (Number.isFinite(xValue) && Number.isFinite(yValue)) {
          setSlopeState(function (current) {
            if (!current || !current.point1) {
              return current;
            }
            return Object.assign({}, current, { cursor: { x: xValue, y: yValue } });
          });
        }
      }
      if (!dragState) {
        return;
      }
      if (dragState.kind === "band") {
        const xValue = xValueFromClientX(event);
        const band = bandFromXValue(xValue);
        if (!Number.isFinite(band)) {
          return;
        }
        setDragState(function (current) {
          if (!current) {
            return current;
          }
          return Object.assign({}, current, { currentBand: band });
        });
      } else {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        setDragState(function (current) {
          if (!current) {
            return current;
          }
          return Object.assign({}, current, { currentX: xValue, currentY: yValue });
        });
      }
    }
    function endPointerDrag(event) {
      if (!dragState) {
        return;
      }
      if (event.currentTarget && event.currentTarget.releasePointerCapture) {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch (_err) {}
      }
      if (dragState.kind === "band") {
        const xValue = xValueFromClientX(event);
        const band = bandFromXValue(xValue);
        const startBand = dragState.startBand;
        const endBand = Number.isFinite(band) ? band : dragState.currentBand;
        setDragState(null);
        if (!Number.isFinite(startBand) || !Number.isFinite(endBand) || !props.onBandWindowSelect) {
          return;
        }
        props.onBandWindowSelect(Math.min(startBand, endBand), Math.max(startBand, endBand), "flag");
        return;
      }
      if (dragState.kind === "zoom") {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        setDragState(null);
        if (!props.onViewportChange || !Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        const nextX = [Math.min(dragState.startX, xValue), Math.max(dragState.startX, xValue)];
        const nextY = [Math.min(dragState.startY, yValue), Math.max(dragState.startY, yValue)];
        if (Math.abs(nextX[1] - nextX[0]) < 1e-6 || Math.abs(nextY[1] - nextY[0]) < 1e-6) {
          return;
        }
        props.onViewportChange({ xLimits: nextX, yLimits: nextY });
        return;
      }
      if (dragState.kind === "pan") {
        const xValue = xValueFromClientX(event);
        const yValue = yValueFromClientY(event);
        setDragState(null);
        if (!props.onViewportChange || !Number.isFinite(xValue) || !Number.isFinite(yValue)) {
          return;
        }
        const dx = dragState.startX - xValue;
        const dy = dragState.startY - yValue;
        props.onViewportChange({
          xLimits: [dragState.baseX[0] + dx, dragState.baseX[1] + dx],
          yLimits: [dragState.baseY[0] + dy, dragState.baseY[1] + dy],
        });
      }
    }
    const hasData = (panel.series || []).some(function (series) {
      return seriesPairs(series).length > 0;
    });
    const hasExplicitSelection = props.showBandWindow && !isFullRangeSelection(activeRanges);
    const excludedWindowRects = hasExplicitSelection ? rangeRects(excludedRangesFromKeptRanges(activeRanges, bandEdges)) : [];
    const dragRect =
      dragState && dragState.kind === "band" ? rangeRect(dragState.startBand, dragState.currentBand) : null;
    const zoomRect =
      dragState && dragState.kind === "zoom"
        ? {
            x: xMap(Math.min(dragState.startX, dragState.currentX)),
            y: yMap(Math.max(dragState.startY, dragState.currentY)),
            width: Math.max(Math.abs(xMap(dragState.currentX) - xMap(dragState.startX)), 1.5),
            height: Math.max(Math.abs(yMap(dragState.currentY) - yMap(dragState.startY)), 1.5),
          }
        : null;
    function startHintTimer(event) {
      if (!hintText) return;
      const rect = containerRef.current && containerRef.current.getBoundingClientRect();
      if (rect) {
        setHintPos({ x: event.clientX - rect.left, y: event.clientY - rect.top });
      }
      if (hintTimerRef.current) {
        clearTimeout(hintTimerRef.current);
      }
      hintTimerRef.current = setTimeout(function () {
        setShowHint(true);
      }, 600);
    }
    function clearHintTimer() {
      if (hintTimerRef.current) {
        clearTimeout(hintTimerRef.current);
        hintTimerRef.current = null;
      }
      setShowHint(false);
    }
    const isSelectedColumn = props.isSelectedColumn === true;
    return html`
      <div
        ref=${containerRef}
        className=${"mini-plot-shell"
          + (interactionMode ? " interactive" : props.onDoubleClick ? " interactive" : "")
          + (panel.disabled ? " disabled" : "")
          + (isSelectedColumn ? " selected-column" : "")}
        style=${{ "--mini-panel-width": String(width), "--mini-panel-height": String(height) }}
        onDoubleClick=${function () {
          if (interactionMode === "zoom" && props.onResetViewport) {
            props.onResetViewport();
            return;
          }
          if (props.onDoubleClick) {
            props.onDoubleClick();
          }
        }}
        title=${null}
        onMouseEnter=${startHintTimer}
        onMouseMove=${function (event) {
          if (!hintText) return;
          const rect = containerRef.current && containerRef.current.getBoundingClientRect();
          if (rect) {
            setHintPos({ x: event.clientX - rect.left, y: event.clientY - rect.top });
          }
          if (!showHint) {
            // While the hover delay is running, restart the timer so the
            // tooltip appears at the cursor's current position rather than
            // where the mouse first entered.
            if (hintTimerRef.current) {
              clearTimeout(hintTimerRef.current);
            }
            hintTimerRef.current = setTimeout(function () {
              setShowHint(true);
            }, 600);
          }
        }}
        onMouseLeave=${clearHintTimer}
      >
        ${showHint && hintText
          ? html`<div className="mini-plot-hint" style=${{ left: (hintPos.x + 14) + "px", top: (hintPos.y + 18) + "px" }}>${hintText}</div>`
          : null}
        <svg viewBox=${"0 0 " + width + " " + height} className="mini-plot" preserveAspectRatio="xMidYMid meet" role="img" aria-label=${panel.title || "panel"}>
          <defs>
            <clipPath id=${clipIdRef.current}>
              <rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} />
            </clipPath>
          </defs>
          ${xTicks.map(function (tick, idx) {
            if (tick < xMin || tick > xMax) {
              return null;
            }
            const x = xMap(tick);
            const lastIdx = xTicks.length - 1;
            const isFirst = idx === 0;
            const isLast = idx === lastIdx;
            const labelX = isFirst ? x + 2 : isLast ? x - 2 : x;
            const anchor = isFirst ? "start" : isLast ? "end" : "middle";
            return html`
              <g key=${"x-" + idx}>
                <line x1=${x} x2=${x} y1=${margin.top} y2=${margin.top + plotHeight} className="grid-line" />
                ${showXAxisLabels
                  ? html`<text x=${labelX} y=${height - 6} textAnchor=${anchor} className="mini-axis-label">${xTickLabels[idx] || formatNumber(tick)}</text>`
                  : null}
              </g>
            `;
          })}
          ${yTicks.map(function (tick, idx) {
            const y = yMap(tick);
            return html`
              <g key=${"y-" + idx}>
                <line x1=${margin.left} x2=${margin.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
              </g>
            `;
          })}

          <rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} className="plot-frame" />
          ${props.showBandWindow
            ? excludedWindowRects.map(function (item, idx) {
                return html`<rect
                  key=${"excluded-range-" + idx}
                  x=${item.x}
                  y=${margin.top}
                  width=${item.width}
                  height=${plotHeight}
                  className="band-window-excluded-bg"
                />`;
              })
            : null}
          ${dragRect && props.showBandWindow
            ? html`<rect
                x=${dragRect.x}
                y=${margin.top}
                width=${dragRect.width}
                height=${plotHeight}
                className="band-window-exclude-rect"
              />`
            : null}
          ${zoomRect
            ? html`<rect
                x=${zoomRect.x}
                y=${zoomRect.y}
                width=${zoomRect.width}
                height=${zoomRect.height}
                className="band-window-drag-rect"
              />`
            : null}
          ${showTitle && panel.title
            ? html`<text
                x=${width / 2}
                y="2"
                textAnchor="middle"
                dominantBaseline="hanging"
                className="mini-panel-title"
              >${panel.title}</text>`
            : null}
          ${showTitle && panel.header_badge
            ? html`<g className="mini-panel-badge-group">
                <title>${panel.header_badge.tooltip}</title>
                <text
                  x=${width - margin.right - 2}
                  y="1"
                  textAnchor="end"
                  dominantBaseline="hanging"
                  className="mini-panel-badge"
                >${panel.header_badge.symbol}</text>
              </g>`
            : null}
          ${panel.annotation
            ? html`<text x=${margin.left + 4} y=${margin.top + 12} className="mini-annotation">${panel.annotation}</text>`
            : null}

          ${!hasData
            ? html`<text x=${width / 2} y=${margin.top + plotHeight / 2} textAnchor="middle" className="mini-empty-label">No data</text>`
            : null}

          <g clipPath=${"url(#" + clipIdRef.current + ")"}>
            ${(panel.series || []).map(function (series, idx) {
              const pairs = seriesPairs(series);
              const path = polylinePath(pairs, xMap, yMap);
              const showLine = series.mode === "line" || series.mode === "linepoints";
              const showPoints = series.mode === "points" || series.mode === "linepoints";
              return html`
                <g key=${(panel.title || "panel") + "-series-" + idx}>
                  ${showLine && path
                    ? html`<path
                        d=${path}
                        fill="none"
                        stroke=${series.color}
                        strokeWidth="1.2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeDasharray=${series.dasharray || null}
                        opacity=${series.opacity === undefined ? 0.95 : series.opacity}
                      />`
                    : null}
                  ${showPoints
                    ? pairs.map(function (pair, pairIdx) {
                        return html`<circle
                          key=${(panel.title || "panel") + "-pt-" + idx + "-" + pairIdx}
                          cx=${xMap(pair[0])}
                          cy=${yMap(pair[1])}
                          r="1.8"
                          fill=${series.color}
                          opacity=${series.opacity === undefined ? 0.95 : series.opacity}
                        />`;
                      })
                    : null}
                </g>
              `;
            })}
            ${panel.disabled
              ? html`<rect x=${margin.left} y=${margin.top} width=${plotWidth} height=${plotHeight} fill="rgba(120, 126, 132, 0.24)" />`
              : null}
            ${slopeState && slopeState.point1
              ? html`
                  <line
                    x1=${xMap(slopeState.point1.x)}
                    y1=${yMap(slopeState.point1.y)}
                    x2=${xMap(slopeState.cursor.x)}
                    y2=${yMap(slopeState.cursor.y)}
                    stroke="#ff7f00"
                    strokeWidth="1.4"
                    strokeDasharray="4 3"
                  />
                  <circle
                    cx=${xMap(slopeState.point1.x)}
                    cy=${yMap(slopeState.point1.y)}
                    r="3.2"
                    fill="none"
                    stroke="#ff7f00"
                    strokeWidth="1.5"
                  />
                `
              : null}
          </g>
          ${interactionMode || props.onSlopeGesture
            ? html`<rect
                x=${margin.left}
                y=${margin.top}
                width=${plotWidth}
                height=${plotHeight}
                className="mini-band-hitbox"
                onPointerDown=${beginPointerDrag}
                onPointerMove=${movePointerDrag}
                onPointerUp=${endPointerDrag}
                onPointerLeave=${function () {}}
              />`
            : null}
        </svg>
      </div>
    `;
  }

  function PanelGridPlot(props) {
    const [rowViewports, setRowViewports] = useState({});
    const data = props.data;
    if (!data) {
      return html`<div className="plot-placeholder">Loading plot...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }
    const rowYLimits = (data.panels || []).map(function (row, rowIdx) {
      const viewport = rowViewports[rowIdx];
      const provided = viewport && viewport.yLimits ? viewport.yLimits : (data.y_limits && data.y_limits[rowIdx]) || [-1, 1];
      if (viewport && viewport.yLimits) {
        return provided;
      }
      if (!data.auto_scale_rows) {
        return provided;
      }
      let dMin = Infinity;
      let dMax = -Infinity;
      row.forEach(function (panel) {
        (panel.series || []).forEach(function (series) {
          seriesPairs(series).forEach(function (pair) {
            if (Number.isFinite(pair[1])) {
              dMin = Math.min(dMin, pair[1]);
              dMax = Math.max(dMax, pair[1]);
            }
          });
        });
      });
      if (!Number.isFinite(dMin)) {
        return provided;
      }
      const pad = Math.max((dMax - dMin) * 0.2, 0.05);
      return [dMin - pad, dMax + pad];
    });
    function rowXLimits(rowIdx) {
      const viewport = rowViewports[rowIdx];
      return viewport && viewport.xLimits ? viewport.xLimits : data.x_limits;
    }
    return html`
      <div
        className="panel-grid-shell"
        style=${{
          "--panel-grid-panel-width": (props.panelWidth || PANEL_GRID_PANEL_WIDTH) + "px",
          "--panel-grid-panel-height": String(props.panelHeight || 92),
        }}
      >
        ${!props.hideLegend && data.legend && data.legend.length
          ? html`
              <div className="series-legend">
                ${data.legend.map(function (entry) {
                  return legendItem(entry.label, entry.color, entry.mode);
                })}
              </div>
            `
          : null}
        <div className="panel-grid-section-heading-row">
          <span className="panel-grid-section-heading">${mergedSectionHeading(data.row_labels)}</span>
          <span className="panel-grid-section-xlabel">${abbreviateWallLabel(data.x_label)}</span>
        </div>
        <div className="panel-grid-shared-body">
          <div className="panel-grid-left-rail">
            ${data.column_controls && data.column_controls.length
              ? html`<div className="panel-grid-left-rail-spacer"></div>`
              : null}
            ${(data.panels || []).map(function (_row, rowIdx) {
              const yLimits = rowYLimits[rowIdx];
              const rowYTicks = linearTicks(yLimits[0], yLimits[1], 3);
              const panelHeight = props.panelHeight || 128;
              const isBottomRow = rowIdx === (data.panels || []).length - 1;
              const axisTop = 8;
              const axisBottom = isBottomRow ? 24 : 8;
              const axisHeight = Math.max(panelHeight - axisTop - axisBottom, 1);
              const yMin = Number(yLimits[0]);
              const yMax = Number(yLimits[1]);
              const ySpan = yMax - yMin;
              return html`
                <div key=${"yaxis-" + rowIdx} className="panel-grid-yaxis-block" style=${{ height: panelHeight + "px" }}>
                  <div className="panel-grid-yaxis" aria-hidden="true">
                    ${rowYTicks.map(function (tick, tickIdx) {
                      const fraction = Math.abs(ySpan) < 1.0e-9 ? 0.5 : clamp((Number(tick) - yMin) / ySpan, 0, 1);
                      const top = axisTop + (1.0 - fraction) * axisHeight;
                      return html`<span
                        key=${"y-tick-" + rowIdx + "-" + tickIdx}
                        className="panel-grid-yaxis-label"
                        style=${{ top: top + "px" }}
                      >${formatNumber(tick)}</span>`;
                    })}
                  </div>
                </div>
              `;
            })}
          </div>
          <div className="panel-grid-scroll">
            ${data.column_controls && data.column_controls.length
              ? html`
                  <div className="panel-grid-column-controls">
                    <div className="panel-grid-panels">
                      ${data.column_controls.map(function (control, idx) {
                        const firstRow = (data.panels && data.panels[0]) || [];
                        const badge = (firstRow[idx] && firstRow[idx].header_badge) || null;
                        return html`
                          <div
                            key=${"col-control-" + control.antenna}
                            className=${"panel-grid-column-control"
                              + (control.flagged ? " flagged" : "")
                              + (control.auto_flagged ? " auto-flagged" : "")}
                          >
                            <div className="panel-grid-column-control-main">
                              <input
                                type="checkbox"
                                checked=${!!control.checked}
                                disabled=${!!props.busy || !props.onColumnToggle}
                                onChange=${props.onColumnToggle
                                  ? function (event) {
                                      props.onColumnToggle(Number(control.antenna), !event.target.checked);
                                    }
                                  : null}
                              />
                              <span className="panel-grid-column-control-text">${control.label}</span>
                              ${badge
                                ? html`<span
                                    className="panel-grid-column-control-badge"
                                    title=${badge.tooltip}
                                  >${badge.symbol}</span>`
                                : null}
                            </div>
                            ${props.columnActionRenderer ? props.columnActionRenderer(control) : null}
                          </div>
                        `;
                      })}
                    </div>
                  </div>
                `
              : null}
            <div className="panel-grid-rows">
              ${(data.panels || []).map(function (row, rowIdx) {
                const yLimits = rowYLimits[rowIdx];
                return html`
                  <section key=${"row-" + rowIdx} className="panel-grid-row">
                    <div className="panel-grid-panels">
                      ${row.map(function (panel, panelIdx) {
                        const displayPanel = props.panelOverride ? props.panelOverride(rowIdx, panelIdx, panel) : panel;
                        const isSelectedColumn =
                          props.selectedAnt !== null
                          && props.selectedAnt !== undefined
                          && Number(props.selectedAnt) === Number(panelIdx);
                        const hasColumnHeader = !!(data.column_controls && data.column_controls.length);
                        return html`<${MiniPanelPlot}
                          key=${"panel-" + rowIdx + "-" + panelIdx}
                          panel=${displayPanel}
                          isSelectedColumn=${isSelectedColumn}
                          showTitle=${rowIdx === 0 && !hasColumnHeader}
                          xLimits=${rowXLimits(rowIdx)}
                          xTicks=${data.x_ticks}
                          yLimits=${data.y_limits[rowIdx]}
                          autoYLimits=${yLimits}
                          panelHeight=${props.panelHeight}
                          bandEdges=${data.band_edges}
                          showBandWindow=${!!props.onBandWindowSelect}
                          showXAxisLabels=${rowIdx === (data.panels || []).length - 1}
                          interactionMode=${props.interactionMode === "zoom" ? "zoom" : props.onBandWindowSelect ? "bandselect" : null}
                          bandSelectApplyLabel=${props.bandSelectApplyLabel}
                          onBandWindowSelect=${props.onBandWindowSelect
                            ? function (startBand, endBand, mode) {
                                props.onBandWindowSelect(rowIdx, panelIdx, startBand, endBand, mode, displayPanel);
                              }
                            : null}
                          onViewportChange=${props.interactionMode === "zoom"
                            ? function (nextViewport) {
                                setRowViewports(function (current) {
                                  return Object.assign({}, current, { [rowIdx]: nextViewport });
                                });
                              }
                            : null}
                          onResetViewport=${props.interactionMode === "zoom"
                            ? function () {
                                setRowViewports(function (current) {
                                  const next = Object.assign({}, current);
                                  delete next[rowIdx];
                                  return next;
                                });
                              }
                            : null}
                          onDoubleClick=${props.onPanelDoubleClick
                            ? function () {
                                props.onPanelDoubleClick(rowIdx, panelIdx, displayPanel);
                              }
                            : null}
                          onSlopeGesture=${props.onSlopeGesture
                            ? function (point1, point2) {
                                props.onSlopeGesture(rowIdx, panelIdx, point1, point2, displayPanel);
                              }
                            : null}
                        />`;
                      })}
                    </div>
                  </section>
                `;
              })}
            </div>
          </div>
        </div>
      </div>
    `;
  }

  function ResidualInspectorModal(props) {
    if (!props.data) {
      return null;
    }
    return html`
      <div
        className="residual-modal-backdrop"
        onClick=${props.onClose}
        role="dialog"
        aria-modal="true"
        aria-label="Inband fit residuals"
      >
        <div className="residual-modal-card panel" onClick=${function (event) { event.stopPropagation(); }}>
          <div className="residual-modal-header">
            <div>
              <h2>${props.title}</h2>
              <div className="plot-card-caption-row">Double-clicked from Inband Fit. Residual = wrapped(raw phase - fitted phase).</div>
            </div>
            <button type="button" className="residual-modal-close" onClick=${props.onClose}>Close</button>
          </div>
          <div className="residual-modal-body">
            <${PanelGridPlot}
              data=${props.data}
              hideLegend=${true}
              panelWidth=${760}
              panelHeight=${170}
            />
          </div>
        </div>
      </div>
    `;
  }

  function PlotCard(props) {
    return html`
      <section className="plot-card">
        <div className="plot-card-header">
          <div className="plot-card-title-row">
            <h2>${abbreviateWallLabel(props.title)}</h2>
            ${(props.legend || []).map(function (entry) {
              return legendItem(entry.label, entry.color, entry.mode);
            })}
            ${props.inlineControls ? html`<div className="plot-card-inline-controls">${props.inlineControls}</div>` : null}
            ${props.meta ? html`<span className="plot-card-meta">${props.meta}</span>` : null}
          </div>
          ${props.controls ? html`<div className="plot-card-controls-row">${props.controls}</div>` : null}
          ${props.caption ? html`<div className="plot-card-caption-row">${props.caption}</div>` : null}
        </div>
        <div className="plot-card-body">${props.children}</div>
      </section>
    `;
  }

  function AnchorCompareView(props) {
    const data = props.data;
    if (!data) {
      return null;
    }
    if (data.message) {
      return html`
        <section className="plot-card anchor-compare-card">
          <div className="plot-card-body">
            <div className="plot-placeholder">${data.message}</div>
          </div>
        </section>
      `;
    }
    function sectionPanelHeight(sectionId) {
      if (sectionId === "sum_pha") {
        return 116;
      }
      return 138;
    }
    function summaryBlock(summary) {
      const scanId = Number(summary.scan_id);
      const isCanonical = props.canonicalId === scanId;
      const isSecondary = props.secondaryId === scanId;
      return html`
        <div className=${"anchor-compare-summary" + (isCanonical ? " canonical" : "") + (isSecondary ? " secondary" : "")}>
          <div className="anchor-compare-summary-title">
            <strong>${(summary.scan_time || "") + " · " + String(summary.feed_kind || "").toUpperCase()}</strong>
            <span>${summary.source || ""}</span>
          </div>
          <div className="tiny">
            ${"Kept " + summary.kept_antennas + " / " + summary.total_antennas + " · Flagged " + summary.flagged_antennas}
          </div>
          <div className="anchor-compare-summary-actions">
            <button
              type="button"
              className=${isCanonical ? "btn-dark-fill" : "btn-outline-blue"}
              disabled=${props.busy || isCanonical}
              onClick=${function () {
                props.onSetCanonical(scanId);
              }}
            >
              ${isCanonical ? "Canonical" : "Set Canonical"}
            </button>
            <button
              type="button"
              className=${isSecondary ? "btn-dark-fill" : "btn-outline-blue"}
              disabled=${props.busy || isCanonical}
              onClick=${function () {
                if (isSecondary) {
                  props.onClearSecondary();
                } else {
                  props.onSetSecondary(scanId);
                }
              }}
            >
              ${isSecondary ? "Clear Secondary" : "Set Secondary"}
            </button>
          </div>
        </div>
      `;
    }
    function donorPatchBlock() {
      const donor = data.donor_patch || null;
      if (!donor) {
        return null;
      }
      return html`
        <div className="anchor-donor-block">
          <div className="anchor-donor-header">
            <strong>Donor Patch</strong>
            <span className="tiny">
              ${donor.enabled
                ? "Secondary stays inert until you explicitly apply donor antennas."
                : "Set Canonical and Secondary to enable donor patching."}
            </span>
          </div>
          <div className="anchor-donor-candidates">
            ${(donor.candidates || []).map(function (candidate) {
              const disabled = props.busy || !donor.enabled || !candidate.can_patch;
              const className =
                "anchor-donor-chip"
                + (candidate.recommended ? " recommended" : "")
                + (candidate.staged ? " staged" : "")
                + (candidate.applied ? " applied" : "")
                + (disabled ? " disabled" : "");
              return html`
                <button
                  type="button"
                  key=${candidate.antenna}
                  className=${className}
                  disabled=${disabled}
                  title=${candidate.reason || ""}
                  onClick=${function () {
                    props.onToggleDonorPatch(candidate.antenna, !candidate.staged);
                  }}
                >
                  <span>${candidate.label}</span>
                  <span className="tiny">${candidate.reason}</span>
                </button>
              `;
            })}
          </div>
          <div className="anchor-donor-actions">
            <button
              type="button"
              className="btn-outline-blue"
              disabled=${props.busy || !donor.enabled}
              onClick=${props.onApplyDonorPatch}
            >
              Apply Donor Patch
            </button>
            ${(donor.applied_antennas || []).length
              ? html`<span className="tiny">${"Applied: " + donor.applied_antennas.map(function (ant) {
                  return "Ant " + String(Number(ant) + 1);
                }).join(", ")}</span>`
              : null}
          </div>
        </div>
      `;
    }
    return html`
      <section className="plot-card anchor-compare-card">
        <div className="plot-card-header">
          <div className="plot-card-title-row">
            <h2>${abbreviateWallLabel(data.title || "Compare 2 Anchors")}</h2>
            <span className="plot-card-meta">${String(data.feed_kind || "").toUpperCase() + " same-feed compare"}</span>
            <div className="plot-card-inline-controls">
              <button
                type="button"
                className="btn-outline-blue"
                disabled=${props.busy}
                onClick=${props.onClearCompare}
              >
                Close Compare
              </button>
            </div>
          </div>
        </div>
        <div className="plot-card-body">
          <div className="anchor-compare-summary-row">
            ${summaryBlock(data.left || {})}
            ${summaryBlock(data.right || {})}
          </div>
          ${donorPatchBlock()}
          <div className="anchor-compare-sections">
            ${(data.sections || []).map(function (section) {
              return html`
                <div key=${section.id} className="anchor-compare-section-row">
                  <${PlotCard}
                    title=${(section.left && section.left.title) || section.title}
                    legend=${section.left && section.left.legend}
                  >
                    <${PanelGridPlot}
                      data=${section.left}
                      hideLegend=${true}
                      busy=${props.busy}
                      panelHeight=${sectionPanelHeight(section.id)}
                    />
                  </${PlotCard}>
                  <${PlotCard}
                    title=${(section.right && section.right.title) || section.title}
                    legend=${section.right && section.right.legend}
                  >
                    <${PanelGridPlot}
                      data=${section.right}
                      hideLegend=${true}
                      busy=${props.busy}
                      panelHeight=${sectionPanelHeight(section.id)}
                    />
                  </${PlotCard}>
                </div>
              `;
            })}
          </div>
        </div>
      </section>
    `;
  }

  function InbandWindowControls(props) {
    return html`
      <div className="inband-window-controls">
        <div className="inband-scope-group">
          <span className="tiny">Antennas</span>
          <div className="scope-pills" role="group" aria-label="Inband antenna scope">
            ${INBAND_SCOPE_OPTIONS.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${"ant-scope-" + item.id}
                  className=${"scope-pill" + (props.antennaScope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onAntennaScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
        </div>
        ${!props.hidePol ? html`<div className="inband-scope-group">
          <span className="tiny">Polarizations</span>
          <div className="scope-pills" role="group" aria-label="Inband polarization scope">
            ${INBAND_SCOPE_OPTIONS.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${"pol-scope-" + item.id}
                  className=${"scope-pill" + (props.polScope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onPolScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
        </div>` : null}
        <button
          type="button"
          className="btn-outline-blue"
          disabled=${props.busy || !props.hasPending}
          title="Commit the staged in-band mask using the antenna/pol scope above"
          onClick=${function () {
            props.onApply();
          }}
        >
          ${props.applyLabel || "Apply Mask"}
        </button>
      </div>
    `;
  }

  function TimeHistoryPlot(props) {
    const data = props.data;
    const wrapperRef = useRef(null);
    const svgRef = useRef(null);
    const [hoverOffset, setHoverOffset] = useState(null);
    const [hoverJd, setHoverJd] = useState(null);
    const [pendingAnchorJd, setPendingAnchorJd] = useState(null);
    const [dragState, setDragState] = useState(null);
    const [hoveredGroupId, setHoveredGroupId] = useState(null);

    useEffect(
      function () {
        setHoverOffset(null);
        setHoverJd(null);
        setPendingAnchorJd(null);
        setDragState(null);
        setHoveredGroupId(null);
      },
      [data && data.title]
    );

    if (!data) {
      return html`<div className="plot-placeholder">Loading legacy-equivalent time history...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }
    const width = 940;
    const height = 560;
    const outer = { left: 64, right: 28, top: 58, bottom: 46 };
    const gap = 52;
    const panelWidth = (width - outer.left - outer.right - gap) / 2;
    const panelHeight = height - outer.top - outer.bottom;
    const ampX0 = outer.left;
    const phaseX0 = outer.left + panelWidth + gap;
    const y0 = outer.top;
    const xOffsets = data.offset_min || [];
    const xMin = xOffsets.length ? xOffsets[0] : 0.0;
    const xMax = xOffsets.length ? xOffsets[xOffsets.length - 1] : xMin + 1.0;
    const startJd = Number.isFinite(data.start_jd) ? data.start_jd : 0.0;
    const ampMin = Math.max((data.amp_ylim && data.amp_ylim[0]) || 1e-3, 1e-6);
    const ampMax = Math.max((data.amp_ylim && data.amp_ylim[1]) || 1.0, ampMin * 10.0);
    const phaseMin = (data.phase_ylim && data.phase_ylim[0]) || -4.0;
    const phaseMax = (data.phase_ylim && data.phase_ylim[1]) || 4.0;
    const intervalGroups = data.interval_groups || [];
    const pendingIntervals = props.pendingIntervals || [];
    const allIntervalGroups = intervalGroups.concat(pendingIntervals);

    function focusShell() {
      if (wrapperRef.current && typeof wrapperRef.current.focus === "function") {
        wrapperRef.current.focus({ preventScroll: true });
      }
    }

    function xMap(value, panelStart) {
      const denom = xMax - xMin || 1.0;
      return panelStart + ((value - xMin) / denom) * panelWidth;
    }
    function offsetToJd(offset) {
      return startJd + offset / 1440.0;
    }
    function svgXFromEvent(event) {
      if (!svgRef.current) {
        return null;
      }
      const rect = svgRef.current.getBoundingClientRect();
      if (!rect.width) {
        return null;
      }
      return ((event.clientX - rect.left) / rect.width) * width;
    }
    function offsetFromSvgX(svgX, panelStart) {
      const clampedX = clamp(svgX, panelStart, panelStart + panelWidth);
      const denom = panelWidth || 1.0;
      return xMin + ((clampedX - panelStart) / denom) * (xMax - xMin || 1.0);
    }
    function intervalGroupAtOffset(offset) {
      const active = allIntervalGroups.filter(function (group) {
        const left = Math.min(group.start_offset_min, group.end_offset_min);
        const right = Math.max(group.start_offset_min, group.end_offset_min);
        return offset >= left && offset <= right;
      });
      if (!active.length) {
        return null;
      }
      active.sort(function (a, b) {
        const spanA = Math.abs(a.end_offset_min - a.start_offset_min);
        const spanB = Math.abs(b.end_offset_min - b.start_offset_min);
        return spanA - spanB;
      });
      return active[0].group_id;
    }
    function updateHover(event, panelStart) {
      const svgX = svgXFromEvent(event);
      if (!Number.isFinite(svgX)) {
        return null;
      }
      const offset = offsetFromSvgX(svgX, panelStart);
      setHoverOffset(offset);
      setHoverJd(offsetToJd(offset));
      setHoveredGroupId(intervalGroupAtOffset(offset));
      return offset;
    }
    function ampMap(value) {
      const lv = Math.log10(Math.max(value, ampMin));
      const lmin = Math.log10(ampMin);
      const lmax = Math.log10(ampMax);
      const denom = lmax - lmin || 1.0;
      return y0 + ((lmax - lv) / denom) * panelHeight;
    }
    function phaseMap(value) {
      const denom = phaseMax - phaseMin || 1.0;
      return y0 + ((phaseMax - value) / denom) * panelHeight;
    }
    function ampTicks() {
      const out = [];
      const start = Math.ceil(Math.log10(ampMin));
      const end = Math.floor(Math.log10(ampMax));
      for (let power = start; power <= end; power += 1) {
        out.push(Math.pow(10, power));
      }
      if (!out.length) {
        out.push(ampMin, ampMax);
      }
      return out;
    }
    function phaseTicks() {
      const out = [];
      for (let value = phaseMin; value <= phaseMax + 1e-6; value += 2) {
        out.push(value);
      }
      return out;
    }
    function pointElements(series, key, xStart, yMapFn) {
      return (series[key] || []).map(function (value, idx) {
        const x = xOffsets[idx];
        if (!Number.isFinite(x) || !Number.isFinite(value)) {
          return null;
        }
        if (key === "amp" && value <= 0) {
          return null;
        }
        return html`<circle
          key=${series.label + "-" + key + "-" + idx}
          cx=${xMap(x, xStart)}
          cy=${yMapFn(value)}
          r="3.2"
          fill=${series.color}
          opacity="0.95"
        />`;
      });
    }
    function ampTickLines(panelStart) {
      return ampTicks().map(function (value) {
        const y = ampMap(value);
        const power = Math.round(Math.log10(value));
        return html`
          <g key=${panelStart + "-amp-tick-" + value}>
            <line x1=${panelStart} x2=${panelStart + panelWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${panelStart - 6} x2=${panelStart} y1=${y} y2=${y} className="axis-line" />
            <text x=${panelStart - 10} y=${y + 4} textAnchor="end" className="axis-label">
              10<tspan dy="-4" fontSize="75%">${String(power)}</tspan>
            </text>
          </g>
        `;
      });
    }
    function tickLines(panelStart, yTicks, yMapFn, formatter) {
      return yTicks.map(function (value) {
        const y = yMapFn(value);
        return html`
          <g key=${panelStart + "-tick-" + value}>
            <line x1=${panelStart} x2=${panelStart + panelWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${panelStart - 6} x2=${panelStart} y1=${y} y2=${y} className="axis-line" />
            <text x=${panelStart - 10} y=${y + 4} textAnchor="end" className="axis-label">${formatter(value)}</text>
          </g>
        `;
      });
    }
    function xTicks(panelStart) {
      const ticks = data.tick_offsets || [];
      const labels = data.tick_labels || [];
      return ticks.map(function (value, idx) {
        const x = xMap(value, panelStart);
        return html`
          <g key=${panelStart + "-xtick-" + idx}>
            <line x1=${x} x2=${x} y1=${y0} y2=${y0 + panelHeight} className="grid-line" />
            <line x1=${x} x2=${x} y1=${y0 + panelHeight} y2=${y0 + panelHeight + 6} className="axis-line" />
            <text x=${x} y=${y0 + panelHeight + 22} textAnchor="middle" className="axis-label">${labels[idx]}</text>
          </g>
        `;
      });
    }
    function intervalBands(panelStart) {
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset, panelStart);
        const right = xMap(rightOffset, panelStart);
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g key=${panelStart + "-interval-" + group.group_id} className=${hovered ? "interval-group hovered" : "interval-group"}>
            <rect x=${left} y=${y0} width=${Math.max(right - left, 1.5)} height=${panelHeight} className="interval-band" />
            <line x1=${left} x2=${left} y1=${y0} y2=${y0 + panelHeight} className="interval-boundary interval-start" />
            <line x1=${right} x2=${right} y1=${y0} y2=${y0 + panelHeight} className="interval-boundary interval-end" />
          </g>
        `;
      });
    }
    function deleteIntervalGroup(groupId) {
      if (String(groupId).indexOf("staged:") === 0) {
        props.onDeletePendingInterval(groupId);
      } else {
        props.onDeleteInterval(groupId);
      }
    }
    function intervalDeleteButtons(panelStart) {
      const buttonSize = 15;
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset, panelStart);
        const right = xMap(rightOffset, panelStart);
        const x = clamp(right - buttonSize - 2, left + 1, panelStart + panelWidth - buttonSize - 1);
        const y = y0 + 2;
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g
            key=${panelStart + "-interval-delete-" + group.group_id}
            className=${hovered ? "interval-delete hovered" : "interval-delete"}
            onMouseEnter=${function () {
              setHoveredGroupId(group.group_id);
            }}
            onMouseLeave=${function () {
              setHoveredGroupId(function (current) {
                return current === group.group_id ? null : current;
              });
            }}
            onPointerDown=${function (event) {
              event.stopPropagation();
            }}
            onClick=${function (event) {
              event.stopPropagation();
              deleteIntervalGroup(group.group_id);
            }}
          >
            <rect x=${x} y=${y} width=${buttonSize} height=${buttonSize} rx="3" ry="3" className="interval-delete-bg" />
            <text x=${x + buttonSize / 2} y=${y + buttonSize / 2 + 0.5} textAnchor="middle" dominantBaseline="middle" className="interval-delete-label">
              ×
            </text>
          </g>
        `;
      });
    }
    function crosshair(panelStart) {
      if (!Number.isFinite(hoverOffset)) {
        return null;
      }
      const x = xMap(hoverOffset, panelStart);
      return html`<line x1=${x} x2=${x} y1=${y0} y2=${y0 + panelHeight} className="crosshair-line" />`;
    }
    function pendingAnchorLine(panelStart) {
      if (!Number.isFinite(pendingAnchorJd)) {
        return null;
      }
      const offset = (pendingAnchorJd - startJd) * 1440.0;
      const x = xMap(offset, panelStart);
      return html`<line x1=${x} x2=${x} y1=${y0} y2=${y0 + panelHeight} className="anchor-line" />`;
    }
    function dragBand(panelStart) {
      if (!dragState) {
        return null;
      }
      const left = xMap(Math.min(dragState.startOffset, dragState.currentOffset), panelStart);
      const right = xMap(Math.max(dragState.startOffset, dragState.currentOffset), panelStart);
      return html`<rect x=${left} y=${y0} width=${Math.max(right - left, 1.5)} height=${panelHeight} className="drag-band" />`;
    }
    function pointerDown(event, panelStart) {
      if (props.busy) {
        return;
      }
      focusShell();
      const offset = updateHover(event, panelStart);
      if (!Number.isFinite(offset)) {
        return;
      }
      setPendingAnchorJd(null);
      setDragState({
        startOffset: offset,
        currentOffset: offset,
        startClientX: event.clientX,
        currentClientX: event.clientX,
      });
      if (event.currentTarget && event.currentTarget.setPointerCapture) {
        event.currentTarget.setPointerCapture(event.pointerId);
      }
      if (props.onStatus) {
        props.onStatus("Drag to stage flagged range. Click Apply Mask to apply.");
      }
    }
    function pointerMove(event, panelStart) {
      const offset = updateHover(event, panelStart);
      if (!Number.isFinite(offset)) {
        return;
      }
      setDragState(function (current) {
        if (!current) {
          return current;
        }
        return Object.assign({}, current, {
          currentOffset: offset,
          currentClientX: event.clientX,
        });
      });
    }
    function pointerUp(event, panelStart) {
      const offsetRaw = updateHover(event, panelStart);
      const offset = Number.isFinite(offsetRaw) ? offsetRaw : dragState ? dragState.currentOffset : null;
      if (event.currentTarget && event.currentTarget.releasePointerCapture) {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch (_err) {}
      }
      if (!dragState || !Number.isFinite(offset)) {
        setDragState(null);
        return;
      }
      const start = offsetToJd(dragState.startOffset);
      const end = offsetToJd(offset);
      const dragWidthPx = Math.abs((Number.isFinite(event.clientX) ? event.clientX : dragState.currentClientX) - dragState.startClientX);
      setDragState(null);
      if (dragWidthPx < TIME_FLAG_MIN_DRAG_PX || Math.abs(end - start) <= 1.0e-9) {
        if (props.onStatus) {
          props.onStatus("Ignored click without drag.");
        }
        return;
      }
      props.onStageInterval(start, end);
    }
    function pointerLeave() {
      if (!dragState) {
        setHoverOffset(null);
        setHoverJd(null);
        setHoveredGroupId(null);
      }
    }
    function deleteHoveredInterval() {
      if (!hoveredGroupId) {
        if (props.onStatus) {
          props.onStatus("X ignored. Hover an interval first.");
        }
        return;
      }
      deleteIntervalGroup(hoveredGroupId);
    }
    function handleKeyDown(event) {
      const target = event.target;
      if (target && target.tagName && /^(INPUT|TEXTAREA|SELECT|BUTTON)$/.test(target.tagName)) {
        return;
      }
      const key = String(event.key || "").toUpperCase();
      if (key === "A") {
        event.preventDefault();
        if (!Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("A ignored. Hover inside Time History first.");
          }
          return;
        }
        setPendingAnchorJd(hoverJd);
        if (props.onStatus) {
          props.onStatus("A anchor set at " + formatUtcTime(hoverJd) + " UT.");
        }
      } else if (key === "B") {
        event.preventDefault();
        if (!Number.isFinite(pendingAnchorJd) || !Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("B ignored. Set A first, then hover a second time.");
          }
          return;
        }
        props.onStageInterval(pendingAnchorJd, hoverJd);
        setPendingAnchorJd(null);
      } else if (key === "X") {
        event.preventDefault();
        deleteHoveredInterval();
      } else if (key === "ESCAPE") {
        event.preventDefault();
        setPendingAnchorJd(null);
        if (props.onStatus) {
          props.onStatus("Cleared pending A anchor.");
        }
      }
    }
    const hoverReadout = Number.isFinite(hoverJd)
      ? "UT " + formatUtcTime(hoverJd) + " | JD " + formatJd(hoverJd)
      : "Hover inside the plot";
    return html`
      <div className="time-history-shell" ref=${wrapperRef} tabIndex="0" onKeyDown=${handleKeyDown}>
        <div className="time-history-toolbar">
          <div className="scope-pills" role="group" aria-label="Time-flag scope">
            ${TIME_FLAG_SCOPES.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${item.id}
                  className=${"scope-pill" + (props.scope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
          <div className="time-history-readout">
            <span>${hoverReadout}</span>
            ${Number.isFinite(pendingAnchorJd)
              ? html`<span>${"A = " + formatUtcTime(pendingAnchorJd) + " UT"}</span>`
              : null}
          </div>
        </div>
        <div className="series-legend">
          ${(data.series || []).map(function (series) {
            return legendItem(series.label, series.color, "points");
          })}
        </div>
        <svg
          ref=${svgRef}
          viewBox=${"0 0 " + width + " " + height}
          className="svg-plot"
          role="img"
          aria-label=${"Time history " + data.title}
          onMouseEnter=${focusShell}
        >
          ${ampTickLines(ampX0)}
          ${tickLines(phaseX0, phaseTicks(), phaseMap, function (value) {
            return String(value);
          })}
          ${xTicks(ampX0)}
          ${xTicks(phaseX0)}
          ${intervalBands(ampX0)}
          ${intervalBands(phaseX0)}
          ${dragBand(ampX0)}
          ${dragBand(phaseX0)}
          ${crosshair(ampX0)}
          ${crosshair(phaseX0)}
          ${pendingAnchorLine(ampX0)}
          ${pendingAnchorLine(phaseX0)}

          <rect x=${ampX0} y=${y0} width=${panelWidth} height=${panelHeight} className="plot-frame" />
          <rect x=${phaseX0} y=${y0} width=${panelWidth} height=${panelHeight} className="plot-frame" />

          <text x=${ampX0 + panelWidth / 2} y=${y0 - 12} textAnchor="middle" className="panel-title">Amplitude</text>
          <text x=${phaseX0 + panelWidth / 2} y=${y0 - 12} textAnchor="middle" className="panel-title">Phase</text>
          <text x=${ampX0 + panelWidth / 2} y=${height - 8} textAnchor="middle" className="axis-label">Time [UT]</text>
          <text x=${phaseX0 + panelWidth / 2} y=${height - 8} textAnchor="middle" className="axis-label">Time [UT]</text>
          <text x="18" y=${y0 + panelHeight / 2} transform=${"rotate(-90 18 " + (y0 + panelHeight / 2) + ")"} className="axis-label">Amplitude [arb units]</text>
          <text
            x=${phaseX0 - 40}
            y=${y0 + panelHeight / 2}
            transform=${"rotate(-90 " + (phaseX0 - 40) + " " + (y0 + panelHeight / 2) + ")"}
            className="axis-label"
          >
            Phase [rad]
          </text>

          ${(data.series || []).map(function (series) {
            return pointElements(series, "amp", ampX0, ampMap);
          })}
          ${(data.series || []).map(function (series) {
            return pointElements(series, "phase", phaseX0, phaseMap);
          })}
          <rect
            x=${ampX0}
            y=${y0}
            width=${panelWidth}
            height=${panelHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, ampX0);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, ampX0);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, ampX0);
            }}
            onPointerLeave=${pointerLeave}
          />
          <rect
            x=${phaseX0}
            y=${y0}
            width=${panelWidth}
            height=${panelHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, phaseX0);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, phaseX0);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, phaseX0);
            }}
            onPointerLeave=${pointerLeave}
          />
          ${intervalDeleteButtons(ampX0)}
        </svg>
      </div>
    `;
  }

  function CompactTimeHistoryPlot(props) {
    const data = props.data;
    const wrapperRef = useRef(null);
    const ampSvgRef = useRef(null);
    const phaseSvgRef = useRef(null);
    const shellWidth = useMeasuredWidth(wrapperRef, 300);
    const [hoverOffset, setHoverOffset] = useState(null);
    const [hoverJd, setHoverJd] = useState(null);
    const [pendingAnchorJd, setPendingAnchorJd] = useState(null);
    const [dragState, setDragState] = useState(null);
    const [hoveredGroupId, setHoveredGroupId] = useState(null);

    useEffect(
      function () {
        setHoverOffset(null);
        setHoverJd(null);
        setPendingAnchorJd(null);
        setDragState(null);
        setHoveredGroupId(null);
      },
      [data && data.title]
    );

    if (!data) {
      return html`<div className="plot-placeholder">Loading legacy-equivalent time history...</div>`;
    }
    if (data.message) {
      return html`<div className="plot-placeholder">${data.message}</div>`;
    }

    const width = shellWidth > 0 ? shellWidth : 300;
    const ampHeight = Math.round(width * 0.42);
    const phaseHeight = Math.round(width * 0.50);
    const ampOuter = { left: 48, right: 8, top: 8, bottom: 10 };
    const phaseOuter = { left: 48, right: 8, top: 6, bottom: 30 };
    const plotWidth = width - ampOuter.left - ampOuter.right;
    const ampPlotHeight = ampHeight - ampOuter.top - ampOuter.bottom;
    const phasePlotHeight = phaseHeight - phaseOuter.top - phaseOuter.bottom;
    const xOffsets = data.offset_min || [];
    const xMin = xOffsets.length ? xOffsets[0] : 0.0;
    const xMax = xOffsets.length ? xOffsets[xOffsets.length - 1] : xMin + 1.0;
    const startJd = Number.isFinite(data.start_jd) ? data.start_jd : 0.0;
    const ampMin = Math.max((data.amp_ylim && data.amp_ylim[0]) || 1e-3, 1e-6);
    const ampMax = Math.max((data.amp_ylim && data.amp_ylim[1]) || 1.0, ampMin * 10.0);
    const phaseMin = (data.phase_ylim && data.phase_ylim[0]) || -4.0;
    const phaseMax = (data.phase_ylim && data.phase_ylim[1]) || 4.0;
    const intervalGroups = data.interval_groups || [];
    const pendingIntervals = props.pendingIntervals || [];
    const allIntervalGroups = intervalGroups.concat(pendingIntervals);

    function focusShell() {
      if (wrapperRef.current && typeof wrapperRef.current.focus === "function") {
        wrapperRef.current.focus({ preventScroll: true });
      }
    }

    function xMap(value) {
      const denom = xMax - xMin || 1.0;
      return ampOuter.left + ((value - xMin) / denom) * plotWidth;
    }
    function offsetToJd(offset) {
      return startJd + offset / 1440.0;
    }
    function svgXFromEvent(event, svgRef, svgWidth) {
      if (!svgRef.current) {
        return null;
      }
      const rect = svgRef.current.getBoundingClientRect();
      if (!rect.width) {
        return null;
      }
      return ((event.clientX - rect.left) / rect.width) * svgWidth;
    }
    function offsetFromSvgX(svgX) {
      const clampedX = clamp(svgX, ampOuter.left, ampOuter.left + plotWidth);
      const denom = plotWidth || 1.0;
      return xMin + ((clampedX - ampOuter.left) / denom) * (xMax - xMin || 1.0);
    }
    function intervalGroupAtOffset(offset) {
      const active = allIntervalGroups.filter(function (group) {
        const left = Math.min(group.start_offset_min, group.end_offset_min);
        const right = Math.max(group.start_offset_min, group.end_offset_min);
        return offset >= left && offset <= right;
      });
      if (!active.length) {
        return null;
      }
      active.sort(function (a, b) {
        const spanA = Math.abs(a.end_offset_min - a.start_offset_min);
        const spanB = Math.abs(b.end_offset_min - b.start_offset_min);
        return spanA - spanB;
      });
      return active[0].group_id;
    }
    function updateHover(event, svgRef, svgWidth) {
      const svgX = svgXFromEvent(event, svgRef, svgWidth);
      if (!Number.isFinite(svgX)) {
        return null;
      }
      const offset = offsetFromSvgX(svgX);
      setHoverOffset(offset);
      setHoverJd(offsetToJd(offset));
      setHoveredGroupId(intervalGroupAtOffset(offset));
      return offset;
    }
    function ampMap(value) {
      const lv = Math.log10(Math.max(value, ampMin));
      const lmin = Math.log10(ampMin);
      const lmax = Math.log10(ampMax);
      const denom = lmax - lmin || 1.0;
      return ampOuter.top + ((lmax - lv) / denom) * ampPlotHeight;
    }
    function phaseMap(value) {
      const denom = phaseMax - phaseMin || 1.0;
      return phaseOuter.top + ((phaseMax - value) / denom) * phasePlotHeight;
    }
    function ampTicks() {
      const out = [];
      const start = Math.ceil(Math.log10(ampMin));
      const end = Math.floor(Math.log10(ampMax));
      for (let power = start; power <= end; power += 1) {
        out.push(Math.pow(10, power));
      }
      if (!out.length) {
        out.push(ampMin, ampMax);
      }
      return out;
    }
    function phaseTicks() {
      return linearTicks(phaseMin, phaseMax, 3);
    }
    function pointElements(series, key, yMapFn) {
      return (series[key] || []).map(function (value, idx) {
        const x = xOffsets[idx];
        if (!Number.isFinite(x) || !Number.isFinite(value)) {
          return null;
        }
        if (key === "amp" && value <= 0) {
          return null;
        }
        return html`<circle
          key=${series.label + "-" + key + "-" + idx}
          cx=${xMap(x)}
          cy=${yMapFn(value)}
          r="2.3"
          fill=${series.color}
          opacity="0.95"
        />`;
      });
    }
    function ampTickLines() {
      return ampTicks().map(function (value) {
        const y = ampMap(value);
        const power = Math.round(Math.log10(value));
        return html`
          <g key=${"amp-tick-" + value}>
            <line x1=${ampOuter.left} x2=${ampOuter.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${ampOuter.left - 5} x2=${ampOuter.left} y1=${y} y2=${y} className="axis-line" />
            <text x=${ampOuter.left - 8} y=${y + 4} textAnchor="end" className="axis-label">
              10<tspan dy="-4" fontSize="75%">${String(power)}</tspan>
            </text>
          </g>
        `;
      });
    }
    function phaseTickLines() {
      return phaseTicks().map(function (value) {
        const y = phaseMap(value);
        return html`
          <g key=${"phase-tick-" + value}>
            <line x1=${phaseOuter.left} x2=${phaseOuter.left + plotWidth} y1=${y} y2=${y} className="grid-line" />
            <line x1=${phaseOuter.left - 5} x2=${phaseOuter.left} y1=${y} y2=${y} className="axis-line" />
            <text x=${phaseOuter.left - 8} y=${y + 4} textAnchor="end" className="axis-label">${formatNumber(value)}</text>
          </g>
        `;
      });
    }
    function phaseXTicks() {
      const ticks = data.tick_offsets || [];
      const labels = data.tick_labels || [];
      return ticks.map(function (value, idx) {
        const x = xMap(value);
        return html`
          <g key=${"phase-xtick-" + idx}>
            <line x1=${x} x2=${x} y1=${phaseOuter.top} y2=${phaseOuter.top + phasePlotHeight} className="grid-line" />
            <line x1=${x} x2=${x} y1=${phaseOuter.top + phasePlotHeight} y2=${phaseOuter.top + phasePlotHeight + 5} className="axis-line" />
            <text x=${x} y=${phaseHeight - 6} textAnchor="middle" className="axis-label">${labels[idx]}</text>
          </g>
        `;
      });
    }
    function intervalBands(top, plotHeight) {
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset);
        const right = xMap(rightOffset);
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g key=${top + "-interval-" + group.group_id} className=${hovered ? "interval-group hovered" : "interval-group"}>
            <rect x=${left} y=${top} width=${Math.max(right - left, 1.5)} height=${plotHeight} className="interval-band" />
            <line x1=${left} x2=${left} y1=${top} y2=${top + plotHeight} className="interval-boundary interval-start" />
            <line x1=${right} x2=${right} y1=${top} y2=${top + plotHeight} className="interval-boundary interval-end" />
          </g>
        `;
      });
    }
    function deleteIntervalGroup(groupId) {
      if (String(groupId).indexOf("staged:") === 0) {
        props.onDeletePendingInterval(groupId);
      } else {
        props.onDeleteInterval(groupId);
      }
    }
    function intervalDeleteButtons(top) {
      const buttonSize = 15;
      return allIntervalGroups.map(function (group) {
        const leftOffset = Math.min(group.start_offset_min, group.end_offset_min);
        const rightOffset = Math.max(group.start_offset_min, group.end_offset_min);
        const left = xMap(leftOffset);
        const right = xMap(rightOffset);
        const x = clamp(right - buttonSize - 2, left + 1, ampOuter.left + plotWidth - buttonSize - 1);
        const y = top + 2;
        const hovered = hoveredGroupId === group.group_id;
        return html`
          <g
            key=${top + "-interval-delete-" + group.group_id}
            className=${hovered ? "interval-delete hovered" : "interval-delete"}
            onMouseEnter=${function () {
              setHoveredGroupId(group.group_id);
            }}
            onMouseLeave=${function () {
              setHoveredGroupId(function (current) {
                return current === group.group_id ? null : current;
              });
            }}
            onPointerDown=${function (event) {
              event.stopPropagation();
            }}
            onClick=${function (event) {
              event.stopPropagation();
              deleteIntervalGroup(group.group_id);
            }}
          >
            <rect x=${x} y=${y} width=${buttonSize} height=${buttonSize} rx="3" ry="3" className="interval-delete-bg" />
            <text x=${x + buttonSize / 2} y=${y + buttonSize / 2 + 0.5} textAnchor="middle" dominantBaseline="middle" className="interval-delete-label">
              ×
            </text>
          </g>
        `;
      });
    }
    function crosshair(top, plotHeight) {
      if (!Number.isFinite(hoverOffset)) {
        return null;
      }
      const x = xMap(hoverOffset);
      return html`<line x1=${x} x2=${x} y1=${top} y2=${top + plotHeight} className="crosshair-line" />`;
    }
    function pendingAnchorLine(top, plotHeight) {
      if (!Number.isFinite(pendingAnchorJd)) {
        return null;
      }
      const offset = (pendingAnchorJd - startJd) * 1440.0;
      const x = xMap(offset);
      return html`<line x1=${x} x2=${x} y1=${top} y2=${top + plotHeight} className="anchor-line" />`;
    }
    function dragBand(top, plotHeight) {
      if (!dragState) {
        return null;
      }
      const left = xMap(Math.min(dragState.startOffset, dragState.currentOffset));
      const right = xMap(Math.max(dragState.startOffset, dragState.currentOffset));
      return html`<rect x=${left} y=${top} width=${Math.max(right - left, 1.5)} height=${plotHeight} className="drag-band" />`;
    }
    function pointerDown(event, svgRef, svgWidth) {
      if (props.busy) {
        return;
      }
      focusShell();
      const offset = updateHover(event, svgRef, svgWidth);
      if (!Number.isFinite(offset)) {
        return;
      }
      setPendingAnchorJd(null);
      setDragState({
        startOffset: offset,
        currentOffset: offset,
        startClientX: event.clientX,
        currentClientX: event.clientX,
      });
      if (event.currentTarget && event.currentTarget.setPointerCapture) {
        event.currentTarget.setPointerCapture(event.pointerId);
      }
      if (props.onStatus) {
        props.onStatus("Drag to stage flagged range. Click Apply Mask to apply.");
      }
    }
    function pointerMove(event, svgRef, svgWidth) {
      const offset = updateHover(event, svgRef, svgWidth);
      if (!Number.isFinite(offset)) {
        return;
      }
      setDragState(function (current) {
        if (!current) {
          return current;
        }
        return Object.assign({}, current, {
          currentOffset: offset,
          currentClientX: event.clientX,
        });
      });
    }
    function pointerUp(event, svgRef, svgWidth) {
      const offsetRaw = updateHover(event, svgRef, svgWidth);
      const offset = Number.isFinite(offsetRaw) ? offsetRaw : dragState ? dragState.currentOffset : null;
      if (event.currentTarget && event.currentTarget.releasePointerCapture) {
        try {
          event.currentTarget.releasePointerCapture(event.pointerId);
        } catch (_err) {}
      }
      if (!dragState || !Number.isFinite(offset)) {
        setDragState(null);
        return;
      }
      const start = offsetToJd(dragState.startOffset);
      const end = offsetToJd(offset);
      const dragWidthPx = Math.abs((Number.isFinite(event.clientX) ? event.clientX : dragState.currentClientX) - dragState.startClientX);
      setDragState(null);
      if (dragWidthPx < TIME_FLAG_MIN_DRAG_PX || Math.abs(end - start) <= 1.0e-9) {
        if (props.onStatus) {
          props.onStatus("Ignored click without drag.");
        }
        return;
      }
      props.onStageInterval(start, end);
    }
    function pointerLeave() {
      if (!dragState) {
        setHoverOffset(null);
        setHoverJd(null);
        setHoveredGroupId(null);
      }
    }
    function deleteHoveredInterval() {
      if (!hoveredGroupId) {
        if (props.onStatus) {
          props.onStatus("X ignored. Hover an interval first.");
        }
        return;
      }
      deleteIntervalGroup(hoveredGroupId);
    }
    function handleKeyDown(event) {
      const target = event.target;
      if (target && target.tagName && /^(INPUT|TEXTAREA|SELECT|BUTTON)$/.test(target.tagName)) {
        return;
      }
      const key = String(event.key || "").toUpperCase();
      if (key === "A") {
        event.preventDefault();
        if (!Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("A ignored. Hover inside Time History first.");
          }
          return;
        }
        setPendingAnchorJd(hoverJd);
        if (props.onStatus) {
          props.onStatus("A anchor set at " + formatUtcTime(hoverJd) + " UT.");
        }
      } else if (key === "B") {
        event.preventDefault();
        if (!Number.isFinite(pendingAnchorJd) || !Number.isFinite(hoverJd)) {
          if (props.onStatus) {
            props.onStatus("B ignored. Set A first, then hover a second time.");
          }
          return;
        }
        props.onStageInterval(pendingAnchorJd, hoverJd);
        setPendingAnchorJd(null);
      } else if (key === "X") {
        event.preventDefault();
        deleteHoveredInterval();
      } else if (key === "ESCAPE") {
        event.preventDefault();
        setPendingAnchorJd(null);
        if (props.onStatus) {
          props.onStatus("Cleared pending A anchor.");
        }
      }
    }

    return html`
      <div className="time-history-shell compact-time-history-shell" ref=${wrapperRef} tabIndex="0" onKeyDown=${handleKeyDown}>
        <div className="time-history-toolbar">
          <div className="scope-pills" role="group" aria-label="Time-flag scope">
            ${TIME_FLAG_SCOPES.map(function (item) {
              return html`
                <button
                  type="button"
                  key=${item.id}
                  className=${"scope-pill" + (props.scope === item.id ? " active" : "")}
                  disabled=${props.busy}
                  onClick=${function () {
                    props.onScopeChange(item.id);
                  }}
                >
                  ${item.label}
                </button>
              `;
            })}
          </div>
        </div>
        <svg
          ref=${ampSvgRef}
          viewBox=${"0 0 " + width + " " + ampHeight}
          className="svg-plot"
          preserveAspectRatio="xMidYMid meet"
          style=${{ aspectRatio: width + " / " + ampHeight }}
          role="img"
          aria-label=${"Time history amplitude " + data.title}
          onMouseEnter=${focusShell}
        >
          ${ampTickLines()}
          ${intervalBands(ampOuter.top, ampPlotHeight)}
          ${dragBand(ampOuter.top, ampPlotHeight)}
          ${crosshair(ampOuter.top, ampPlotHeight)}
          ${pendingAnchorLine(ampOuter.top, ampPlotHeight)}
          <rect x=${ampOuter.left} y=${ampOuter.top} width=${plotWidth} height=${ampPlotHeight} className="plot-frame" />
          <text x=${ampOuter.left + 6} y=${ampOuter.top + 12} className="panel-title">Amplitude</text>
          <text
            x="16"
            y=${ampOuter.top + ampPlotHeight / 2}
            transform=${"rotate(-90 16 " + (ampOuter.top + ampPlotHeight / 2) + ")"}
            className="axis-label"
          >
            Amp
          </text>
          ${(data.series || []).map(function (series) {
            return pointElements(series, "amp", ampMap);
          })}
          <rect
            x=${ampOuter.left}
            y=${ampOuter.top}
            width=${plotWidth}
            height=${ampPlotHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, ampSvgRef, width);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, ampSvgRef, width);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, ampSvgRef, width);
            }}
            onPointerLeave=${pointerLeave}
          />
          ${intervalDeleteButtons(ampOuter.top)}
        </svg>
        <svg
          ref=${phaseSvgRef}
          viewBox=${"0 0 " + width + " " + phaseHeight}
          className="svg-plot"
          preserveAspectRatio="xMidYMid meet"
          style=${{ aspectRatio: width + " / " + phaseHeight }}
          role="img"
          aria-label=${"Time history phase " + data.title}
          onMouseEnter=${focusShell}
        >
          ${phaseTickLines()}
          ${phaseXTicks()}
          ${intervalBands(phaseOuter.top, phasePlotHeight)}
          ${dragBand(phaseOuter.top, phasePlotHeight)}
          ${crosshair(phaseOuter.top, phasePlotHeight)}
          ${pendingAnchorLine(phaseOuter.top, phasePlotHeight)}
          <rect x=${phaseOuter.left} y=${phaseOuter.top} width=${plotWidth} height=${phasePlotHeight} className="plot-frame" />
          <text x=${phaseOuter.left + 6} y=${phaseOuter.top + 12} className="panel-title">Phase</text>
          <text
            x="16"
            y=${phaseOuter.top + phasePlotHeight / 2}
            transform=${"rotate(-90 16 " + (phaseOuter.top + phasePlotHeight / 2) + ")"}
            className="axis-label"
          >
            Phase
          </text>
          <text x=${phaseOuter.left + plotWidth / 2} y=${phaseHeight - 6} textAnchor="middle" className="axis-label">Time [UT]</text>
          ${(data.series || []).map(function (series) {
            return pointElements(series, "phase", phaseMap);
          })}
          <rect
            x=${phaseOuter.left}
            y=${phaseOuter.top}
            width=${plotWidth}
            height=${phasePlotHeight}
            className="time-history-hitbox"
            onPointerDown=${function (event) {
              pointerDown(event, phaseSvgRef, width);
            }}
            onPointerMove=${function (event) {
              pointerMove(event, phaseSvgRef, width);
            }}
            onPointerUp=${function (event) {
              pointerUp(event, phaseSvgRef, width);
            }}
            onPointerLeave=${pointerLeave}
          />
        </svg>
      </div>
    `;
  }

  function App() {
    const [sessionId, setSessionId] = useState(null);
    const [state, setState] = useState(null);
    const [dateText, setDateText] = useState(todayIso());
    const [requestedDateText, setRequestedDateText] = useState(null);
    const [checkedScanIds, setCheckedScanIds] = useState([]);
    const [scanSelectionAnchor, setScanSelectionAnchor] = useState(null);
    const [dataRevision, setDataRevision] = useState(0);
    const [selectionRevision, setSelectionRevision] = useState(0);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState("");
    const [interactionMessage, setInteractionMessage] = useState("");
    const [timeFlagScope, setTimeFlagScope] = useState("selected");
    const [inbandAntennaScope, setInbandAntennaScope] = useState("selected");
    const [inbandPolScope, setInbandPolScope] = useState("selected");
    const [phacalMaskMode, setPhacalMaskMode] = useState("shared_xy");
    const [delayDraft, setDelayDraft] = useState({ ant: 1, x: "", y: "" });
    const [relativeDelayDraft, setRelativeDelayDraft] = useState({ ant: 1, x: "", y: "", xoff: "", yoff: "" });
    const [residualPatchDraft, setResidualPatchDraft] = useState({
      ant: 1,
      xBlue: "",
      yBlue: "",
      xRed: "",
      yRed: "",
    });
    const [ant1DxyDraft, setAnt1DxyDraft] = useState("");
    const [phacalEditorAdvanced, setPhacalEditorAdvanced] = useState(true);
    const [controlRailCollapsed, setControlRailCollapsed] = useState(false);
    const [controlRailWidthPx, setControlRailWidthPx] = useState(315);
    const [lastExpandedControlRailWidthPx, setLastExpandedControlRailWidthPx] = useState(315);
    const [yxThresholdDraft, setYxThresholdDraft] = useState(String(1.5));
    const [residualThresholdDraft, setResidualThresholdDraft] = useState(String(1.0));
    const [timeHistoryData, setTimeHistoryData] = useState(null);
    const [heatmapData, setHeatmapData] = useState(null);
    const [overviewData, setOverviewData] = useState(null);
    const [compareData, setCompareData] = useState(null);
    const [stagedTimeIntervals, setStagedTimeIntervals] = useState([]);
    const [stagedInbandPanels, setStagedInbandPanels] = useState({});
    const [stagedInbandMasks, setStagedInbandMasks] = useState({});
    const [stagedResidualPanels, setStagedResidualPanels] = useState({});
    const [stagedResidualMasks, setStagedResidualMasks] = useState({});
    const [stagedRelPhasePanels, setStagedRelPhasePanels] = useState({});
    const [stagedRelPhaseMasks, setStagedRelPhaseMasks] = useState({});
    const [heatmapLoadedRevision, setHeatmapLoadedRevision] = useState(-1);
    const [timeHistoryLoadedRevision, setTimeHistoryLoadedRevision] = useState(-1);
    const [overviewLoadedRevision, setOverviewLoadedRevision] = useState(-1);
    const [pendingProgressLoad, setPendingProgressLoad] = useState(null);
    const [activity, setActivity] = useState(null);
    const [inbandResidualInspector, setInbandResidualInspector] = useState(null);
    const [paneMaxHeight, setPaneMaxHeight] = useState(null);
    const [toolbarCollapsed, setToolbarCollapsed] = useState(function () {
      if (typeof window === "undefined") {
        return false;
      }
      try {
        return window.localStorage.getItem("calwidget_v2_toolbar_collapsed") === "1";
      } catch (_err) {
        return false;
      }
    });
    const progressTimerRef = useRef(null);
    const finishTimerRef = useRef(null);
    const ant1DxyPreviewSeqRef = useRef(0);
    const ant1DxyPreviewActiveRef = useRef(false);
    const shellRef = useRef(null);
    const toolbarRef = useRef(null);
    const controlRailDragRef = useRef(null);

    function clearActivityTimers() {
      if (progressTimerRef.current) {
        window.clearInterval(progressTimerRef.current);
        progressTimerRef.current = null;
      }
      if (finishTimerRef.current) {
        window.clearTimeout(finishTimerRef.current);
        finishTimerRef.current = null;
      }
    }

    useEffect(function () {
      return function () {
        clearActivityTimers();
      };
    }, []);

    useEffect(
      function () {
        if (!pendingProgressLoad) {
          return;
        }
        const loadedCount = [
          heatmapLoadedRevision >= pendingProgressLoad.revision,
          timeHistoryLoadedRevision >= pendingProgressLoad.revision,
          overviewLoadedRevision >= pendingProgressLoad.revision,
        ].filter(Boolean).length;
        setActivity(function (current) {
          if (!current || current.kind !== pendingProgressLoad.kind) {
            return current;
          }
          const progress = loadedCount >= 3 ? current.progress : 94 + loadedCount * 2;
          return Object.assign({}, current, {
            progress: Math.max(current.progress, progress),
            stage: loadedCount > 0 && loadedCount < 3
              ? "Loading plots and refreshed products (" + String(loadedCount) + "/3)"
              : current.stage,
          });
        });
        if (
          heatmapLoadedRevision < pendingProgressLoad.revision ||
          timeHistoryLoadedRevision < pendingProgressLoad.revision ||
          overviewLoadedRevision < pendingProgressLoad.revision
        ) {
          return;
        }
        finishProgressActivity(pendingProgressLoad.kind, pendingProgressLoad.message, false);
        setPendingProgressLoad(null);
        setBusy(false);
      },
      [pendingProgressLoad, heatmapLoadedRevision, timeHistoryLoadedRevision, overviewLoadedRevision]
    );

    useEffect(function () {
      jsonFetch("/api/session", { method: "POST" })
        .then(function (data) {
          setSessionId(data.session_id);
        })
        .catch(function (err) {
          setError(err.message);
        });
    }, []);

    useEffect(
      function () {
        if (typeof window === "undefined") {
          return;
        }
        try {
          window.localStorage.setItem("calwidget_v2_toolbar_collapsed", toolbarCollapsed ? "1" : "0");
        } catch (_err) {}
      },
      [toolbarCollapsed]
    );

    useEffect(
      function () {
        function updatePaneMaxHeight() {
          if (!shellRef.current || !toolbarRef.current || typeof window === "undefined") {
            return;
          }
          const shellStyle = window.getComputedStyle(shellRef.current);
          const paddingTop = parseFloat(shellStyle.paddingTop) || 0;
          const paddingBottom = parseFloat(shellStyle.paddingBottom) || 0;
          const gap = parseFloat(shellStyle.rowGap || shellStyle.gap) || 0;
          const toolbarHeight = toolbarRef.current.getBoundingClientRect().height || 0;
          const available = Math.floor(window.innerHeight - paddingTop - paddingBottom - gap - toolbarHeight);
          setPaneMaxHeight(Math.max(260, available));
        }
        updatePaneMaxHeight();
        window.addEventListener("resize", updatePaneMaxHeight);
        let resizeObserver = null;
        if (typeof ResizeObserver !== "undefined" && toolbarRef.current) {
          resizeObserver = new ResizeObserver(updatePaneMaxHeight);
          resizeObserver.observe(toolbarRef.current);
        }
        return function () {
          window.removeEventListener("resize", updatePaneMaxHeight);
          if (resizeObserver) {
            resizeObserver.disconnect();
          }
        };
      },
      []
    );

    useEffect(
      function () {
        function handlePointerMove(event) {
          if (!controlRailDragRef.current) {
            return;
          }
          const nextWidth = clamp(controlRailDragRef.current.startWidth + (event.clientX - controlRailDragRef.current.startX), 260, 520);
          setControlRailWidthPx(nextWidth);
          setLastExpandedControlRailWidthPx(nextWidth);
        }
        function handlePointerUp() {
          if (!controlRailDragRef.current) {
            return;
          }
          controlRailDragRef.current = null;
          if (typeof document !== "undefined") {
            document.body.style.cursor = "";
            document.body.style.userSelect = "";
          }
        }
        window.addEventListener("pointermove", handlePointerMove);
        window.addEventListener("pointerup", handlePointerUp);
        return function () {
          window.removeEventListener("pointermove", handlePointerMove);
          window.removeEventListener("pointerup", handlePointerUp);
        };
      },
      []
    );

    useEffect(
      function () {
        setInbandResidualInspector(null);
        setStagedTimeIntervals([]);
        setStagedInbandPanels({});
        setStagedInbandMasks({});
        setStagedResidualPanels({});
        setStagedResidualMasks({});
      },
      [dataRevision]
    );

    function startControlRailResize(event) {
      if (controlRailCollapsed) {
        return;
      }
      controlRailDragRef.current = {
        startX: event.clientX,
        startWidth: controlRailWidthPx,
      };
      if (typeof document !== "undefined") {
        document.body.style.cursor = "col-resize";
        document.body.style.userSelect = "none";
      }
      event.preventDefault();
    }

    function toggleControlRailCollapsed() {
      setControlRailCollapsed(function (current) {
        if (current) {
          const nextWidth = clamp(lastExpandedControlRailWidthPx || 315, 260, 520);
          setControlRailWidthPx(nextWidth);
          return false;
        }
        setLastExpandedControlRailWidthPx(controlRailWidthPx);
        return true;
      });
    }

    function syncDraft(nextState) {
      const selectedAnt = nextState ? nextState.selected_ant : 0;
      const currentKind = nextState && nextState.current_scan ? nextState.current_scan.kind : null;
      const active = nextState ? nextState.active_refcal : null;
      const activePhacal = currentKind === "phacal" && nextState ? nextState.active_phacal : null;
      setDelayDraft({
        ant: selectedAnt + 1,
        x: active && active.x_delay_ns !== null ? String(active.x_delay_ns.toFixed(3)) : "",
        y: active && active.y_delay_ns !== null ? String(active.y_delay_ns.toFixed(3)) : "",
      });
      setRelativeDelayDraft({
        ant: selectedAnt + 1,
        x:
          activePhacal && activePhacal.x_applied_delay_ns !== null
            ? String(Number(activePhacal.x_applied_delay_ns).toFixed(3))
            : active && active.x_applied_relative_delay_ns !== null
              ? String(active.x_applied_relative_delay_ns.toFixed(3))
              : active && active.x_relative_delay_ns !== null
                ? String(active.x_relative_delay_ns.toFixed(3))
                : "",
        y:
          activePhacal && activePhacal.y_applied_delay_ns !== null
            ? String(Number(activePhacal.y_applied_delay_ns).toFixed(3))
            : active && active.y_applied_relative_delay_ns !== null
              ? String(active.y_applied_relative_delay_ns.toFixed(3))
              : active && active.y_relative_delay_ns !== null
                ? String(active.y_relative_delay_ns.toFixed(3))
                : "",
        xoff:
          activePhacal && activePhacal.x_applied_offset_rad !== null
            ? String(Number(activePhacal.x_applied_offset_rad).toFixed(3))
            : "",
        yoff:
          activePhacal && activePhacal.y_applied_offset_rad !== null
            ? String(Number(activePhacal.y_applied_offset_rad).toFixed(3))
            : "",
      });
      setResidualPatchDraft({
        ant: selectedAnt + 1,
        xBlue:
          active && active.x_residual_inband_suggest_ns !== null && active.x_residual_inband_suggest_ns !== undefined
            ? String(Number(active.x_residual_inband_suggest_ns).toFixed(3))
            : "",
        yBlue:
          active && active.y_residual_inband_suggest_ns !== null && active.y_residual_inband_suggest_ns !== undefined
            ? String(Number(active.y_residual_inband_suggest_ns).toFixed(3))
            : "",
        xRed:
          active && active.x_suggested_relative_delay_ns !== null && active.x_suggested_relative_delay_ns !== undefined
            ? String(Number(active.x_suggested_relative_delay_ns).toFixed(3))
            : "",
        yRed:
          active && active.y_suggested_relative_delay_ns !== null && active.y_suggested_relative_delay_ns !== undefined
            ? String(Number(active.y_suggested_relative_delay_ns).toFixed(3))
            : "",
      });
      setYxThresholdDraft(
        active && active.yx_residual_threshold_rad !== null && active.yx_residual_threshold_rad !== undefined
          ? String(Number(active.yx_residual_threshold_rad).toFixed(2))
          : String(1.5)
      );
      setResidualThresholdDraft(
        active && active.residual_band_threshold_rad !== null && active.residual_band_threshold_rad !== undefined
          ? String(Number(active.residual_band_threshold_rad).toFixed(2))
          : String(1.0)
      );
      setAnt1DxyDraft(
        active && active.ant1_manual_dxy_corr_rad !== null && active.ant1_manual_dxy_corr_rad !== undefined
          ? String(Number(active.ant1_manual_dxy_corr_rad).toFixed(3))
          : String(0.0)
      );
    }

    async function refresh() {
      if (!sessionId) {
        return;
      }
      const next = await jsonFetch("/api/state?session_id=" + encodeURIComponent(sessionId));
      setState(next);
      setInteractionMessage("");
      syncDraft(next);
      setDataRevision(function (value) {
        return value + 1;
      });
      setSelectionRevision(function (value) {
        return value + 1;
      });
    }

    function startProgressActivity(kind) {
      const profile = ACTION_PROGRESS[kind];
      if (!profile) {
        return;
      }
      clearActivityTimers();
      const startedAt = Date.now();
      setActivity({
        kind: kind,
        title: profile.label,
        stage: profile.stages[0],
        progress: 4,
        error: false,
      });
      progressTimerRef.current = window.setInterval(function () {
        const elapsed = Date.now() - startedAt;
        setActivity(function (current) {
          if (!current || current.kind !== kind) {
            return current;
          }
          const progress = Math.min(92, 4 + (elapsed / profile.paceMs) * 88);
          const stageIdx = Math.min(profile.stages.length - 1, Math.floor((progress - 4) / 24));
          return Object.assign({}, current, {
            progress: progress,
            stage: profile.stages[stageIdx],
          });
        });
      }, 160);
    }

    function finishProgressActivity(kind, message, isError) {
      clearActivityTimers();
      setActivity(function (current) {
        if (!current || current.kind !== kind) {
          return current;
        }
        return Object.assign({}, current, {
          progress: isError ? current.progress : 100,
          stage: message,
          error: !!isError,
        });
      });
      finishTimerRef.current = window.setTimeout(function () {
        setActivity(null);
      }, isError ? 2800 : 900);
    }

    async function runAction(fn, options) {
      setBusy(true);
      setError("");
      const progressKind = options && options.progressKind ? options.progressKind : null;
      const progressProfile = progressKind ? ACTION_PROGRESS[progressKind] : null;
      let deferBusyClear = false;
      if (progressProfile) {
        startProgressActivity(progressKind);
      }
      try {
        await fn();
        if (progressProfile && progressProfile.waitForPlots) {
          setActivity(function (current) {
            if (!current || current.kind !== progressKind) {
              return current;
            }
            return Object.assign({}, current, {
              progress: Math.max(current.progress, 94),
              stage: "Loading plots and refreshed products",
            });
          });
          setPendingProgressLoad({
            kind: progressKind,
            revision: dataRevision + 1,
            message: (options && options.successMessage) || progressProfile.success,
          });
          deferBusyClear = true;
        } else if (progressProfile) {
          finishProgressActivity(progressKind, (options && options.successMessage) || progressProfile.success, false);
        }
      } catch (err) {
        setPendingProgressLoad(null);
        if (progressProfile) {
          finishProgressActivity(progressKind, err.message || String(err), true);
        }
        setError(err.message || String(err));
      } finally {
        if (!deferBusyClear) {
          setBusy(false);
        }
      }
    }

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        refresh().catch(function () {});
      },
      [sessionId]
    );

    useEffect(
      function () {
        if (!sessionId || busy) {
          return;
        }
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) {
          return;
        }
        if (dateText === requestedDateText) {
          return;
        }
        setRequestedDateText(dateText);
        runAction(function () {
          return loadDate(dateText);
        });
      },
      [sessionId, dateText, busy, requestedDateText]
    );

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        let cancelled = false;
        const revision = dataRevision;
        setHeatmapData(null);
        jsonFetch("/api/plot/heatmap-data?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setHeatmapData(data);
              setHeatmapLoadedRevision(revision);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setHeatmapData({ message: err.message || String(err) });
              setHeatmapLoadedRevision(revision);
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision]
    );

    useEffect(
      function () {
        if (!sessionId || !(state && state.compare_ref_scan_ids && state.compare_ref_scan_ids.length === 2)) {
          setCompareData(null);
          return;
        }
        let cancelled = false;
        jsonFetch("/api/plot/refcal-compare?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setCompareData(data);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setCompareData({ message: err.message || String(err) });
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision, state && state.compare_ref_scan_ids ? state.compare_ref_scan_ids.join(",") : ""]
    );

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        let cancelled = false;
        const revision = dataRevision;
        setTimeHistoryData(null);
        jsonFetch("/api/plot/time-history?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setTimeHistoryData(data);
              setTimeHistoryLoadedRevision(revision);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setTimeHistoryData({ message: err.message || String(err) });
              setTimeHistoryLoadedRevision(revision);
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision, selectionRevision]
    );

    useEffect(
      function () {
        if (!sessionId) {
          return;
        }
        let cancelled = false;
        const revision = dataRevision;
        setOverviewData(null);
        jsonFetch("/api/plot/overview-data?session_id=" + encodeURIComponent(sessionId))
          .then(function (data) {
            if (!cancelled) {
              setOverviewData(data);
              setOverviewLoadedRevision(revision);
            }
          })
          .catch(function (err) {
            if (!cancelled) {
              setOverviewData({
                sum_amp: { message: err.message || String(err), title: "Sum Amp" },
                sum_pha: { message: err.message || String(err), title: "Sum Pha" },
                phacal_phase_compare: { message: err.message || String(err), title: "Refcal vs Phacal Phase" },
                inband_fit: { message: err.message || String(err), title: "Inband Fit" },
                inband_residual_phase_band: { message: err.message || String(err), title: "Per-Band Residual Phase" },
                inband_relative_phase: { message: err.message || String(err), title: "Relative Phase + Fit" },
                inband_residual_delay_band: { message: err.message || String(err), title: "Residual Delay Per Band" },
              });
              setOverviewLoadedRevision(revision);
            }
          });
        return function () {
          cancelled = true;
        };
      },
      [sessionId, dataRevision]
    );

    async function loadDate(dateOverride) {
      if (!sessionId) {
        return;
      }
      const targetDate = dateOverride || dateText;
      const next = await jsonFetch(
        "/api/scans?session_id=" + encodeURIComponent(sessionId) + "&date=" + encodeURIComponent(targetDate)
      );
      setState(next);
      setInteractionMessage("");
      syncDraft(next);
      setCheckedScanIds([]);
      setScanSelectionAnchor(next && next.selected_scan_id !== null ? next.selected_scan_id : null);
      setDataRevision(function (value) {
        return value + 1;
      });
      setSelectionRevision(function (value) {
        return value + 1;
      });
    }

    async function postJson(url, payload, options) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (options && options.selectionOnly) {
        setSelectionRevision(function (value) {
          return value + 1;
        });
        return next;
      }
      setDataRevision(function (value) {
        return value + 1;
      });
      setSelectionRevision(function (value) {
        return value + 1;
      });
      return next;
    }

    async function postJsonWithOverviewPatch(url, payload, antennaIndex) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (next && next.overview_updates) {
        const responseAntenna = next.updated_antenna;
        const patchAntenna = Number.isFinite(responseAntenna) ? responseAntenna : antennaIndex;
        setOverviewData(function (current) {
          return mergeOverviewAntennaUpdates(current, next.overview_updates, patchAntenna);
        });
      }
      if (next && next.heatmap) {
        setHeatmapData(next.heatmap);
      }
      return next;
    }

    async function postJsonWithOverviewRefresh(url, payload) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (next && next.overview) {
        setOverviewData(next.overview);
      }
      if (next && next.heatmap) {
        setHeatmapData(next.heatmap);
      }
      return next;
    }

    async function previewOverviewSection(url, payload) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (next && next.overview_updates) {
        setOverviewData(function (current) {
          return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
        });
      }
      return next;
    }

    async function postJsonWithTimeFlagPatch(url, payload) {
      const next = await jsonFetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const nextState = next && next.state ? next.state : next;
      setState(nextState);
      setInteractionMessage("");
      syncDraft(nextState);
      if (next && next.overview_updates) {
        setOverviewData(function (current) {
          return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
        });
      }
      if (next && next.heatmap) {
        setHeatmapData(next.heatmap);
      }
      if (next && next.time_history) {
        setTimeHistoryData(next.time_history);
      }
      return next;
    }

    function selectedScanId() {
      return state && state.selected_scan_id !== null ? state.selected_scan_id : null;
    }

    function orderedScanIds(scanIds) {
      const scans = state && state.scans ? state.scans : [];
      const order = new Map(
        scans.map(function (scan, idx) {
          return [scan.scan_id, idx];
        })
      );
      return Array.from(new Set(scanIds)).sort(function (left, right) {
        return (order.has(left) ? order.get(left) : Number.MAX_SAFE_INTEGER) - (order.has(right) ? order.get(right) : Number.MAX_SAFE_INTEGER);
      });
    }

    function scanRangeIds(anchorId, targetId) {
      const scans = state && state.scans ? state.scans : [];
      const anchorIndex = scans.findIndex(function (scan) {
        return scan.scan_id === anchorId;
      });
      const targetIndex = scans.findIndex(function (scan) {
        return scan.scan_id === targetId;
      });
      if (anchorIndex < 0 || targetIndex < 0) {
        return [targetId];
      }
      const start = Math.min(anchorIndex, targetIndex);
      const stop = Math.max(anchorIndex, targetIndex) + 1;
      return scans.slice(start, stop).map(function (scan) {
        return scan.scan_id;
      });
    }

    function onScanRowClick(event, scanId) {
      if (!sessionId) {
        return;
      }
      const isRange = !!(event && event.shiftKey);
      const isToggle = !!(event && (event.metaKey || event.ctrlKey));
      const anchorId = scanSelectionAnchor !== null ? scanSelectionAnchor : selectedScanId();
      if (isRange) {
        const nextSelection = anchorId !== null ? scanRangeIds(anchorId, scanId) : [scanId];
        setCheckedScanIds(orderedScanIds(nextSelection));
        setScanSelectionAnchor(anchorId !== null ? anchorId : scanId);
      } else if (isToggle) {
        setCheckedScanIds(function (current) {
          const exists = current.indexOf(scanId) >= 0;
          return orderedScanIds(
            exists
              ? current.filter(function (value) {
                  return value !== scanId;
                })
              : current.concat([scanId])
          );
        });
        setScanSelectionAnchor(scanId);
      } else {
        setCheckedScanIds([]);
        setScanSelectionAnchor(scanId);
      }
      runAction(function () {
        return postJson("/api/select-scan", { session_id: sessionId, scan_id: scanId });
      });
    }

    function updateSelection(ant, band) {
      if (!sessionId || !state) {
        return Promise.resolve();
      }
      const nextAnt = Math.max(0, ant);
      const nextBand = Math.max(0, band);
      if (state.selected_ant === nextAnt && state.selected_band === nextBand) {
        return Promise.resolve();
      }
      return postJson(
        "/api/select-band",
        {
          session_id: sessionId,
          antenna: nextAnt,
          band: nextBand,
        },
        { selectionOnly: true }
      );
    }

    function onHeatmapSelect(ant, band) {
      if (busy) {
        return;
      }
      if (state && state.selected_ant === ant && state.selected_band === band) {
        return;
      }
      runAction(function () {
        return updateSelection(ant, band);
      });
    }

    function stageTimeFlag(startJd, endJd) {
      if (!state) {
        return;
      }
      const normalized = normalizeJdInterval(startJd, endJd);
      if (Math.abs(normalized.end_jd - normalized.start_jd) <= 1.0e-9) {
        setInteractionMessage("Ignored click without drag.");
        return;
      }
      const pending = {
        temp_id: "staged:" + Date.now().toString(36) + ":" + Math.random().toString(36).slice(2, 8),
        antenna: Number(state.selected_ant),
        band: Number(state.selected_band),
        scope: String(timeFlagScope),
        start_jd: Number(normalized.start_jd),
        end_jd: Number(normalized.end_jd),
      };
      setStagedTimeIntervals(function (current) {
        return mergeTimeFlagIntervals(current, pending);
      });
      setInteractionMessage("Time flag staged. Click Apply Mask to apply.");
    }

    function removeStagedTimeFlag(groupId) {
      setStagedTimeIntervals(function (current) {
        return current.filter(function (item) {
          return String(item.temp_id) !== String(groupId);
        });
      });
      setInteractionMessage("Removed staged time flag.");
    }

    function applyStagedTimeFlags() {
      if (busy || !sessionId || !stagedTimeIntervals.length) {
        return;
      }
      setInteractionMessage("");
      runAction(
        function () {
          return postJsonWithTimeFlagPatch("/api/time-flags/add-batch", {
            session_id: sessionId,
            intervals: stagedTimeIntervals.map(function (item) {
              return {
                antenna: Number(item.antenna),
                band: Number(item.band),
                start_jd: Number(item.start_jd),
                end_jd: Number(item.end_jd),
                scope: String(item.scope),
              };
            }),
          }).then(function (next) {
            setStagedTimeIntervals([]);
            return next;
          });
        },
        { progressKind: "time_flag", successMessage: "Time flag applied" }
      );
    }

    function deleteTimeFlag(groupId) {
      setInteractionMessage("");
      runAction(
        function () {
          return postJsonWithTimeFlagPatch("/api/time-flags/delete", {
            session_id: sessionId,
            group_id: groupId,
          });
        },
        { progressKind: "time_flag", successMessage: "Time flag removed" }
      );
    }

    function targetInbandRows(sourcePol) {
      const sourceRow = Number(sourcePol);
      if (isPhacalScan && sourceRow === 2) {
        return [];
      }
      if (sourceRow === 2) {
        return [0, 1];
      }
      const pol = clamp(sourceRow, 0, 1);
      if (isPhacalScan) {
        return phacalMaskMode === "per_pol" ? [pol] : [0, 1];
      }
      return inbandPolScope === "all" ? [0, 1] : [pol];
    }

    function targetInbandPanels(sourcePol, sourceAntenna) {
      const panelIdx = Math.max(0, Number(sourceAntenna));
      const baseSection = (
        overviewData &&
        (isPhacalScan ? overviewData.inband_fit : (overviewData.inband_fit || overviewData.inband_relative_phase))
      ) || null;
      const targets = [];
      targetInbandRows(sourcePol).forEach(function (rowIdx) {
        const row = baseSection && baseSection.panels && baseSection.panels[rowIdx] ? baseSection.panels[rowIdx] : [];
        if (inbandAntennaScope === "all") {
          row.forEach(function (_panel, idx) {
            targets.push({ rowIdx: rowIdx, panelIdx: idx });
          });
        } else if (row[panelIdx]) {
          targets.push({ rowIdx: rowIdx, panelIdx: panelIdx });
        }
      });
      return targets;
    }

    function editableInbandSectionIds() {
      return ["inband_fit"];
    }

    function stageInbandSelection(startBand, endBand, sourcePol, sourceAntenna, mode) {
      if (!overviewData) {
        return false;
      }
      const sectionIds = editableInbandSectionIds();
      const targets = targetInbandPanels(sourcePol, sourceAntenna);
      const nextOverrides = {};
      const nextMasks = {};
      sectionIds.forEach(function (sectionId) {
        const section = overviewData[sectionId];
        const bandEdges = section && section.band_edges ? section.band_edges : [];
        targets.forEach(function (target) {
          const originalPanel = section && section.panels && section.panels[target.rowIdx] ? section.panels[target.rowIdx][target.panelIdx] : null;
          if (!originalPanel || !bandEdges.length) {
            return;
          }
          const key = panelSelectionKey(sectionId, target.rowIdx, target.panelIdx);
          const currentPanel = stagedInbandPanels[key]
            ? Object.assign({}, originalPanel, stagedInbandPanels[key])
            : originalPanel;
          const nextRanges = updateKeptRanges(currentPanel.kept_ranges, bandEdges, startBand, endBand, mode);
          if (sameKeptRanges(nextRanges, originalPanel.kept_ranges)) {
            return;
          }
          nextOverrides[key] = optimisticPanelUpdate(currentPanel, nextRanges, bandEdges);
          nextMasks[targetMaskKey(target.rowIdx, target.panelIdx)] = {
            antenna: target.panelIdx,
            polarization: target.rowIdx,
            kept_ranges: nextRanges,
          };
        });
      });
      const keys = Object.keys(nextOverrides);
      if (keys.length) {
        setStagedInbandPanels(function (current) {
          return Object.assign({}, current, nextOverrides);
        });
        setStagedInbandMasks(function (current) {
          return Object.assign({}, current, nextMasks);
        });
        setInteractionMessage("Flag mask staged. Click Apply Mask to apply.");
      }
      return keys.length > 0;
    }

    function stageInbandMaskClear(rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const sectionIds = editableInbandSectionIds();
      const rowTargets = isPhacalScan && Number(rowIdx) === 2 ? [0, 1] : targetInbandRows(rowIdx);
      const nextOverrides = {};
      const clearPanelKeys = [];
      const nextMasks = {};
      const clearMaskKeys = [];
      sectionIds.forEach(function (sectionId) {
        const section = overviewData[sectionId];
        const bandEdges = section && section.band_edges ? section.band_edges : [];
        rowTargets.forEach(function (maskRowIdx) {
          const row = section && section.panels && section.panels[maskRowIdx] ? section.panels[maskRowIdx] : [];
          const originalPanel = row[panelIdx];
          if (!originalPanel || !bandEdges.length) {
            return;
          }
          const fullRanges = defaultKeptRanges(bandEdges);
          const panelKey = panelSelectionKey(sectionId, maskRowIdx, panelIdx);
          const maskKey = targetMaskKey(maskRowIdx, panelIdx);
          if (sameKeptRanges(fullRanges, originalPanel.kept_ranges)) {
            clearPanelKeys.push(panelKey);
            clearMaskKeys.push(maskKey);
            return;
          }
          nextOverrides[panelKey] = optimisticPanelUpdate(originalPanel, fullRanges, bandEdges);
          nextMasks[maskKey] = {
            antenna: panelIdx,
            polarization: maskRowIdx,
            kept_ranges: fullRanges,
          };
        });
      });
      if (!clearPanelKeys.length && !Object.keys(nextOverrides).length) {
        return false;
      }
      setStagedInbandPanels(function (current) {
        const next = Object.assign({}, current);
        clearPanelKeys.forEach(function (key) {
          delete next[key];
        });
        return Object.assign(next, nextOverrides);
      });
      setStagedInbandMasks(function (current) {
        const next = Object.assign({}, current);
        clearMaskKeys.forEach(function (key) {
          delete next[key];
        });
        return Object.assign(next, nextMasks);
      });
      setInteractionMessage("Mask cleared. Click Apply Mask to apply.");
      return true;
    }

    function clearStagedInbandSelection() {
      setStagedInbandPanels({});
      setStagedInbandMasks({});
    }

    function clearStagedRelPhaseSelection() {
      setStagedRelPhasePanels({});
      setStagedRelPhaseMasks({});
    }

    function panelWithStagedRelPhaseSelection(rowIdx, panelIdx, panel) {
      const override = stagedRelPhasePanels[panelSelectionKey("inband_relative_phase", rowIdx, panelIdx)];
      return override ? Object.assign({}, panel, override) : panel;
    }

    function stageRelPhaseSelection(startBand, endBand, sourcePol, sourceAntenna, mode) {
      if (!overviewData) {
        return false;
      }
      const section = overviewData.inband_relative_phase;
      const bandEdges = section && section.band_edges ? section.band_edges : [];
      const sourceRow = Number(sourcePol);
      const panelIdx = Math.max(0, Number(sourceAntenna));
      const antTargets = inbandAntennaScope === "all"
        ? (section && section.panels && section.panels[Math.min(sourceRow, 2)] ? section.panels[Math.min(sourceRow, 2)].map(function (_p, idx) { return idx; }) : [panelIdx])
        : [panelIdx];
      const nextOverrides = {};
      const nextMasks = {};
      antTargets.forEach(function (antIdx) {
        const rowsToUpdate = sourceRow === 2 ? [2] : [sourceRow];
        rowsToUpdate.forEach(function (rowIdx) {
          const row = section && section.panels && section.panels[rowIdx] ? section.panels[rowIdx] : [];
          const originalPanel = row[antIdx];
          if (!originalPanel || !bandEdges.length) {
            return;
          }
          const key = panelSelectionKey("inband_relative_phase", rowIdx, antIdx);
          const currentPanel = stagedRelPhasePanels[key]
            ? Object.assign({}, originalPanel, stagedRelPhasePanels[key])
            : originalPanel;
          const nextRanges = updateKeptRanges(currentPanel.kept_ranges, bandEdges, startBand, endBand, mode);
          if (sameKeptRanges(nextRanges, originalPanel.kept_ranges)) {
            return;
          }
          nextOverrides[key] = optimisticPanelUpdate(currentPanel, nextRanges, bandEdges);
          nextMasks[panelSelectionKey("inband_relative_phase", rowIdx, antIdx)] = {
            antenna: antIdx,
            polarization: rowIdx,
            kept_ranges: nextRanges,
          };
        });
      });
      const keys = Object.keys(nextOverrides);
      if (keys.length) {
        setStagedRelPhasePanels(function (current) {
          return Object.assign({}, current, nextOverrides);
        });
        setStagedRelPhaseMasks(function (current) {
          return Object.assign({}, current, nextMasks);
        });
        setInteractionMessage("Rel. Phase mask staged. Click Apply Mask to apply.");
      }
      return keys.length > 0;
    }

    function stageRelPhaseMaskClear(rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const section = overviewData.inband_relative_phase;
      const bandEdges = section && section.band_edges ? section.band_edges : [];
      const row = section && section.panels && section.panels[rowIdx] ? section.panels[rowIdx] : [];
      const originalPanel = row[panelIdx];
      if (!originalPanel || !bandEdges.length) {
        return false;
      }
      const fullRanges = defaultKeptRanges(bandEdges);
      const panelKey = panelSelectionKey("inband_relative_phase", rowIdx, panelIdx);
      if (sameKeptRanges(fullRanges, originalPanel.kept_ranges)) {
        setStagedRelPhasePanels(function (current) {
          const next = Object.assign({}, current);
          delete next[panelKey];
          return next;
        });
        setStagedRelPhaseMasks(function (current) {
          const next = Object.assign({}, current);
          delete next[panelKey];
          return next;
        });
        return false;
      }
      setStagedRelPhasePanels(function (current) {
        return Object.assign({}, current, {
          [panelKey]: optimisticPanelUpdate(originalPanel, fullRanges, bandEdges),
        });
      });
      setStagedRelPhaseMasks(function (current) {
        return Object.assign({}, current, {
          [panelKey]: {
            antenna: panelIdx,
            polarization: rowIdx,
            kept_ranges: fullRanges,
          },
        });
      });
      setInteractionMessage("Rel. Phase mask cleared. Click Apply Mask to apply.");
      return true;
    }

    function applyRelPhaseInbandWindow() {
      const targets = Object.values(stagedRelPhaseMasks);
      if (busy || !targets.length) {
        return;
      }
      setInteractionMessage("");
      runAction(
        function () {
          return jsonFetch("/api/inband/mask/batch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              targets: targets,
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            clearStagedRelPhaseSelection();
            return next;
          });
        },
        { progressKind: "inband_mask", successMessage: "Rel. Phase mask applied" }
      );
    }

    function clearStagedResidualSelection() {
      setStagedResidualPanels({});
      setStagedResidualMasks({});
    }

    function stageResidualSelection(startBand, endBand, rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const section = overviewData.inband_residual_phase_band;
      const bandEdges = section && section.band_edges ? section.band_edges : [];
      const row = section && section.panels && section.panels[rowIdx] ? section.panels[rowIdx] : [];
      const originalPanel = row[panelIdx];
      if (!originalPanel || !bandEdges.length) {
        return false;
      }
      const key = panelSelectionKey("inband_residual_phase_band", rowIdx, panelIdx);
      const currentPanel = stagedResidualPanels[key]
        ? Object.assign({}, originalPanel, stagedResidualPanels[key])
        : originalPanel;
      const nextRanges = excludeRangeFromKeptRanges(currentPanel.kept_ranges, bandEdges, startBand, endBand);
      setStagedResidualPanels(function (current) {
        return Object.assign({}, current, {
          [key]: optimisticPanelUpdate(currentPanel, nextRanges, bandEdges),
        });
      });
      setStagedResidualMasks(function (current) {
        return Object.assign({}, current, {
          [targetMaskKey(rowIdx, panelIdx)]: {
            antenna: panelIdx,
            polarization: rowIdx,
            kept_ranges: nextRanges,
          },
        });
      });
      setInteractionMessage("Residual mask staged. Click Apply In-Band to apply.");
      return true;
    }

    function stageResidualMaskClear(rowIdx, panelIdx) {
      if (!overviewData) {
        return false;
      }
      const section = overviewData.inband_residual_phase_band;
      const bandEdges = section && section.band_edges ? section.band_edges : [];
      const row = section && section.panels && section.panels[rowIdx] ? section.panels[rowIdx] : [];
      const originalPanel = row[panelIdx];
      if (!originalPanel || !bandEdges.length) {
        return false;
      }
      const nextRanges =
        originalPanel.auto_kept_ranges && originalPanel.auto_kept_ranges.length
          ? originalPanel.auto_kept_ranges
          : defaultKeptRanges(bandEdges);
      const key = panelSelectionKey("inband_residual_phase_band", rowIdx, panelIdx);
      setStagedResidualPanels(function (current) {
        return Object.assign({}, current, {
          [key]: optimisticPanelUpdate(originalPanel, nextRanges, bandEdges),
        });
      });
      setStagedResidualMasks(function (current) {
        return Object.assign({}, current, {
          [targetMaskKey(rowIdx, panelIdx)]: {
            antenna: panelIdx,
            polarization: rowIdx,
            kept_ranges: nextRanges,
          },
        });
      });
      setInteractionMessage("Residual mask reset to the auto mask. Click Apply In-Band to apply.");
      return true;
    }

    function panelWithStagedResidualSelection(rowIdx, panelIdx, panel) {
      const override = stagedResidualPanels[panelSelectionKey("inband_residual_phase_band", rowIdx, panelIdx)];
      return override ? Object.assign({}, panel, override) : panel;
    }

    function panelWithStagedInbandSelection(sectionId, rowIdx, panelIdx, panel) {
      if (isPhacalScan && sectionId === "inband_fit" && rowIdx === 2) {
        const section = overviewData && overviewData[sectionId] ? overviewData[sectionId] : null;
        const bandEdges = section && section.band_edges ? section.band_edges : [];
        const xKey = panelSelectionKey(sectionId, 0, panelIdx);
        const yKey = panelSelectionKey(sectionId, 1, panelIdx);
        const xRow = section && section.panels && section.panels[0] ? section.panels[0] : [];
        const yRow = section && section.panels && section.panels[1] ? section.panels[1] : [];
        const xPanel = stagedInbandPanels[xKey]
          ? Object.assign({}, xRow[panelIdx] || {}, stagedInbandPanels[xKey])
          : xRow[panelIdx];
        const yPanel = stagedInbandPanels[yKey]
          ? Object.assign({}, yRow[panelIdx] || {}, stagedInbandPanels[yKey])
          : yRow[panelIdx];
        if (xPanel && yPanel && bandEdges.length) {
          return Object.assign({}, panel, {
            kept_ranges: intersectKeptRanges(xPanel.kept_ranges, yPanel.kept_ranges, bandEdges),
          });
        }
      }
      const override = stagedInbandPanels[panelSelectionKey(sectionId, rowIdx, panelIdx)];
      return override ? Object.assign({}, panel, override) : panel;
    }

    function stageInbandWindow(startBand, endBand, sourcePol, sourceAntenna, mode) {
      if (busy) {
        return;
      }
      const nextMode = isPhacalScan ? "exclude" : mode;
      stageInbandSelection(startBand, endBand, sourcePol, sourceAntenna, nextMode);
    }

    function applyStagedInbandWindow() {
      const targets = Object.values(stagedInbandMasks);
      if (busy || !targets.length) {
        return;
      }
      setInteractionMessage("");
      runAction(
        function () {
          return jsonFetch("/api/inband/mask/batch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              targets: targets,
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            clearStagedInbandSelection();
            return next;
          });
        },
        { progressKind: "inband_mask", successMessage: "Mask applied" }
      );
    }

    function previewStagedInbandSection(sectionId) {
      const isRelPhase = sectionId === "inband_relative_phase";
      const targets = Object.values(isRelPhase ? stagedRelPhaseMasks : stagedInbandMasks);
      if (busy || !sessionId || !targets.length) {
        return;
      }
      setInteractionMessage("");
      const successMessage = isRelPhase ? "Rel. Phase preview refreshed" : "Inband Fit preview refreshed";
      runAction(
        function () {
          return previewOverviewSection("/api/inband/mask/preview", {
            session_id: sessionId,
            section_id: sectionId,
            targets: targets,
          }).then(function (next) {
            if (isRelPhase) {
              setStagedRelPhasePanels({});
            } else {
              setStagedInbandPanels({});
            }
            setInteractionMessage("Preview refreshed.");
            return next;
          });
        },
        { progressKind: "relative_delay", successMessage: successMessage }
      );
    }

    function applyDelayEditorUpdate() {
      const antennaIndex = Math.max(0, parseInt(delayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/inband/update",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              x_delay_ns: delayDraft.x === "" ? null : parseFloat(delayDraft.x),
              y_delay_ns: delayDraft.y === "" ? null : parseFloat(delayDraft.y),
            },
            antennaIndex
          );
        },
        { progressKind: "active_delay", successMessage: "In-band delay updated" }
      );
    }

    function resetDelayEditorAntenna() {
      const antennaIndex = Math.max(0, parseInt(delayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/inband/reset",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "active_delay", successMessage: "In-band delay reset" }
      );
    }

    function resetAllDelayEditor() {
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/inband/reset",
            {
              session_id: sessionId,
              antenna: null,
            },
            null
          );
        },
        { progressKind: "active_delay", successMessage: "All in-band delays reset" }
      );
    }

    function applyRelativeDelayEditorUpdate() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      const isPhacal = state && state.current_scan && state.current_scan.kind === "phacal";
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            isPhacal ? "/api/phacal/solve/update" : "/api/relative-delay/update",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              x_delay_ns: relativeDelayDraft.x === "" ? null : parseFloat(relativeDelayDraft.x),
              y_delay_ns: relativeDelayDraft.y === "" ? null : parseFloat(relativeDelayDraft.y),
              x_offset_rad: isPhacal && relativeDelayDraft.xoff !== "" ? parseFloat(relativeDelayDraft.xoff) : null,
              y_offset_rad: isPhacal && relativeDelayDraft.yoff !== "" ? parseFloat(relativeDelayDraft.yoff) : null,
              ant1_manual_dxy_corr_rad:
                !isPhacal && antennaIndex === 0 && ant1DxyDraftValue !== null ? Number(ant1DxyDraftValue) : null,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: isPhacal ? "Phasecal solve updated" : "Relative-phase fit updated" }
      );
    }

    function resetRelativeDelayEditorAntenna() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      const isPhacal = state && state.current_scan && state.current_scan.kind === "phacal";
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            isPhacal ? "/api/phacal/solve/reset" : "/api/relative-delay/reset",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: isPhacal ? "Phasecal solve reset" : "Relative-phase fit reset" }
      );
    }

    function applyPhacalSlopeSeed(antennaIndex, seedDelayNs, seedOffsetRad) {
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/phacal/solve/seed-from-slope",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              seed_delay_ns: seedDelayNs,
              seed_offset_rad: seedOffsetRad,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Phasecal seeded from slope" }
      );
    }

    function handlePhacalAnchorSlopeGesture(rowIdx, panelIdx, point1, point2, _panel) {
      // Gated to Anchor-Ref. Phase XX/YY rows only. Row 2 is the Y-X display
      // and is not a valid seed target for a multiband delay fit.
      if (Number(rowIdx) !== 0 && Number(rowIdx) !== 1) {
        return;
      }
      const plotData = overviewData ? overviewData.inband_fit : null;
      const controls = (plotData && plotData.column_controls) || [];
      const control = controls[Number(panelIdx)];
      if (!control) {
        return;
      }
      const antenna = Number(control.antenna);
      const fa = Number(point1.x);
      const fb = Number(point2.x);
      if (!Number.isFinite(fa) || !Number.isFinite(fb) || Math.abs(fb - fa) < 1e-6) {
        return;
      }
      // Use the raw phase delta: the user is clicking two points on a
      // visually-continuous line segment (no 2π jump between them), so
      // y2 − y1 in the wrapped plot coords already equals the true phase
      // change along the line. Wrapping into (−π, π] here would collapse
      // steep slopes (|dphi| > π), which shows up on fallback/self-ref
      // antennas whose base vis retains a large residual delay.
      const dphi = Number(point2.y) - Number(point1.y);
      const seedDelayNs = dphi / (2.0 * Math.PI * (fb - fa));
      const offsetRaw = Number(point1.y) - 2.0 * Math.PI * fa * seedDelayNs;
      const seedOffsetRad = Math.atan2(Math.sin(offsetRaw), Math.cos(offsetRaw));
      if (!Number.isFinite(seedDelayNs) || !Number.isFinite(seedOffsetRad)) {
        return;
      }
      applyPhacalSlopeSeed(antenna, seedDelayNs, seedOffsetRad);
    }

    function applyRelativeDelaySuggestion() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      const isPhacal = state && state.current_scan && state.current_scan.kind === "phacal";
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            isPhacal ? "/api/phacal/solve/apply-suggestion" : "/api/relative-delay/apply-suggestion",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: isPhacal ? "Phasecal suggestion applied" : "Relative-phase suggestion applied" }
      );
    }

    function applyResidualPatch(color) {
      const antennaIndex = Math.max(0, parseInt(delayDraft.ant || "1", 10) - 1);
      const activeRef = state && state.active_refcal ? state.active_refcal : null;
      if (!activeRef) {
        return;
      }
      if (color === "blue") {
        const xPatch = parseOptionalFloat(residualPatchDraft.xBlue);
        const yPatch = parseOptionalFloat(residualPatchDraft.yBlue);
        const xBase = Number(activeRef.x_effective_inband_delay_ns);
        const yBase = Number(activeRef.y_effective_inband_delay_ns);
        runAction(
          function () {
            return postJsonWithOverviewPatch(
              "/api/inband/update",
              {
                session_id: sessionId,
                antenna: antennaIndex,
                x_delay_ns: Number.isFinite(xPatch) && Number.isFinite(xBase) ? xBase + xPatch : null,
                y_delay_ns: Number.isFinite(yPatch) && Number.isFinite(yBase) ? yBase + yPatch : null,
              },
              antennaIndex
            );
          },
          { progressKind: "active_delay", successMessage: "In-band residual patch applied" }
        );
        return;
      }
      const xPatch = parseOptionalFloat(residualPatchDraft.xRed);
      const yPatch = parseOptionalFloat(residualPatchDraft.yRed);
      const xBase = Number(activeRef.x_applied_relative_delay_ns);
      const yBase = Number(activeRef.y_applied_relative_delay_ns);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/relative-delay/update",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              x_delay_ns: Number.isFinite(xPatch) && Number.isFinite(xBase) ? xBase + xPatch : null,
              y_delay_ns: Number.isFinite(yPatch) && Number.isFinite(yBase) ? yBase + yPatch : null,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Multiband residual patch applied" }
      );
    }

    function resetResidualPatchDrafts() {
      const activeRef = state && state.active_refcal ? state.active_refcal : null;
      const ant = state ? state.selected_ant + 1 : residualPatchDraft.ant;
      setResidualPatchDraft({
        ant: ant,
        xBlue:
          activeRef && activeRef.x_residual_inband_suggest_ns !== null && activeRef.x_residual_inband_suggest_ns !== undefined
            ? String(Number(activeRef.x_residual_inband_suggest_ns).toFixed(3))
            : "",
        yBlue:
          activeRef && activeRef.y_residual_inband_suggest_ns !== null && activeRef.y_residual_inband_suggest_ns !== undefined
            ? String(Number(activeRef.y_residual_inband_suggest_ns).toFixed(3))
            : "",
        xRed:
          activeRef && activeRef.x_suggested_relative_delay_ns !== null && activeRef.x_suggested_relative_delay_ns !== undefined
            ? String(Number(activeRef.x_suggested_relative_delay_ns).toFixed(3))
            : "",
        yRed:
          activeRef && activeRef.y_suggested_relative_delay_ns !== null && activeRef.y_suggested_relative_delay_ns !== undefined
            ? String(Number(activeRef.y_suggested_relative_delay_ns).toFixed(3))
            : "",
      });
    }

    function setSecondaryAnchor(scanId) {
      runAction(function () {
        return postJson("/api/refcal/secondary", {
          session_id: sessionId,
          scan_id: scanId,
        });
      });
    }

    function clearSecondaryAnchor() {
      runAction(function () {
        return postJson("/api/refcal/secondary", {
          session_id: sessionId,
          scan_id: null,
        });
      });
    }

    function compareSelectedAnchors() {
      if (!sessionId || checkedScanIds.length !== 2) {
        return;
      }
      runAction(function () {
        return postJson("/api/refcal/compare", {
          session_id: sessionId,
          scan_ids: checkedScanIds,
        });
      });
    }

    function clearAnchorCompare() {
      runAction(function () {
        return postJson("/api/refcal/compare/clear", {
          session_id: sessionId,
        });
      });
    }

    function toggleDonorPatchCandidate(antennaIndex, selected) {
      runAction(function () {
        return postJson("/api/refcal/donor-patch/candidate", {
          session_id: sessionId,
          antenna: antennaIndex,
          selected: selected,
        });
      });
    }

    function applyDonorPatchSelection() {
      runAction(function () {
        return postJson("/api/refcal/donor-patch/apply", {
          session_id: sessionId,
        });
      });
    }

    function togglePhacalAnchorFallback(enabled) {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/phacal/anchor-fallback",
            {
              session_id: sessionId,
              antenna: antennaIndex,
              enabled: enabled,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: enabled ? "Temporary fallback enabled" : "Temporary fallback cleared" }
      );
    }

    function promotePhacalAntennaToRefcal() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/refcal/promote-from-phacal",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Promoted antenna to refcal" }
      );
    }

    function applyResidualInbandFit(antennaOverride) {
      const antennaIndex = (antennaOverride === undefined || antennaOverride === null)
        ? null
        : Math.max(0, Number(antennaOverride));
      const body = {
        session_id: sessionId,
        targets: Object.values(stagedResidualMasks),
      };
      if (antennaIndex !== null) {
        body.antenna = antennaIndex;
      }
      const message = antennaIndex !== null
        ? "Residual in-band fit applied to Ant " + (antennaIndex + 1)
        : "Residual in-band fit applied";
      runAction(
        function () {
          return jsonFetch("/api/inband/apply-residual-fit", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, antennaIndex);
              });
            }
            if (antennaIndex === null) {
              clearStagedResidualSelection();
            }
            return next;
          });
        },
        { progressKind: "active_delay", successMessage: message }
      );
    }

    function applyResidualMultibandFit(antennaOverride) {
      const requestedAnt = antennaOverride === undefined || antennaOverride === null
        ? parseInt(relativeDelayDraft.ant || "1", 10) - 1
        : Number(antennaOverride);
      const antennaIndex = Math.max(0, Number.isFinite(requestedAnt) ? requestedAnt : 0);
      setRelativeDelayDraft(function (current) {
        return Object.assign({}, current || {}, { ant: String(antennaIndex + 1) });
      });
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/residual-panel/apply-multiband-fit",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: "Multiband fit applied" }
      );
    }

    function undoResidualPanelAction() {
      runAction(
        function () {
          return jsonFetch("/api/residual-panel/undo", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            return next;
          });
        },
        { progressKind: "relative_delay", successMessage: "Residual-panel action undone" }
      );
    }

    function previewStagedResidualSection() {
      const targets = Object.values(stagedResidualMasks);
      if (busy || !sessionId || !targets.length) {
        return;
      }
      setInteractionMessage("");
      runAction(function () {
        return previewOverviewSection("/api/inband/residual-mask/preview", {
          session_id: sessionId,
          section_id: "inband_residual_phase_band",
          targets: targets,
        }).then(function (next) {
          setInteractionMessage("Residual preview refreshed.");
          return next;
        });
      });
    }

    function toggleManualAntennaFlag(antennaIndex, flagged) {
      if (!sessionId) {
        return;
      }
      const isPhacal = state && state.current_scan && state.current_scan.kind === "phacal";
      postJsonWithOverviewPatch(
        isPhacal ? "/api/phacal/antenna-flag" : "/api/relative-phase/antenna-flag",
        {
          session_id: sessionId,
          antenna: antennaIndex,
          flagged: flagged,
        },
        antennaIndex
      )
        .then(function (next) {
          const nextState = next && next.state ? next.state : null;
          setInteractionMessage(
            nextState && nextState.status_message
              ? nextState.status_message
              : flagged
                ? "Antenna excluded"
                : "Antenna restored"
          );
        })
        .catch(function (err) {
          setError(err.message || String(err));
        });
    }

    function setMultibandFitKind(antennaIndex, kind) {
      const ant = Math.max(0, Number(antennaIndex) || 0);
      const allowed = { linear: true, poly2: true, poly3: true };
      const next = allowed[kind] ? kind : "linear";
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            "/api/refcal/multiband-fit-kind",
            {
              session_id: sessionId,
              antenna: ant,
              kind: next,
            },
            ant
          );
        },
        { progressKind: "relative_delay", successMessage: "Multiband fit kind set to " + next + " for Ant " + (ant + 1) }
      );
    }

    function applyYxResidualThreshold() {
      const threshold = parseFloat(yxThresholdDraft);
      if (!Number.isFinite(threshold) || threshold < 0) {
        setError("Y-X residual RMS threshold must be a non-negative number.");
        return;
      }
      runAction(
        function () {
          return postJsonWithOverviewRefresh("/api/relative-phase/yx-threshold", {
            session_id: sessionId,
            value: threshold,
          });
        },
        { progressKind: "relative_delay", successMessage: "Y-X residual RMS threshold updated" }
      );
    }

    function applyResidualBandThreshold() {
      const threshold = parseFloat(residualThresholdDraft);
      if (!Number.isFinite(threshold) || threshold < 0) {
        setError("Residual bad-band threshold must be a non-negative number.");
        return;
      }
      runAction(
        function () {
          return jsonFetch("/api/inband/residual-threshold", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              value: threshold,
            }),
          }).then(function (next) {
            const nextState = next && next.state ? next.state : next;
            setState(nextState);
            setInteractionMessage("");
            syncDraft(nextState);
            if (next && next.overview_updates) {
              setOverviewData(function (current) {
                return mergeOverviewAntennaUpdates(current, next.overview_updates, null);
              });
            }
            clearStagedResidualSelection();
            return next;
          });
        },
        { progressKind: "active_delay", successMessage: "Residual bad-band threshold updated" }
      );
    }

    function undoRelativeDelayEditor() {
      const antennaIndex = Math.max(0, parseInt(relativeDelayDraft.ant || "1", 10) - 1);
      const isPhacal = state && state.current_scan && state.current_scan.kind === "phacal";
      runAction(
        function () {
          return postJsonWithOverviewPatch(
            isPhacal ? "/api/phacal/solve/undo" : "/api/relative-delay/undo",
            {
              session_id: sessionId,
              antenna: antennaIndex,
            },
            antennaIndex
          );
        },
        { progressKind: "relative_delay", successMessage: isPhacal ? "Phasecal edit undone" : "Relative-phase edit undone" }
      );
    }

    const canAnalyze = sessionId && selectedScanId() !== null;
    const selectedScanEntryForGate =
      state && state.scans
        ? state.scans.find(function (scan) {
            return scan.scan_id === selectedScanId();
          }) || null
        : null;
    const selectedIsAnalyzedOrSaved =
      !!selectedScanEntryForGate &&
      (!!selectedScanEntryForGate.analyzed ||
        selectedScanEntryForGate.status === "refcal" ||
        selectedScanEntryForGate.status === "phacal");
    const selectedIsActiveRefcalGate =
      state && state.ref_scan_id !== null && selectedScanId() === state.ref_scan_id;
    const canRefcal = canAnalyze && (selectedIsAnalyzedOrSaved || selectedIsActiveRefcalGate);
    const canPhacal = canAnalyze && state && state.ref_scan_id !== null;
    const checkedScans = state && state.scans
      ? state.scans.filter(function (scan) {
          return checkedScanIds.indexOf(scan.scan_id) >= 0;
        })
      : [];
    const canCompareAnchors =
      checkedScans.length === 2 &&
      checkedScans.every(function (scan) {
        return String(scan.status || "").indexOf("refcal") >= 0 || !!scan.analyzed;
      }) &&
      String(checkedScans[0].feed_kind || "") === String(checkedScans[1].feed_kind || "");
    const canCombine =
      checkedScans.length === 2 &&
      String(checkedScans[0].feed_kind || "") !== String(checkedScans[1].feed_kind || "");
    const selectedAntLabel = state ? state.selected_ant + 1 : "-";
    const selectedBandLabel = state ? state.selected_band + 1 : "-";
    const selectedScanEntry =
      state && state.scans
        ? state.scans.find(function (scan) {
            return scan.scan_id === selectedScanId();
          }) || null
        : null;
    const currentScanLabel =
      state && state.current_scan
        ? state.current_scan.kind + " " + (state.current_scan.scan_time || state.current_scan.timestamp_iso)
        : "No scan selected";
    const activeRefLabel =
      state && state.active_refcal ? state.active_refcal.scan_time || state.active_refcal.timestamp_iso : "None";
    const secondaryRefLabel =
      state && state.secondary_ref_scan_id !== null
        ? (state.scans || []).filter(function (scan) {
            return scan.scan_id === state.secondary_ref_scan_id;
          }).map(function (scan) {
            return scan.scan_time;
          })[0] || "Set"
        : "None";
    const isPhacalScan = !!(state && state.current_scan && state.current_scan.kind === "phacal");
    const activeRelativeRef = isPhacalScan
      ? state && state.active_phacal
        ? state.active_phacal
        : null
      : state && state.active_refcal
        ? state.active_refcal
        : null;
    const hasRelativeSuggestion =
      !!(
        activeRelativeRef &&
        (
          Math.abs(Number(
            isPhacalScan ? activeRelativeRef.x_suggested_delay_ns : activeRelativeRef.x_suggested_relative_delay_ns || 0
          )) > 1e-9 ||
          Math.abs(Number(
            isPhacalScan ? activeRelativeRef.y_suggested_delay_ns : activeRelativeRef.y_suggested_relative_delay_ns || 0
          )) > 1e-9 ||
          Math.abs(Number(isPhacalScan ? activeRelativeRef.x_suggested_offset_rad : 0)) > 1e-9 ||
          Math.abs(Number(isPhacalScan ? activeRelativeRef.y_suggested_offset_rad : 0)) > 1e-9
        )
      );
    const canApplyResidualFit = !!(state && (state.active_refcal || state.active_phacal));
    const canUndoResidualPanel = !!(state && state.residual_panel_undo_available);
    const workflowGuide = workflowGuideState({
      state: state,
      checkedScans: checkedScans,
      compareData: compareData,
      isPhacalScan: isPhacalScan,
    });
    function residualColumnActionRenderer(control, sectionId) {
      if (!overviewData || !overviewData.inband_residual_phase_band) {
        return null;
      }
      const antennaIndex = Number(control.antenna);
      const hasData =
        columnHasAnyData(overviewData.inband_residual_phase_band, antennaIndex) ||
        columnHasAnyData(overviewData.inband_relative_phase, antennaIndex);
      const hasSuggestion = columnHasUsableMultibandSuggestion(overviewData.inband_relative_phase, antennaIndex, isPhacalScan);
      const disabled = !!(
        busy ||
        control.flagged ||
        control.auto_flagged ||
        !hasData ||
        !hasSuggestion
      );
      const fitKinds = (overviewData.inband_residual_phase_band && overviewData.inband_residual_phase_band.multiband_fit_kinds) || [];
      const currentKind = String(fitKinds[antennaIndex] || "linear");
      const fitKindDisabled = !!(busy || isPhacalScan || control.flagged || control.auto_flagged || !hasData || antennaIndex === 0);
      // Per-(section, scan-kind) control gating:
      //   Refcal Per-Band Res. Phase    -> fit-kind + I + M
      //     (residual panel is where the user fits the multiband through
      //      residuals; M applies the suggested multiband to the model.)
      //   Phacal Per-Band Res. Phase    -> I only
      //     (phacal does not fit a multiband through residuals; M and the
      //      fit-kind dropdown are meaningless here.)
      //   Phacal Anchor-Ref. Phase      -> M only
      //     (this section plots the multiband model line; M moves it. The
      //      fit-kind dropdown is inert for phacal so it's hidden too.
      //      I has no per-band visualization here, so it's hidden.)
      //   Refcal Anchor-Ref. Phase      -> n/a (dispatch never wires it).
      const isRefcalResidual = sectionId === "inband_residual_phase_band" && !isPhacalScan;
      const isPhacalResidual = sectionId === "inband_residual_phase_band" && isPhacalScan;
      const isPhacalAnchor = sectionId === "inband_fit" && isPhacalScan;
      const showFitKind = isRefcalResidual;
      const showI = isRefcalResidual || isPhacalResidual;
      const showM = isRefcalResidual || isPhacalAnchor;
      return html`
        ${showFitKind
          ? html`
              <select
                className="panel-grid-column-fit-kind"
                value=${currentKind}
                disabled=${fitKindDisabled}
                title=${antennaIndex === 0
                  ? "Ant 1 uses the dedicated multiband-shape fit; cannot change kind here."
                  : "Multiband fit model for " + control.label}
                onPointerDown=${function (event) {
                  event.stopPropagation();
                }}
                onChange=${function (event) {
                  const next = String(event.target.value);
                  if (next === currentKind) {
                    return;
                  }
                  setMultibandFitKind(antennaIndex, next);
                }}
              >
                <option value="linear">Linear</option>
                <option value="poly2">Poly2</option>
                <option value="poly3">Poly3</option>
              </select>
            `
          : null}
        ${showI
          ? html`
              <button
                type="button"
                className="panel-grid-column-action panel-grid-column-action-blue"
                title=${"Apply per-band in-band residual correction to " + control.label + " (flattens within-band residual slopes; preserves multiband)"}
                aria-label=${"Apply per-band in-band residual correction to " + control.label}
                disabled=${!!(busy || control.flagged || control.auto_flagged || !hasData)}
                onPointerDown=${function (event) {
                  event.preventDefault();
                  event.stopPropagation();
                }}
                onClick=${function (event) {
                  event.preventDefault();
                  event.stopPropagation();
                  if (!(busy || control.flagged || control.auto_flagged || !hasData)) {
                    applyResidualInbandFit(antennaIndex);
                  }
                }}
              >
                <span className="panel-grid-column-action-glyph" aria-hidden="true">I</span>
              </button>
            `
          : null}
        ${showM
          ? html`
              <button
                type="button"
                className="panel-grid-column-action panel-grid-column-action-red"
                title=${"Apply auto-suggested multiband delay to " + control.label + " (adds suggested → applied for this antenna)"}
                aria-label=${"Apply auto-suggested multiband delay to " + control.label}
                disabled=${disabled}
                onPointerDown=${function (event) {
                  event.preventDefault();
                  event.stopPropagation();
                }}
                onClick=${function (event) {
                  event.preventDefault();
                  event.stopPropagation();
                  if (!disabled) {
                    applyResidualMultibandFit(antennaIndex);
                  }
                }}
              >
                <span className="panel-grid-column-action-glyph" aria-hidden="true">M</span>
              </button>
            `
          : null}
      `;
    }
    function canEditKeptMaskSection(sectionId) {
      return isPhacalScan ? sectionId === "inband_fit" : sectionId === "inband_fit" || sectionId === "inband_relative_phase";
    }
    function canEditResidualMaskSection(sectionId) {
      return sectionId === "inband_residual_phase_band";
    }
    const selectedCellFlagSum =
      heatmapData &&
      !heatmapData.message &&
      state &&
      heatmapData.values &&
      heatmapData.values[state.selected_band] &&
      Number.isFinite(heatmapData.values[state.selected_band][state.selected_ant])
        ? formatNumber(Number(heatmapData.values[state.selected_band][state.selected_ant]))
        : "—";
    const scanCount = state && state.scans ? state.scans.length : 0;
    const scanlistMaxHeightPx = Math.min(320, Math.max(80, 28 + scanCount * 29));
    const metadataWarnings = state && state.scan_metadata_warnings ? state.scan_metadata_warnings : [];
    const selectedPendingTimeIntervals =
      state && timeHistoryData && !timeHistoryData.message
        ? stagedTimeIntervals
            .filter(function (item) {
              return Number(item.antenna) === Number(state.selected_ant) && Number(item.band) === Number(state.selected_band);
            })
            .sort(function (a, b) {
              return Number(a.start_jd) - Number(b.start_jd) || Number(a.end_jd) - Number(b.end_jd);
            })
            .map(function (item) {
              return {
                group_id: String(item.temp_id),
                scope: String(item.scope),
                scope_label: timeFlagScopeLabel(item.scope),
                source: "staged",
                pending: true,
                start_jd: Number(item.start_jd),
                end_jd: Number(item.end_jd),
                start_offset_min: (Number(item.start_jd) - Number(timeHistoryData.start_jd || 0)) * 24.0 * 60.0,
                end_offset_min: (Number(item.end_jd) - Number(timeHistoryData.start_jd || 0)) * 24.0 * 60.0,
                start_label: formatUtcTime(Number(item.start_jd)),
                end_label: formatUtcTime(Number(item.end_jd)),
              };
            })
        : [];
    const pendingFlagMapCells =
      state && state.current_layout
        ? Array.from(
            stagedTimeIntervals.reduce(function (acc, interval) {
              expandTimeFlagTargets(interval, state.current_layout).forEach(function (target) {
                acc.set(String(target.antenna) + ":" + String(target.band), target);
              });
              return acc;
            }, new Map()).values()
          )
        : [];
    const activeRefMeta = state && state.active_refcal ? state.active_refcal : null;
    const activePhacalMeta = state && state.active_phacal ? state.active_phacal : null;
    const selectedAntennaText = "Ant " + selectedAntLabel;
    const controlRailWidth = controlRailCollapsed ? 28 : clamp(controlRailWidthPx, 260, 520);
    const bluePatchDraftReady =
      parseOptionalFloat(residualPatchDraft.xBlue) !== null || parseOptionalFloat(residualPatchDraft.yBlue) !== null;
    const redPatchDraftReady =
      parseOptionalFloat(residualPatchDraft.xRed) !== null || parseOptionalFloat(residualPatchDraft.yRed) !== null;
    const ant1CommittedDxy =
      activeRefMeta && activeRefMeta.ant1_manual_dxy_corr_rad !== null && activeRefMeta.ant1_manual_dxy_corr_rad !== undefined
        ? Number(activeRefMeta.ant1_manual_dxy_corr_rad)
        : null;
    const ant1AutoDxy =
      activeRefMeta && activeRefMeta.ant1_auto_dxy_rad !== null && activeRefMeta.ant1_auto_dxy_rad !== undefined
        ? Number(activeRefMeta.ant1_auto_dxy_rad)
        : null;
    const isRefcalScan = !!(state && state.current_scan && state.current_scan.kind === "refcal");
    const ant1DxyVisible = !!(isRefcalScan && activeRefMeta && activeRefMeta.is_reference_antenna);
    const ant1DxyDraftValue =
      ant1CommittedDxy !== null
        ? (parseOptionalFloat(ant1DxyDraft) !== null ? parseOptionalFloat(ant1DxyDraft) : ant1CommittedDxy)
        : null;
    const ant1EffectiveDxy =
      ant1AutoDxy !== null && ant1DxyDraftValue !== null
        ? ant1AutoDxy + ant1DxyDraftValue
        : activeRefMeta && activeRefMeta.ant1_effective_dxy_rad !== null && activeRefMeta.ant1_effective_dxy_rad !== undefined
          ? Number(activeRefMeta.ant1_effective_dxy_rad)
          : null;
    const ant1DxyDirty = !!(
      ant1CommittedDxy !== null &&
      ant1DxyDraftValue !== null &&
      Math.abs(Number(ant1DxyDraftValue) - Number(ant1CommittedDxy)) > 1e-9
    );

    function resetAnt1DxyDraft() {
      if (ant1CommittedDxy === null) {
        return;
      }
      setAnt1DxyDraft(formatEditorValue(ant1CommittedDxy));
    }

    useEffect(
      function () {
        if (!sessionId || !activeRefMeta || ant1CommittedDxy === null || busy) {
          return;
        }
        const shouldPreview = !!(ant1DxyVisible && ant1DxyDraftValue !== null && ant1DxyDirty);
        if (!shouldPreview && !ant1DxyPreviewActiveRef.current) {
          return;
        }
        const target = shouldPreview ? ant1DxyDraftValue : ant1CommittedDxy;
        if (target === null) {
          return;
        }
        let cancelled = false;
        const requestSeq = ant1DxyPreviewSeqRef.current + 1;
        ant1DxyPreviewSeqRef.current = requestSeq;
        const timer = window.setTimeout(function () {
          jsonFetch("/api/relative-delay/ant1-dxy/preview", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              session_id: sessionId,
              manual_dxy_corr_rad: Number(target),
            }),
          })
            .then(function (next) {
              if (cancelled || ant1DxyPreviewSeqRef.current !== requestSeq) {
                return;
              }
              if (next && next.overview_updates) {
                setOverviewData(function (current) {
                  return mergeOverviewAntennaUpdates(current, next.overview_updates, 0);
                });
              }
              ant1DxyPreviewActiveRef.current = shouldPreview;
            })
            .catch(function (err) {
              if (cancelled || ant1DxyPreviewSeqRef.current !== requestSeq) {
                return;
              }
              setError(err.message || String(err));
            });
        }, 180);
        return function () {
          cancelled = true;
          window.clearTimeout(timer);
        };
      },
      [
        sessionId,
        busy,
        ant1DxyVisible,
        ant1DxyDirty,
        ant1CommittedDxy,
        ant1DxyDraft,
      ]
    );

    function renderDelaySectionHeader(title, source, toneClass) {
      return html`
        <div className="delay-control-section-header">
          <h3>${title}</h3>
          <span className=${"delay-control-source " + toneClass}>${source}</span>
        </div>
      `;
    }

    function renderRefcalDelayControl() {
      if (!activeRefMeta) {
        return html`
          <div className="delay-empty">
            <div className="delay-summary">
              <span>Refcal: None · ${selectedAntennaText}</span>
            </div>
            <div className="tiny">Set a refcal to edit selected-antenna in-band and multiband delays.</div>
          </div>
        `;
      }
      return html`
        <div className="delay-summary">
          <span>${"Refcal: " + activeRefLabel + " · " + selectedAntennaText}</span>
        </div>
        <div className="delay-control-legend">
          <span className="delay-control-legend-item blue">Blue = In-Band Delay</span>
          <span className="delay-control-legend-item red">Red = Multiband Delay</span>
          <span className="delay-control-legend-item neutral">Residual Patch feeds blue/red</span>
        </div>
        <div className="delay-control-note">
          In-band changes affect corrected data and downstream multiband solve. Multiband changes affect the red fit directly.
        </div>

        <div className="delay-control-section">
          ${renderDelaySectionHeader("In-Band Delay", "From In-Band Fit Diag.", "blue")}
          <div className="delay-grid delay-pair-grid">
            <div className="delay-pair-row">
              <label>
                X Auto In-Band (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.x_auto_inband_delay_ns)} />
              </label>
              <label>
                Y Auto In-Band (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.y_auto_inband_delay_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Res. In-Band Sug. (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.x_residual_inband_suggest_ns)} />
              </label>
              <label>
                Y Res. In-Band Sug. (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.y_residual_inband_suggest_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Manual Eff. In-Band (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${delayDraft.x}
                  onChange=${function (event) {
                    setDelayDraft(Object.assign({}, delayDraft, { x: event.target.value }));
                  }}
                />
              </label>
              <label>
                Y Manual Eff. In-Band (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${delayDraft.y}
                  onChange=${function (event) {
                    setDelayDraft(Object.assign({}, delayDraft, { y: event.target.value }));
                  }}
                />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Override Δ (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.x_inband_override_delta_ns)} />
              </label>
              <label>
                Y Override Δ (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.y_inband_override_delta_ns)} />
              </label>
            </div>
            <div className="delay-actions full">
              <button type="button" className="btn-outline-fit-blue" disabled=${busy} title="Apply the in-band delay/offset values typed above to the selected antenna" onClick=${applyDelayEditorUpdate}>Apply In-Band</button>
              <button type="button" disabled=${busy} title="Discard manual edits for the selected antenna and restore its committed values" onClick=${resetDelayEditorAntenna}>Reset Ant</button>
              <button type="button" disabled=${busy} title="Discard manual edits for ALL antennas and restore their committed values" onClick=${resetAllDelayEditor}>Reset All</button>
            </div>
          </div>
        </div>

        <div className="delay-control-section">
          ${renderDelaySectionHeader("Multiband Delay", "From Rel. Phase + Fit", "red")}
          <div className="delay-control-note">
            Ant 1 uses reference-shape tuning plus a manual Δ(Y-X) trim; other antennas use scalar multiband delay only.
          </div>
          <div className="delay-grid delay-pair-grid">
            <div className="delay-pair-row">
              <label>
                X Auto Multiband (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.x_auto_relative_delay_ns)} />
              </label>
              <label>
                Y Auto Multiband (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.y_auto_relative_delay_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Res. Multiband Sug. (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.x_suggested_relative_delay_ns)} />
              </label>
              <label>
                Y Res. Multiband Sug. (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.y_suggested_relative_delay_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Manual Multiband Corr. (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${relativeDelayDraft.x}
                  onChange=${function (event) {
                    setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { x: event.target.value }));
                  }}
                />
              </label>
              <label>
                Y Manual Multiband Corr. (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${relativeDelayDraft.y}
                  onChange=${function (event) {
                    setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { y: event.target.value }));
                  }}
                />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Eff. Multiband Fit (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.x_effective_relative_delay_ns)} />
              </label>
              <label>
                Y Eff. Multiband Fit (ns)
                <input type="text" disabled value=${formatEditorValue(activeRefMeta.y_effective_relative_delay_ns)} />
              </label>
            </div>
            ${ant1DxyVisible
              ? html`
                  <div className="delay-pair-row">
                    <label>
                      Auto Δ(Y-X) [rad]
                      <input type="text" disabled value=${formatEditorValue(ant1AutoDxy)} />
                    </label>
                    <label>
                      Manual Δ(Y-X) Corr. [rad]
                      <input
                        type="number"
                        step="0.1"
                        value=${ant1DxyDraft}
                        onChange=${function (event) {
                          setAnt1DxyDraft(event.target.value);
                        }}
                      />
                    </label>
                  </div>
                  <div className="delay-pair-row">
                    <label>
                      Effective Δ(Y-X) [rad]
                      <input type="text" disabled value=${formatEditorValue(ant1EffectiveDxy)} />
                    </label>
                    <div></div>
                  </div>
                  <div className="delay-actions full">
                    <button type="button" disabled=${busy || !ant1DxyDirty} onClick=${resetAnt1DxyDraft}>Reset Δ(Y-X)</button>
                  </div>
                `
              : null}
            <div className="delay-actions full">
              <button type="button" className=${"btn-outline-red" + (ant1DxyDirty ? " btn-pending-pulse" : "")} disabled=${busy} title=${ant1DxyDirty ? "Pending Manual Δ(Y-X) change — click to commit" : "Apply the manual multiband delay/offset values typed above to the selected antenna"} onClick=${applyRelativeDelayEditorUpdate}>Apply Multiband</button>
              <button type="button" className="btn-outline-red" disabled=${busy || !hasRelativeSuggestion} title="Adopt the auto-suggested multiband delay/offset (suggested → applied) for the selected antenna" onClick=${applyRelativeDelaySuggestion}>Apply Suggestion</button>
              <button
                type="button"
                disabled=${busy || !(activeRefMeta && activeRefMeta.relative_undo_available)}
                title="Undo the most recent multiband edit on this antenna"
                onClick=${undoRelativeDelayEditor}
              >
                Undo
              </button>
              <button type="button" disabled=${busy} title="Reset the multiband delay/offset edits on the selected antenna" onClick=${resetRelativeDelayEditorAntenna}>Reset</button>
            </div>
          </div>
        </div>

        <div className="delay-control-section">
          ${renderDelaySectionHeader("Residual Patch", "From Per-Band Res. Phase", "mixed")}
          <div className="delay-grid delay-pair-grid">
            <div className="delay-pair-row">
              <label>
                X Blue Res. Patch (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${residualPatchDraft.xBlue}
                  onChange=${function (event) {
                    setResidualPatchDraft(Object.assign({}, residualPatchDraft, { xBlue: event.target.value }));
                  }}
                />
              </label>
              <label>
                Y Blue Res. Patch (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${residualPatchDraft.yBlue}
                  onChange=${function (event) {
                    setResidualPatchDraft(Object.assign({}, residualPatchDraft, { yBlue: event.target.value }));
                  }}
                />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Red Res. Patch (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${residualPatchDraft.xRed}
                  onChange=${function (event) {
                    setResidualPatchDraft(Object.assign({}, residualPatchDraft, { xRed: event.target.value }));
                  }}
                />
              </label>
              <label>
                Y Red Res. Patch (ns)
                <input
                  type="number"
                  step="0.1"
                  value=${residualPatchDraft.yRed}
                  onChange=${function (event) {
                    setResidualPatchDraft(Object.assign({}, residualPatchDraft, { yRed: event.target.value }));
                  }}
                />
              </label>
            </div>
            <div className="delay-actions full">
              <button type="button" className="btn-outline-fit-blue" disabled=${busy || !bluePatchDraftReady} onClick=${function () { applyResidualPatch("blue"); }}>Apply Blue Patch</button>
              <button type="button" className="btn-outline-red" disabled=${busy || !redPatchDraftReady} onClick=${function () { applyResidualPatch("red"); }}>Apply Red Patch</button>
              <button type="button" disabled=${busy} onClick=${resetResidualPatchDrafts}>Reset Patch Drafts</button>
            </div>
            <div className="full tiny">
              Blue patch writes into the effective in-band path. Red patch writes into the multiband correction path.
            </div>
          </div>
        </div>
      `;
    }

    function renderPhacalSolveControl() {
      if (!activePhacalMeta) {
        return html`
          <div className="delay-empty">
            <div className="delay-summary">
              <span>Phacal: None · ${selectedAntennaText}</span>
            </div>
            <div className="tiny">Analyze a phacal against an anchor refcal to edit the selected-antenna solve.</div>
          </div>
        `;
      }
      return html`
        <div className="delay-summary">
          <span>${"Phacal: " + currentScanLabel + " · " + selectedAntennaText}</span>
        </div>
        <div className="delay-control-section">
          ${renderDelaySectionHeader("Anchor Summary", "Read-only anchor context", "neutral")}
          <div className="summary-grid">
            <div>
              <strong>Canonical</strong>
              <span>${activePhacalMeta.anchor_scan_time || activeRefLabel}</span>
            </div>
            <div>
              <strong>Secondary</strong>
              <span>${activePhacalMeta.secondary_anchor_scan_time || "None"}</span>
            </div>
            <div>
              <strong>Fallback</strong>
              <span>${activePhacalMeta.fallback_in_use ? "Active" : "Off"}</span>
            </div>
            <div>
              <strong>Donor/Patch</strong>
              <span>${activePhacalMeta.donor_patch_used ? "Active" : "None"}</span>
            </div>
          </div>
        </div>

        <div className="delay-control-section">
          ${renderDelaySectionHeader("Primary Solve", "Selected-antenna phacal solve", "red")}
          <div className="delay-grid delay-pair-grid">
            <div className="delay-pair-row">
              <label>
                X Auto Delay (ns)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.x_auto_delay_ns)} />
              </label>
              <label>
                Y Auto Delay (ns)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.y_auto_delay_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Auto Offset (rad)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.x_auto_offset_rad)} />
              </label>
              <label>
                Y Auto Offset (rad)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.y_auto_offset_rad)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Sug. Delay (ns)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.x_suggested_delay_ns)} />
              </label>
              <label>
                Y Sug. Delay (ns)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.y_suggested_delay_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Sug. Offset (rad)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.x_suggested_offset_rad)} />
              </label>
              <label>
                Y Sug. Offset (rad)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.y_suggested_offset_rad)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Eff. Saved (ns)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.x_effective_delay_ns)} />
              </label>
              <label>
                Y Eff. Saved (ns)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.y_effective_delay_ns)} />
              </label>
            </div>
            <div className="delay-pair-row">
              <label>
                X Eff. Offset (rad)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.x_effective_offset_rad)} />
              </label>
              <label>
                Y Eff. Offset (rad)
                <input type="text" disabled value=${formatEditorValue(activePhacalMeta.y_effective_offset_rad)} />
              </label>
            </div>
            <div className="delay-actions full">
              <button type="button" className="btn-outline-red" disabled=${busy || !phacalEditorAdvanced} title="Apply the manual delay/offset values typed in Manual Tuning to the selected antenna" onClick=${applyRelativeDelayEditorUpdate}>Apply</button>
              <button type="button" className="btn-outline-red" disabled=${busy || !hasRelativeSuggestion} title="Adopt the auto-suggested delay/offset (suggested → applied) for the selected antenna" onClick=${applyRelativeDelaySuggestion}>Apply Suggestion</button>
              <button
                type="button"
                disabled=${busy || !(activePhacalMeta && activePhacalMeta.phacal_undo_available)}
                title="Undo the most recent phasecal edit on this antenna"
                onClick=${undoRelativeDelayEditor}
              >
                Undo
              </button>
              <button type="button" disabled=${busy} title="Reset all manual edits on the selected antenna back to the auto-solve values" onClick=${resetRelativeDelayEditorAntenna}>Reset</button>
              <button
                type="button"
                disabled=${busy || activePhacalMeta.missing_in_phacal}
                title=${activePhacalMeta.manual_anchor_fallback_override
                  ? "Stop using phasecal data as a temporary anchor for this antenna; revert to the refcal anchor"
                  : "Use this antenna's own phasecal data as a temporary anchor when its refcal is unusable"}
                onClick=${function () {
                  togglePhacalAnchorFallback(!activePhacalMeta.manual_anchor_fallback_override);
                }}
              >
                ${activePhacalMeta.manual_anchor_fallback_override ? "Clear Temp Fallback" : "Use Temp Fallback"}
              </button>
              <button
                type="button"
                disabled=${busy || !activePhacalMeta.manual_anchor_fallback_override}
                title=${activePhacalMeta.manual_anchor_fallback_override
                  ? (activePhacalMeta.promoted_to_refcal
                      ? "Re-promote: overwrite refcal anchor with this phacal's data for this antenna. Re-analyze later phacals to pick up."
                      : "Patch the refcal anchor with this phacal's complex visibility for this antenna. Re-analyze later phacals to pick up.")
                  : "Enable Use Temp Fallback first"}
                onClick=${function () {
                  promotePhacalAntennaToRefcal();
                }}
              >
                ${activePhacalMeta.promoted_to_refcal ? "Re-Promote to Refcal" : "Promote to Refcal"}
              </button>
            </div>
          </div>
        </div>

        <div className="delay-control-section">
          ${renderDelaySectionHeader("Manual Tuning", "Override the auto solve when needed", "neutral")}
          <div className="delay-actions">
            <button
              type="button"
              disabled=${busy}
              title=${phacalEditorAdvanced
                ? "Collapse the Manual Tuning controls"
                : "Open Manual Tuning to type override values for delay/offset"}
              onClick=${function () {
                setPhacalEditorAdvanced(!phacalEditorAdvanced);
              }}
            >
              ${phacalEditorAdvanced ? "Hide Manual Tuning" : "Show Manual Tuning"}
            </button>
          </div>
          ${phacalEditorAdvanced
            ? html`
                <div className="delay-grid delay-pair-grid">
                  <div className="tiny" style=${{ marginBottom: "0.25rem" }}>
                    Effective Saved = Auto + Applied. Type a manual value to override what the auto solve produced; click <b>Apply Manual</b> to commit it for the selected antenna.
                  </div>
                  <div className="delay-pair-row">
                    <label title="Manual delay added on top of the auto solve. Effective = Auto + Applied.">
                      X Applied Delay (ns)
                      <input
                        type="number"
                        step="0.1"
                        value=${relativeDelayDraft.x}
                        onChange=${function (event) {
                          setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { x: event.target.value }));
                        }}
                      />
                    </label>
                    <label>
                      Y Applied Delay (ns)
                      <input
                        type="number"
                        step="0.1"
                        value=${relativeDelayDraft.y}
                        onChange=${function (event) {
                          setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { y: event.target.value }));
                        }}
                      />
                    </label>
                  </div>
                  <div className="delay-pair-row">
                    <label>
                      X Applied Offset (rad)
                      <input
                        type="number"
                        step="0.1"
                        value=${relativeDelayDraft.xoff}
                        onChange=${function (event) {
                          setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { xoff: event.target.value }));
                        }}
                      />
                    </label>
                    <label>
                      Y Applied Offset (rad)
                      <input
                        type="number"
                        step="0.1"
                        value=${relativeDelayDraft.yoff}
                        onChange=${function (event) {
                          setRelativeDelayDraft(Object.assign({}, relativeDelayDraft, { yoff: event.target.value }));
                        }}
                      />
                    </label>
                  </div>
                  <div className="delay-actions">
                    <button
                      type="button"
                      className="btn-outline-red"
                      disabled=${busy}
                      title="Apply the manual delay/offset values above to the selected antenna"
                      onClick=${applyRelativeDelayEditorUpdate}
                    >
                      Apply Manual
                    </button>
                  </div>
                </div>
              `
            : null}
          <div className="tiny">
            ${activePhacalMeta.missing_in_phacal
              ? "Selected antenna has no usable phacal data. Saved delay/offset remain zero."
              : activePhacalMeta.missing_in_refcal
                ? "Selected antenna uses a temporary phacal-derived anchor fallback."
                : "Anchor-first solve: use Anchor-Ref. Phase first; residual panels are advanced QA only."}
          </div>
          <div className="tiny">
            ${"Kept bands X/Y: "
              + String(activePhacalMeta.x_kept_band_count || 0)
              + "/"
              + String(activePhacalMeta.y_kept_band_count || 0)
              + " · Kept ch. X/Y: "
              + String(activePhacalMeta.x_kept_channel_count || 0)
              + "/"
              + String(activePhacalMeta.y_kept_channel_count || 0)
              + " · Mask mode: "
              + (phacalMaskMode === "per_pol" ? "Per-pol override" : "Shared XY")}
          </div>
        </div>
      `;
    }

    return html`
      <div className="app-shell" ref=${shellRef}>
        <section className=${"toolbar panel" + (toolbarCollapsed ? " toolbar-collapsed" : "")} ref=${toolbarRef}>
          ${toolbarCollapsed
            ? activity
              ? html`
                  <div className=${"toolbar-collapsed-progress" + (activity.error ? " error" : "")}>
                    <div className="toolbar-collapsed-progress-top">
                      <span className="toolbar-status-title">${activity.title}</span>
                      <span className="toolbar-status-percent">${Math.max(0, Math.min(100, Math.round(activity.progress)))}%</span>
                    </div>
                    <div className="toolbar-collapsed-progress-stage">${activity.stage}</div>
                    <div className="activity-meter toolbar-collapsed-progress-meter">
                      <div
                        className="activity-meter-fill"
                        style=${{ width: Math.max(4, Math.min(100, activity.progress)) + "%" }}
                      ></div>
                    </div>
                  </div>
                `
              : null
            : html`<div className="toolbar-main">
            <div className="toolbar-column toolbar-left-column">
              <div className="toolbar-controls-row">
                <div className="field">
                  <label>Date</label>
                  <input type="date" value=${dateText} onChange=${function (event) { setDateText(event.target.value); }} />
                </div>
                <label className="field">
                  <input
                    type="checkbox"
                    checked=${!!(state && state.fix_drift)}
                    onChange=${function (event) {
                      runAction(function () {
                        return postJson("/api/settings/fix-drift", {
                          session_id: sessionId,
                          fix_drift: event.target.checked,
                        });
                      });
                    }}
                  />
                  Fix Phase Drift
                </label>
              </div>

              <div className="toolbar-session-grid">
                <div>
                  <strong>Selected Scan</strong>
                  <span>${currentScanLabel}</span>
                </div>
                <div>
                  <strong>Anchor Refcal</strong>
                  <span>${activeRefLabel}</span>
                </div>
                <div>
                  <strong>Secondary</strong>
                  <span>${secondaryRefLabel}</span>
                </div>
                <div>
                  <strong>Antenna</strong>
                  <span>${selectedAntLabel}</span>
                </div>
                <div>
                  <strong>Band</strong>
                  <span>${selectedBandLabel}</span>
                </div>
              </div>

              <div className="toolbar-actions-row">
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!canAnalyze || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/refcal/analyze", { session_id: sessionId, scan_id: selectedScanId() });
                      },
                      { progressKind: "refcal", successMessage: "Refcal analysis complete" }
                    );
                  }}
                >
                  Analyze Refcal
                </button>
                <button
                  type="button"
                  className="btn-dark-fill"
                  disabled=${!canRefcal || busy}
                  onClick=${function () {
                    const selectedIsActiveRefcal =
                      state && state.ref_scan_id !== null && selectedScanId() === state.ref_scan_id;
                    runAction(function () {
                      if (selectedIsActiveRefcal) {
                        return postJson("/api/refcal/clear", { session_id: sessionId });
                      }
                      return isPhacalScan
                        ? postJsonWithOverviewRefresh(
                            "/api/refcal/select",
                            { session_id: sessionId, scan_id: selectedScanId() }
                          )
                        : postJson(
                            "/api/refcal/select",
                            { session_id: sessionId, scan_id: selectedScanId() },
                            { selectionOnly: true }
                          );
                    });
                  }}
                >
                  ${state && state.ref_scan_id !== null && selectedScanId() === state.ref_scan_id
                    ? "Unset Refcal"
                    : "Set Refcal"}
                </button>
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!canRefcal || busy || !state || state.ref_scan_id === null || selectedScanId() === state.ref_scan_id || !selectedScanEntry || String(selectedScanEntry.feed_kind || "") !== String((state.active_refcal && state.active_refcal.feed_kind) || "")}
                  onClick=${function () {
                    runAction(function () {
                      return postJson("/api/refcal/secondary", {
                        session_id: sessionId,
                        scan_id: selectedScanId(),
                      });
                    });
                  }}
                >
                  Set Secondary
                </button>
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!canCompareAnchors || busy}
                  onClick=${function () {
                    compareSelectedAnchors();
                  }}
                >
                  Compare 2 Anchors
                </button>
                <button
                  type="button"
                  className="btn-filter-pill"
                  disabled=${!canCombine || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/refcal/combine", { session_id: sessionId, scan_ids: checkedScanIds });
                      },
                      { progressKind: "combine_refcal", successMessage: "Refcal combination complete" }
                    );
                  }}
                >
                  Combine 2 Refcals
                </button>
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!canPhacal || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/phacal/analyze", { session_id: sessionId, scan_id: selectedScanId() });
                      },
                      { progressKind: "phacal", successMessage: "Phacal analysis complete" }
                    );
                  }}
                >
                  Analyze Phacal
                </button>
                <button
                  type="button"
                  className="btn-blue"
                  disabled=${!canAnalyze || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return postJson("/api/save/sql", { session_id: sessionId, scan_id: selectedScanId() });
                      },
                      { progressKind: "save_sql", successMessage: "Saved scan to SQL" }
                    );
                  }}
                >
                  Save SQL
                </button>
                <button
                  type="button"
                  className="btn-outline-blue"
                  disabled=${!sessionId || busy}
                  onClick=${function () {
                    runAction(
                      function () {
                        return jsonFetch("/api/save/calibeovsa-npz", {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ session_id: sessionId }),
                        }).then(function (next) {
                          const nextState = next && next.state ? next.state : next;
                          setState(nextState);
                          syncDraft(nextState);
                          return next;
                        });
                      },
                      { progressKind: "save_calibeovsa_npz", successMessage: "calibeovsa NPZ bundle saved" }
                    );
                  }}
                >
                  Save calibeovsa NPZ
                </button>
              </div>

              <section className="toolbar-workflow-box">
                <h2>${workflowGuide.title}</h2>
                <p className="toolbar-workflow-next">${workflowGuide.next}</p>
                <ol className="toolbar-workflow-steps">
                  ${(workflowGuide.steps || []).map(function (step, idx) {
                    return html`<li key=${idx}>${step}</li>`;
                  })}
                </ol>
                ${workflowGuide.note ? html`<p className="toolbar-workflow-note">${workflowGuide.note}</p>` : null}
              </section>

              <div className=${"toolbar-status" + (activity ? " toolbar-status-progress" : "") + (activity && activity.error ? " error" : "")}>
                ${activity
                  ? html`
                      <div className="toolbar-status-top">
                        <span className="toolbar-status-title">${activity.title}</span>
                        <span className="toolbar-status-percent">${Math.max(0, Math.min(100, Math.round(activity.progress)))}%</span>
                      </div>
                      <div className="toolbar-status-stage">${activity.stage}</div>
                      <div className="activity-meter">
                        <div
                          className="activity-meter-fill"
                          style=${{ width: Math.max(4, Math.min(100, activity.progress)) + "%" }}
                        ></div>
                      </div>
                    `
                  : error
                    ? html`<span className="error">${error}</span>`
                    : interactionMessage || (state ? state.status_message : "Initializing session...")}
              </div>
            </div>

            <div className="toolbar-column toolbar-right-column">
              <div className="rail-title-row">
                <h2>Scans</h2>
                <div className="rail-title-meta">
                  ${metadataWarnings.length ? html`<span>${metadataWarnings[0]}</span>` : null}
                </div>
              </div>
              <div className="scanlist-scroll toolbar-scanlist-scroll" style=${{ maxHeight: scanlistMaxHeightPx + "px" }}>
                <table className="scan-table">
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Feed</th>
                      <th>Source</th>
                      <th>Dur</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${state && state.scans
                      ? state.scans.map(function (scan) {
                          const cls = [
                            scan.selected ? "selected" : "",
                            scan.is_refcal ? "refcal" : "",
                            checkedScanIds.indexOf(scan.scan_id) >= 0 ? "checked" : "",
                          ].join(" ");
                          return html`
                            <tr
                              key=${scan.scan_id}
                              className=${cls}
                              style=${{ backgroundColor: scan.color }}
                              onClick=${function (event) {
                                onScanRowClick(event, scan.scan_id);
                              }}
                            >
                              <td>${scan.scan_time}</td>
                              <td>${String(scan.feed_kind || "unknown").toUpperCase()}</td>
                              <td>${scan.source}</td>
                              <td>${scan.duration_min.toFixed(1)}m</td>
                              <td>${scan.status}${scan.is_refcal ? " *R" : ""}${scan.is_secondary_anchor ? " *S" : ""}</td>
                            </tr>
                          `;
                        })
                      : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>`}
          <button
            type="button"
            className=${"toolbar-chevron-toggle" + (toolbarCollapsed ? " collapsed" : "")}
            aria-label=${toolbarCollapsed ? "Show toolbar" : "Hide toolbar"}
            onClick=${function () {
              setToolbarCollapsed(function (current) {
                return !current;
              });
            }}
          >
            <span className="toolbar-chevron-glyph">»</span>
          </button>
        </section>

        <main className=${"content-grid" + (controlRailCollapsed ? " control-rail-collapsed" : "")} style=${paneMaxHeight ? { "--content-pane-max-height": paneMaxHeight + "px" } : null}>
          <aside className="control-rail-shell" style=${{ width: controlRailWidth + "px" }}>
            <div className=${"control-rail panel" + (controlRailCollapsed ? " collapsed" : "")}>
              ${controlRailCollapsed
                ? null
                : html`
                    <div className="control-rail-scroll">
                      <section className="rail-section heatmap-box">
                        <div className="rail-title-row">
                          <h2>Flag Map</h2>
                          <div className="rail-title-meta">
                            <span>${"Flag " + selectedCellFlagSum}</span>
                            <button
                              type="button"
                              className="btn-outline-blue"
                              disabled=${busy || !stagedTimeIntervals.length}
                              title="Commit the staged time-interval flags shown on the Flag Map"
                              onClick=${applyStagedTimeFlags}
                            >
                              Apply Mask
                            </button>
                          </div>
                        </div>
                        <${HeatmapPlot}
                          data=${heatmapData}
                          selectedAnt=${state ? state.selected_ant : null}
                          selectedBand=${state ? state.selected_band : null}
                          pendingCells=${pendingFlagMapCells}
                          onSelect=${onHeatmapSelect}
                        />
                      </section>

                      <section className="rail-section compact-th-box">
                        <div className="rail-title-row">
                          <h2>Time History</h2>
                          <div className="rail-title-meta">
                            <span>${timeHistoryData && timeHistoryData.title ? String(timeHistoryData.title).replace(", ", " · ") : "Ant " + selectedAntLabel + " · Band " + selectedBandLabel}</span>
                          </div>
                        </div>
                        <${CompactTimeHistoryPlot}
                          data=${timeHistoryData}
                          scope=${timeFlagScope}
                          pendingIntervals=${selectedPendingTimeIntervals}
                          onScopeChange=${setTimeFlagScope}
                          onStageInterval=${stageTimeFlag}
                          onApplyPending=${applyStagedTimeFlags}
                          onDeletePendingInterval=${removeStagedTimeFlag}
                          onDeleteInterval=${deleteTimeFlag}
                          onStatus=${setInteractionMessage}
                          busy=${busy}
                        />
                      </section>

                      <section className="rail-section delay-box delay-control-box">
                        <div className="rail-title-row">
                          <h2>${isPhacalScan ? "Phacal Solve Control" : "Delay Control"}</h2>
                          <div className="rail-title-meta">
                            <span>${selectedAntennaText}</span>
                          </div>
                        </div>
                        ${isPhacalScan ? renderPhacalSolveControl() : renderRefcalDelayControl()}
                      </section>
                    </div>
                  `}
              <button
                type="button"
                className=${"control-rail-toggle" + (controlRailCollapsed ? " collapsed" : "")}
                aria-label=${controlRailCollapsed ? "Show control rail" : "Hide control rail"}
                onClick=${toggleControlRailCollapsed}
              >
                <span className="control-rail-chevron-glyph">${controlRailCollapsed ? "»" : "«"}</span>
              </button>
            </div>
          </aside>
          ${controlRailCollapsed
            ? null
            : html`<div
                className="content-grid-splitter"
                role="separator"
                aria-orientation="vertical"
                aria-label="Resize control rail"
                onPointerDown=${startControlRailResize}
              ></div>`}

          <section className="analysis-wall panel">
            <div className="analysis-stack">
              ${!isPhacalScan && compareData
                ? html`<${AnchorCompareView}
                    data=${compareData}
                    busy=${busy}
                    canonicalId=${state && state.ref_scan_id !== null ? state.ref_scan_id : null}
                    secondaryId=${state && state.secondary_ref_scan_id !== null ? state.secondary_ref_scan_id : null}
                    onSetCanonical=${function (scanId) {
                      runAction(function () {
                        return postJson("/api/refcal/select", {
                          session_id: sessionId,
                          scan_id: scanId,
                        });
                      });
                    }}
                    onSetSecondary=${setSecondaryAnchor}
                    onClearSecondary=${clearSecondaryAnchor}
                    onToggleDonorPatch=${toggleDonorPatchCandidate}
                    onApplyDonorPatch=${applyDonorPatchSelection}
                    onClearCompare=${clearAnchorCompare}
                  />`
                : null}
              ${OVERVIEW_SECTIONS.filter(function (section) {
                if (section.phacalOnly && !isPhacalScan) {
                  return false;
                }
                if (section.refcalOnly && isPhacalScan) {
                  return false;
                }
                return true;
              }).map(function (section) {
                const plotData = overviewData ? overviewData[section.id] : null;
                return html`
                  <${PlotCard}
                    key=${section.id}
                    title=${plotData && plotData.title ? plotData.title : section.label}
                    legend=${section.showLegend === false ? null : plotData && plotData.legend}
                    inlineControls=${section.id === "inband_fit"
                      ? (isPhacalScan
                          ? html`
                              <div className="scope-pills" role="group" aria-label="Phacal mask mode">
                                <button
                                  type="button"
                                  className=${"scope-pill" + (phacalMaskMode === "shared_xy" ? " active" : "")}
                                  disabled=${busy}
                                  onClick=${function () {
                                    setPhacalMaskMode("shared_xy");
                                  }}
                                >
                                  Shared XY
                                </button>
                                <button
                                  type="button"
                                  className=${"scope-pill" + (phacalMaskMode === "per_pol" ? " active" : "")}
                                  disabled=${busy}
                                  onClick=${function () {
                                    setPhacalMaskMode("per_pol");
                                  }}
                                >
                                  Per-pol Override
                                </button>
                              </div>
                              <button
                                type="button"
                                className="btn-outline-blue"
                                disabled=${busy || !Object.keys(stagedInbandMasks).length}
                                onClick=${function () {
                                  previewStagedInbandSection("inband_fit");
                                }}
                                title="Preview the fit with staged mask — does not commit"
                              >
                                Preview
                              </button>
                              <button
                                type="button"
                                className="btn-outline-red"
                                disabled=${busy || !Object.keys(stagedInbandMasks).length}
                                onClick=${function () {
                                  applyStagedInbandWindow();
                                }}
                                title="Commit the staged mask. Activates the previewed fit; no recomputation needed."
                              >
                                Commit
                              </button>
                            `
                          : html`<${InbandWindowControls}
                              antennaScope=${inbandAntennaScope}
                              polScope=${inbandPolScope}
                              busy=${busy}
                              hasPending=${Object.keys(stagedInbandMasks).length > 0}
                              pendingCount=${Object.keys(stagedInbandMasks).length}
                              onAntennaScopeChange=${setInbandAntennaScope}
                              onPolScopeChange=${setInbandPolScope}
                              onApply=${applyStagedInbandWindow}
                            />
                            <button
                              type="button"
                              className="btn-outline-blue"
                              disabled=${busy || !Object.keys(stagedInbandMasks).length}
                              title="Preview the fit with staged mask — does not commit"
                              onClick=${function () {
                                previewStagedInbandSection("inband_fit");
                              }}
                            >
                              Preview
                            </button>`)
                      : section.id === "inband_relative_phase"
                        ? html`
                            ${!isPhacalScan
                              ? html`
                                  <${InbandWindowControls}
                                    antennaScope=${inbandAntennaScope}
                                    hidePol=${true}
                                    busy=${busy}
                                    hasPending=${Object.keys(stagedRelPhaseMasks).length > 0}
                                    pendingCount=${Object.keys(stagedRelPhaseMasks).length}
                                    onAntennaScopeChange=${setInbandAntennaScope}
                                    onPolScopeChange=${function () {}}
                                    onApply=${applyRelPhaseInbandWindow}
                                    applyLabel="Commit"
                                  />
                                  <button
                                    type="button"
                                    className="btn-outline-blue"
                                    disabled=${busy || !Object.keys(stagedRelPhaseMasks).length}
                                    title="Preview the fit with staged mask — does not commit"
                                    onClick=${function () {
                                      previewStagedInbandSection("inband_relative_phase");
                                    }}
                                  >
                                    Preview
                                  </button>
                                `
                              : html`<span className="plot-inline-summary">Adv. QA only</span>`}
                            ${!isPhacalScan
                              ? html`<div className="plot-inline-threshold">
                                  <label>
                                    <span>Y-X RMS Threshold (rad)</span>
                                    <input
                                      type="number"
                                      step="0.1"
                                      value=${yxThresholdDraft}
                                      disabled=${busy || !(state && state.active_refcal)}
                                      onChange=${function (event) {
                                        setYxThresholdDraft(event.target.value);
                                      }}
                                    />
                                  </label>
                                  <button
                                    type="button"
                                    className="btn-outline-blue"
                                    disabled=${busy || !(state && state.active_refcal)}
                                    title="Auto-flag antennas whose Y-X residual RMS exceeds the threshold above"
                                    onClick=${function () {
                                      applyYxResidualThreshold();
                                    }}
                                  >
                                    Apply
                                  </button>
                                </div>`
                              : null}
                          `
                      : section.id === "inband_residual_phase_band"
                        ? html`
                            ${!isPhacalScan
                              ? html`<div className="plot-inline-threshold">
                                  <label>
                                    <span>Inband Res. Thrshd (rad)</span>
                                    <input
                                      type="number"
                                      step="0.1"
                                      value=${residualThresholdDraft}
                                      disabled=${busy || !(state && state.active_refcal)}
                                      onChange=${function (event) {
                                        setResidualThresholdDraft(event.target.value);
                                      }}
                                    />
                                  </label>
                                  <button
                                    type="button"
                                    className="btn-outline-blue"
                                    disabled=${busy || !(state && state.active_refcal)}
                                    title="Auto-flag bands whose in-band residual RMS exceeds the threshold above"
                                    onClick=${function () {
                                      applyResidualBandThreshold();
                                    }}
                                  >
                                    Apply
                                  </button>
                                </div>`
                              : null}
                            <button
                              type="button"
                              className="btn-outline-fit-blue"
                              disabled=${busy || !canApplyResidualFit}
                              title="Apply per-band in-band residual correction to all eligible antennas"
                              onClick=${function () {
                                applyResidualInbandFit();
                              }}
                            >
                              Apply In-Band
                            </button>
                            <button
                              type="button"
                              className="btn-outline-blue"
                              disabled=${busy || !canUndoResidualPanel}
                              title="Undo the last residual-panel action (apply auto delay, in-band fit, or column flag)"
                              onClick=${function () {
                                undoResidualPanelAction();
                              }}
                            >
                              Undo
                            </button>
                            <button
                              type="button"
                              className="btn-outline-blue"
                              disabled=${busy || !Object.keys(stagedResidualMasks).length}
                              title="Preview the residual fit using the staged residual mask"
                              onClick=${function () {
                                previewStagedResidualSection();
                              }}
                            >
                              Preview
                            </button>
                            ${isPhacalScan ? html`<span className="plot-inline-summary">Adv. QA only</span>` : null}
                          `
                      : null}
                  >
                    <${PanelGridPlot}
                      data=${plotData}
                      hideLegend=${true}
                      busy=${busy}
                      panelHeight=${section.panelHeight}
                      selectedAnt=${state ? state.selected_ant : null}
                      interactionMode=${section.id === "inband_residual_delay_band" ? "zoom" : null}
                      panelOverride=${section.id === "inband_relative_phase"
                        ? function (rowIdx, panelIdx, panel) {
                            return panelWithStagedRelPhaseSelection(rowIdx, panelIdx, panel);
                          }
                        : canEditKeptMaskSection(section.id)
                          ? function (rowIdx, panelIdx, panel) {
                              return panelWithStagedInbandSelection(section.id, rowIdx, panelIdx, panel);
                            }
                          : canEditResidualMaskSection(section.id)
                            ? function (rowIdx, panelIdx, panel) {
                                return panelWithStagedResidualSelection(rowIdx, panelIdx, panel);
                              }
                            : null}
                      onBandWindowSelect=${section.id === "inband_relative_phase" && !isPhacalScan
                        ? function (rowIdx, panelIdx, startBand, endBand, mode) {
                            stageRelPhaseSelection(startBand, endBand, rowIdx, panelIdx, mode);
                          }
                        : canEditKeptMaskSection(section.id)
                          ? function (rowIdx, panelIdx, startBand, endBand, mode) {
                              if (isPhacalScan && section.id === "inband_fit" && Number(rowIdx) === 2) {
                                return;
                              }
                              stageInbandWindow(startBand, endBand, rowIdx, panelIdx, mode);
                            }
                          : canEditResidualMaskSection(section.id)
                            ? function (rowIdx, panelIdx, startBand, endBand) {
                                stageResidualSelection(startBand, endBand, rowIdx, panelIdx);
                              }
                            : null}
                      onPanelDoubleClick=${section.id === "inband_relative_phase" && !isPhacalScan
                        ? function (rowIdx, panelIdx) {
                            stageRelPhaseMaskClear(rowIdx, panelIdx);
                          }
                        : canEditKeptMaskSection(section.id)
                          ? function (rowIdx, panelIdx) {
                              stageInbandMaskClear(rowIdx, panelIdx);
                            }
                          : canEditResidualMaskSection(section.id)
                            ? function (rowIdx, panelIdx) {
                                stageResidualMaskClear(rowIdx, panelIdx);
                              }
                            : null}
                      bandSelectApplyLabel=${section.id === "inband_residual_phase_band"
                        ? "Apply In-Band"
                        : (isPhacalScan && section.id === "inband_fit" ? "Commit" : "Apply Mask")}
                      onColumnToggle=${(isPhacalScan && section.id === "inband_fit") || section.id === "inband_relative_phase" || section.id === "inband_residual_phase_band"
                        ? function (antennaIndex, flagged) {
                            toggleManualAntennaFlag(antennaIndex, flagged);
                          }
                        : null}
                      columnActionRenderer=${section.id === "inband_residual_phase_band"
                        || (isPhacalScan && section.id === "inband_fit")
                        ? function (control) { return residualColumnActionRenderer(control, section.id); }
                        : null}
                      onSlopeGesture=${isPhacalScan && section.id === "inband_fit"
                        ? handlePhacalAnchorSlopeGesture
                        : null}
                    />
                  </${PlotCard}>
                `;
              })}
            </div>
          </section>
        </main>

        <${ResidualInspectorModal}
          data=${inbandResidualInspector && inbandResidualInspector.data}
          title=${(inbandResidualInspector && inbandResidualInspector.title) || "Inband Fit Residuals"}
          onClose=${function () {
            setInbandResidualInspector(null);
          }}
        />

      </div>
    `;
  }

  ReactDOM.createRoot(document.getElementById("app")).render(html`<${App} />`);
})();
